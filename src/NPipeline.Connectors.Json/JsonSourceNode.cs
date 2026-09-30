using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Serialization;

namespace NPipeline.Connectors.Json;

/// <summary>
///     Reads JSON files into records with System.Text.Json: the elements of a root array, the elements of an array at
///     <see cref="JsonReadOptions.ItemsPath" />, or top-level values (NDJSON, or a single object). Each record is
///     deserialized straight from UTF-8, so a record that fails to convert is a row error and the read can go on.
/// </summary>
/// <typeparam name="T">The record type: anything the serializer can read, including nested objects, lists and scalars.</typeparam>
/// <remarks>
///     The options' <see cref="FileNodeOptions.Uri" /> can name a file, a directory (ending in <c>/</c>, which reads its
///     <c>.json</c>, <c>.ndjson</c> and <c>.jsonl</c> files) or a glob. Files ending in <c>.gz</c>, <c>.br</c> or
///     <c>.zz</c> are decompressed.
/// </remarks>
public sealed class JsonSourceNode<T> : FileSourceNode<T>
{
    private readonly IReadOnlyList<string>? _itemsPath;
    private readonly Func<JsonRow, T>? _map;
    private readonly JsonReadOptions _options;
    private readonly JsonReaderOptions _readerOptions;
    private readonly JsonSerializerOptions _serializerOptions;
    private readonly JsonTypeInfo<T>? _typeInfo;

    /// <summary>Creates a source that deserializes each record with the options' serializer settings.</summary>
    public JsonSourceNode(JsonReadOptions options)
        : this(options, null, null)
    {
    }

    /// <summary>Creates a source that deserializes each record with <paramref name="typeInfo" />, such as one from a source-generated <c>JsonSerializerContext</c>.</summary>
    /// <param name="options">The source's options. Their <see cref="JsonReadOptions.SerializerOptions" /> only matter for <see cref="JsonRow" /> reads.</param>
    /// <param name="typeInfo">The metadata to deserialize records with.</param>
    public JsonSourceNode(JsonReadOptions options, JsonTypeInfo<T> typeInfo)
        : this(options, typeInfo ?? throw new ArgumentNullException(nameof(typeInfo)), null)
    {
    }

    /// <summary>Creates a source that builds each record with <paramref name="map" />.</summary>
    /// <param name="options">The source's options.</param>
    /// <param name="map">Builds a record from the current one. An exception it throws is a row error.</param>
    public JsonSourceNode(JsonReadOptions options, Func<JsonRow, T> map)
        : this(options, null, map ?? throw new ArgumentNullException(nameof(map)))
    {
    }

    private JsonSourceNode(JsonReadOptions options, JsonTypeInfo<T>? typeInfo, Func<JsonRow, T>? map)
        : base(options)
    {
        _options = options;
        _map = map;
        _serializerOptions = ConnectorJson.Resolve(options.SerializerOptions);
        _typeInfo = map is null ? typeInfo ?? ConnectorJson.TypeInfo<T>(_serializerOptions) : null;
        _itemsPath = options.ItemsPath is null ? null : JsonItemsPath.Parse(options.ItemsPath);

        _readerOptions = new JsonReaderOptions
        {
            AllowTrailingCommas = _serializerOptions.AllowTrailingCommas,
            CommentHandling = _serializerOptions.ReadCommentHandling,
            MaxDepth = _serializerOptions.MaxDepth,
        };
    }

    /// <inheritdoc />
    protected override string ConnectorName => "json";

    /// <inheritdoc />
    protected override IReadOnlyList<string> DirectoryFileExtensions { get; } =
        [".json", ".ndjson", ".jsonl", ".json.gz", ".ndjson.gz", ".jsonl.gz", ".json.br", ".ndjson.br", ".jsonl.br"];

    /// <inheritdoc />
    protected override async IAsyncEnumerable<T> ReadAsync(Stream stream, FileReadContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var scanner = new JsonValueScanner(stream, context.BufferSize, _readerOptions);

        try
        {
            if (!await scanner.StartAsync(_options.Format, _itemsPath, cancellationToken).ConfigureAwait(false))
                yield break;
        }
        catch (JsonException ex)
        {
            throw new JsonException($"'{context.Source}': {ex.Message}", ex.Path, ex.LineNumber, ex.BytePositionInLine, ex);
        }

        long recordNumber = 0;

        while (true)
        {
            string? malformed = null;

            try
            {
                if (!await scanner.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    break;
            }
            catch (JsonException ex) when (scanner.IsSequence)
            {
                // In NDJSON a malformed line is one bad record: report it and carry on from the next line.
                malformed = await scanner.SkipLineAsync(cancellationToken).ConfigureAwait(false);
                await context.HandleRowErrorAsync(++recordNumber, ex, malformed, cancellationToken).ConfigureAwait(false);
                continue;
            }
            catch (JsonException ex)
            {
                throw new JsonException($"'{context.Source}' is not valid JSON after record {recordNumber}: {ex.Message}", ex.Path, ex.LineNumber, ex.BytePositionInLine, ex);
            }

            recordNumber++;
            T item = default!;
            Exception? error = null;

            try
            {
                item = Map(scanner.Current, recordNumber);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                error = ex is JsonException { Path: { Length: > 1 } path } ? new FieldMappingException(path, path, ex) : ex;
            }

            if (error is not null)
            {
                await context.HandleRowErrorAsync(recordNumber, error, Excerpt(scanner.Current.Span), cancellationToken).ConfigureAwait(false);
                continue;
            }

            yield return item;
        }
    }

    private T Map(ReadOnlyMemory<byte> record, long recordNumber)
    {
        if (_typeInfo is not null)
            return JsonSerializer.Deserialize(record.Span, _typeInfo)!;

        // The document reads the scanner's buffer in place and is disposed before the scanner moves on.
        using var document = JsonDocument.Parse(record, new JsonDocumentOptions { AllowTrailingCommas = _readerOptions.AllowTrailingCommas, CommentHandling = _readerOptions.CommentHandling });
        return _map!(new JsonRow(document.RootElement, _serializerOptions, recordNumber));
    }

    // Decodes only as much of the record as the excerpt can hold, however large the record is.
    private string? Excerpt(ReadOnlySpan<byte> record) =>
        _options.RawExcerptLength == 0
            ? null
            : Encoding.UTF8.GetString(record[..Math.Min(record.Length, (_options.RawExcerptLength * 4) + 4)]);
}
