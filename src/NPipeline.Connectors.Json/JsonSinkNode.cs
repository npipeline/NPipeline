using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Serialization;

namespace NPipeline.Connectors.Json;

/// <summary>
///     Writes records to a JSON file with System.Text.Json, as an array or as NDJSON. Output is flushed to storage every
///     64 KB, so memory stays constant however many records are written.
/// </summary>
/// <typeparam name="T">The record type: anything the serializer can write.</typeparam>
/// <remarks>
///     With <see cref="JsonFormat.Auto" />, files ending in <c>.ndjson</c> or <c>.jsonl</c> (before a compression suffix)
///     are NDJSON and other files are arrays. A <c>null</c> item is written as JSON <c>null</c> when
///     <see cref="FileSinkOptions.NullItems" /> is <see cref="NullItemHandling.Write" />.
/// </remarks>
public sealed class JsonSinkNode<T> : FileSinkNode<T>
{
    private const int FlushThresholdBytes = 64 * 1024;

    private readonly JsonWriteOptions _options;
    private readonly JsonTypeInfo<T> _typeInfo;
    private readonly JsonWriterOptions _writerOptions;

    /// <summary>Creates a sink that serializes each record with the options' serializer settings.</summary>
    public JsonSinkNode(JsonWriteOptions options)
        : this(options, ConnectorJson.TypeInfo<T>(ConnectorJson.Resolve(options?.SerializerOptions)))
    {
    }

    /// <summary>Creates a sink that serializes each record with <paramref name="typeInfo" />, such as one from a source-generated <c>JsonSerializerContext</c>.</summary>
    /// <param name="options">The sink's options. Serializer settings come from <paramref name="typeInfo" />.</param>
    /// <param name="typeInfo">The metadata to serialize records with.</param>
    public JsonSinkNode(JsonWriteOptions options, JsonTypeInfo<T> typeInfo)
        : base(options)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        _options = options;
        _typeInfo = typeInfo;
        var serializerOptions = typeInfo.Options;

        _writerOptions = new JsonWriterOptions
        {
            Encoder = serializerOptions.Encoder,
            Indented = options.WriteIndented || serializerOptions.WriteIndented,
            MaxDepth = serializerOptions.MaxDepth,
            SkipValidation = true,
        };
    }

    /// <inheritdoc />
    protected override string ConnectorName => "json";

    /// <inheritdoc />
    protected override bool SupportsNullItems => true;

    /// <inheritdoc />
    protected override async Task WriteAsync(Stream stream, IAsyncEnumerable<T> items, FileWriteContext context, CancellationToken cancellationToken)
    {
        var ndjson = _options.Format switch
        {
            JsonFormat.NewlineDelimited => true,
            JsonFormat.Array => false,
            _ => IsNdjsonPath(context.Uri.Path),
        };

        // Serialize into a pooled buffer and write it out asynchronously in chunks: Utf8JsonWriter's own stream writes are synchronous.
        var buffer = new ArrayBufferWriter<byte>(FlushThresholdBytes * 2);
        var writer = new Utf8JsonWriter(buffer, ndjson ? _writerOptions with { Indented = false } : _writerOptions);

        await using (writer.ConfigureAwait(false))
        {
            if (!ndjson)
                writer.WriteStartArray();

            await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                JsonSerializer.Serialize(writer, item, _typeInfo);

                if (ndjson)
                {
                    writer.Flush();
                    buffer.Write("\n"u8);

                    // The next record is a new top-level value.
                    writer.Reset(buffer);
                }
                else if (writer.BytesPending < FlushThresholdBytes && buffer.WrittenCount < FlushThresholdBytes)
                    continue;
                else
                    writer.Flush();

                if (buffer.WrittenCount >= FlushThresholdBytes)
                {
                    await stream.WriteAsync(buffer.WrittenMemory, cancellationToken).ConfigureAwait(false);
                    buffer.ResetWrittenCount();
                }
            }

            if (!ndjson)
                writer.WriteEndArray();

            writer.Flush();
        }

        if (buffer.WrittenCount > 0)
            await stream.WriteAsync(buffer.WrittenMemory, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsNdjsonPath(string path)
    {
        foreach (var compressed in (ReadOnlySpan<string>)[".gz", ".br", ".zz", ".zlib", ".deflate"])
        {
            if (path.EndsWith(compressed, StringComparison.OrdinalIgnoreCase))
            {
                path = path[..^compressed.Length];
                break;
            }
        }

        return path.EndsWith(".ndjson", StringComparison.OrdinalIgnoreCase) || path.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase);
    }
}
