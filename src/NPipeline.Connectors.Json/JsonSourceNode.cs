using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using NPipeline.Connectors.Json.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Json;

/// <summary>
///     Source node that reads JSON data using a pluggable <see cref="IStorageProvider" />.
/// </summary>
/// <typeparam name="T">Type emitted for each JSON object.</typeparam>
/// <remarks>
///     <para>
///         This source node supports reading both JSON arrays and newline-delimited JSON (NDJSON) formats
///         using System.Text.Json for efficient streaming. It provides streaming access to JSON data with
///         configurable options for format handling, property naming, and error handling.
///     </para>
///     <para>
///         The node supports two constructor patterns:
///         <list type="bullet">
///             <item>
///                 <description>Using an <see cref="IStorageResolver" /> to resolve the provider at execution time</description>
///             </item>
///             <item>
///                 <description>Using a specific <see cref="IStorageProvider" /> instance</description>
///             </item>
///         </list>
///     </para>
///     <para>
    ///         Data mapping is performed using either:
///         <list type="bullet">
    ///             <item>
    ///                 <description>Attribute-based mapping using Column/JsonPropertyName attributes</description>
    ///             </item>
///             <item>
///                 <description>Explicit <see cref="JsonRow" /> mapper delegate supplied by the caller</description>
///             </item>
///         </list>
///     </para>
/// </remarks>
public sealed class JsonSourceNode<T> : SourceNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => StorageProviderFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly JsonConfiguration _configuration;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly Func<JsonRow, T>? _rowMapper;
    private readonly StorageUri _uri;

    private JsonSourceNode(
        StorageUri uri,
        JsonConfiguration? configuration,
        Func<JsonRow, T>? rowMapper)
    {
        ArgumentNullException.ThrowIfNull(uri);
        _uri = uri;
        _configuration = configuration ?? new JsonConfiguration();
        _rowMapper = rowMapper;
    }

    /// <summary>
    ///     Construct a JSON source that uses attribute-based mapping.
    ///     Properties are mapped using ColumnAttribute or convention (lowercase by default).
    /// </summary>
    /// <param name="uri">The URI of the JSON file to read from.</param>
    /// <param name="resolver">
    ///     The storage resolver used to obtain the storage provider. If <c>null</c>, a default resolver
    ///     created by <see cref="StorageProviderFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="configuration">Optional configuration for JSON reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    /// <remarks>
    ///     <para>
    ///         This constructor uses attribute-based mapping, where properties are mapped using
    ///         <see cref="Attributes.ColumnAttribute" /> or <see cref="System.Text.Json.Serialization.JsonPropertyNameAttribute" />.
    ///     </para>
    /// </remarks>
    public JsonSourceNode(
        StorageUri uri,
        IStorageResolver? resolver = null,
        JsonConfiguration? configuration = null)
        : this(uri, configuration, null)
    {
        _resolver = resolver;
    }

    /// <summary>
    ///     Construct a JSON source that uses a specific storage provider with attribute-based mapping.
    ///     Properties are mapped using ColumnAttribute or convention (lowercase by default).
    /// </summary>
    /// <param name="provider">The storage provider to use for reading the JSON file.</param>
    /// <param name="uri">The URI of the JSON file to read from.</param>
    /// <param name="configuration">Optional configuration for JSON reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="provider" /> or <paramref name="uri" /> is <c>null</c>.</exception>
    /// <remarks>
    ///     <para>
    ///         This constructor uses attribute-based mapping, where properties are mapped using
    ///         <see cref="Attributes.ColumnAttribute" /> or <see cref="System.Text.Json.Serialization.JsonPropertyNameAttribute" />.
    ///     </para>
    /// </remarks>
    public JsonSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        JsonConfiguration? configuration = null)
        : this(uri, configuration, null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        _provider = provider;
    }

    /// <summary>
    ///     Construct a JSON source that resolves a storage provider from a resolver at execution time.
    /// </summary>
    /// <param name="uri">The URI of the JSON file to read from.</param>
    /// <param name="rowMapper">Row mapper used to construct <typeparamref name="T" /> from a <see cref="JsonRow" />.</param>
    /// <param name="resolver">
    ///     The storage resolver used to obtain the storage provider. If <c>null</c>, a default resolver
    ///     created by <see cref="StorageProviderFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="configuration">Optional configuration for JSON reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> or <paramref name="rowMapper" /> is <c>null</c>.</exception>
    public JsonSourceNode(
        StorageUri uri,
        Func<JsonRow, T> rowMapper,
        IStorageResolver? resolver = null,
        JsonConfiguration? configuration = null)
        : this(uri, configuration, rowMapper)
    {
        ArgumentNullException.ThrowIfNull(rowMapper);
        _resolver = resolver;
    }

    /// <summary>
    ///     Construct a JSON source that uses a specific storage provider.
    /// </summary>
    /// <param name="provider">The storage provider to use for reading the JSON file.</param>
    /// <param name="uri">The URI of the JSON file to read from.</param>
    /// <param name="rowMapper">Row mapper used to construct <typeparamref name="T" /> from a <see cref="JsonRow" />.</param>
    /// <param name="configuration">Optional configuration for JSON reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="provider" />, <paramref name="uri" />, or <paramref name="rowMapper" /> is <c>null</c>.</exception>
    public JsonSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        Func<JsonRow, T> rowMapper,
        JsonConfiguration? configuration = null)
        : this(uri, configuration, rowMapper)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(rowMapper);
        _provider = provider;
    }

    /// <inheritdoc />
    public override IDataPipe<T> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = _provider ?? StorageProviderFactory.GetProviderOrThrow(
            _resolver ?? DefaultResolver.Value,
            _uri);

        if (provider is IStorageProviderMetadataProvider metaProvider)
        {
            var meta = metaProvider.GetMetadata();

            if (!meta.SupportsRead)
                throw new UnsupportedStorageCapabilityException(_uri, "read", meta.Name);
        }

        var stream = Read(provider, _uri, _configuration, cancellationToken);
        return new StreamingDataPipe<T>(stream, $"JsonSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<T> Read(
        IStorageProvider provider,
        StorageUri uri,
        JsonConfiguration config,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Open the stream per-enumeration so disposal is bound to consumer lifetime
        await using var stream = await provider.OpenReadAsync(uri, cancellationToken).ConfigureAwait(false);

        if (config.Format == JsonFormat.Array)
        {
            await foreach (var item in ReadJsonArray(stream, config, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        else
        {
            // Use UTF-8 encoding without BOM
            using var reader = new StreamReader(stream, new UTF8Encoding(false), true, config.BufferSize);
            await foreach (var item in ReadNdjson(reader, config, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
    }

    private async IAsyncEnumerable<T> ReadJsonArray(
        Stream stream,
        JsonConfiguration config,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var items = JsonSerializer.DeserializeAsyncEnumerable<JsonElement>(
            stream,
            config.SerializerOptions,
            cancellationToken);

        if (items is null)
            yield break;

        await foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();

            T? result = MapJsonElement(item, config, cancellationToken);
            if (result is not null)
                yield return result;
        }
    }

    private async IAsyncEnumerable<T> ReadNdjson(
        StreamReader reader,
        JsonConfiguration config,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string? line;

        while ((line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false)) is not null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (string.IsNullOrWhiteSpace(line))
                continue;

            JsonElement item;

            try
            {
                var jsonDoc = JsonDocument.Parse(line, new JsonDocumentOptions
                {
                    AllowTrailingCommas = true
                });
                item = jsonDoc.RootElement;
            }
            catch (JsonException ex)
            {
                throw new JsonException(
                    $"Failed to parse NDJSON line: {line}. {ex.Message}",
                    ex);
            }

            T? result = MapJsonElement(item, config, cancellationToken);
            if (result is not null)
                yield return result;
        }
    }

    private T? MapJsonElement(
        JsonElement element,
        JsonConfiguration config,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Check if we have a manual mapper
        if (_rowMapper is not null)
        {
            var row = new JsonRow(element, config.PropertyNameCaseInsensitive);

            try
            {
                return _rowMapper(row);
            }
            catch (Exception ex)
            {
                var handler = config.RowErrorHandler;

                if (handler is not null && handler(ex, row))
                    return default!; // handler opted to swallow

                throw;
            }
        }

        // Use attribute-based mapping via JsonMapperBuilder
        var attrRow = new JsonRow(element, config.PropertyNameCaseInsensitive);
        var mapper = JsonMapperBuilder.Build<T>(config.PropertyNamingPolicy);

        try
        {
            return mapper(attrRow);
        }
        catch (Exception ex)
        {
            var handler = config.RowErrorHandler;

            if (handler is not null && handler(ex, attrRow))
                return default!; // handler opted to swallow

            throw;
        }
    }
}
