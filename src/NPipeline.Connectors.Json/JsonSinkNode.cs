using System.Text;
using System.Text.Json;
using NPipeline.Connectors.Json.Mapping;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Json;

/// <summary>
///     Sink node that writes items to JSON format using a pluggable <see cref="IStorageProvider" />.
/// </summary>
/// <typeparam name="T">Record type to serialize for each JSON object.</typeparam>
/// <remarks>
///     <para>
///         This sink node writes data to JSON files in either JSON array format or newline-delimited JSON (NDJSON) format.
///         It uses streaming via <see cref="Utf8JsonWriter" /> for memory efficiency and supports attribute-based
///         property mapping for automatic serialization.
///     </para>
///     <para>
///         The node supports two constructor patterns:
///         <list type="bullet">
///             <item>
///                 <description>Using default <see cref="IStorageResolver" /> (recommended for simplicity)</description>
///             </item>
///             <item>
///                 <description>Using a specific <see cref="IStorageProvider" /> instance for direct provider injection</description>
///             </item>
///         </list>
///     </para>
///     <para>
///         Data mapping is performed using compiled delegates from <see cref="JsonWriterMapperBuilder" />.
///         Properties are mapped using <see cref="Attributes.ColumnAttribute" /> or by applying a naming policy
///         (lowercase by default for consistency with CSV/Excel connectors).
///     </para>
/// </remarks>
public sealed class JsonSinkNode<T> : SinkNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver =
        new(() => StorageProviderFactory.CreateResolver());

    private readonly JsonConfiguration _configuration;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly StorageUri _uri;

    private JsonSinkNode(
        StorageUri uri,
        JsonConfiguration? configuration)
    {
        ArgumentNullException.ThrowIfNull(uri);
        _uri = uri;
        _configuration = configuration ?? new JsonConfiguration();
    }

    /// <summary>
    ///     Construct a JSON sink node that uses attribute-based mapping.
    ///     Properties are mapped using ColumnAttribute or convention (lowercase by default).
    /// </summary>
    /// <param name="uri">The URI of the JSON file to write to.</param>
    /// <param name="resolver">
    ///     The storage resolver used to obtain storage provider. If <c>null</c>, a default resolver
    ///     created by <see cref="StorageProviderFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="configuration">Optional configuration for JSON writing. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    /// <remarks>
    ///     <para>
    ///         This constructor uses attribute-based mapping, where properties are mapped to JSON properties
    ///         using the <see cref="Attributes.ColumnAttribute" /> or by applying a naming policy.
    ///         The default naming policy is <see cref="JsonPropertyNamingPolicy.LowerCase" /> for consistency
    ///         with the CSV and Excel connectors.
    ///     </para>
    ///     <para>
    ///         For custom mapping logic, you can implement a custom transform node before this sink node.
    ///     </para>
    /// </remarks>
    public JsonSinkNode(
        StorageUri uri,
        IStorageResolver? resolver = null,
        JsonConfiguration? configuration = null)
        : this(uri, configuration)
    {
        _resolver = resolver ?? DefaultResolver.Value;
    }

    /// <summary>
    ///     Construct a JSON sink node that uses a specific storage provider with attribute-based mapping.
    ///     Properties are mapped using ColumnAttribute or convention (lowercase by default).
    /// </summary>
    /// <param name="provider">The storage provider to use for writing the JSON file.</param>
    /// <param name="uri">The URI of the JSON file to write to.</param>
    /// <param name="configuration">Optional configuration for JSON writing. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="provider" /> or <paramref name="uri" /> is <c>null</c>.</exception>
    /// <remarks>
    ///     <para>
    ///         This constructor allows you to specify a storage provider explicitly, which is useful when
    ///         you need to use a custom provider or when you want to avoid the overhead of resolver-based
    ///         provider resolution.
    ///     </para>
    ///     <para>
    ///         The provider must support write operations. If the provider implements
    ///         <see cref="IStorageProviderMetadataProvider" />, its metadata is checked for write capability.
    ///     </para>
    /// </remarks>
    public JsonSinkNode(
        IStorageProvider provider,
        StorageUri uri,
        JsonConfiguration? configuration = null)
        : this(uri, configuration)
    {
        ArgumentNullException.ThrowIfNull(provider);
        _provider = provider;
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = _provider ?? StorageProviderFactory.GetProviderOrThrow(
            _resolver ?? throw new InvalidOperationException("No storage resolver configured for JsonSinkNode."),
            _uri);

        if (provider is IStorageProviderMetadataProvider metaProvider)
        {
            var meta = metaProvider.GetMetadata();

            if (!meta.SupportsWrite)
                throw new UnsupportedStorageCapabilityException(_uri, "write", meta.Name);
        }

        await using var stream = await provider.OpenWriteAsync(_uri, cancellationToken).ConfigureAwait(false);
        await WriteToStream(stream, input, _configuration, cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteToStream(
        Stream stream,
        IDataPipe<T> input,
        JsonConfiguration config,
        CancellationToken cancellationToken)
    {
        // Use UTF-8 encoding without BOM
        var utf8Encoding = new UTF8Encoding(false);

        await using var writer = new BufferedStream(stream, config.BufferSize);

        var type = typeof(T);
        var isComplexType = type.IsClass && type != typeof(string);

        // Get property names and value getters for complex types
        var propertyNames = isComplexType
            ? JsonWriterMapperBuilder.GetPropertyNames<T>(config.PropertyNamingPolicy)
            : Array.Empty<string>();

        var valueGetters = isComplexType
            ? JsonWriterMapperBuilder.GetValueGetters<T>(config.PropertyNamingPolicy)
            : Array.Empty<Func<T, object?>>();

        var useMapper = isComplexType && valueGetters.Length > 0;

        // Write start array for JSON Array format
        Utf8JsonWriter? jsonWriter = null;

        if (config.Format == JsonFormat.Array)
        {
            jsonWriter = new Utf8JsonWriter(writer, new JsonWriterOptions
            {
                Indented = config.WriteIndented,
            });

            jsonWriter.WriteStartArray();
        }

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            if (item is null)
                continue;

            if (config.Format == JsonFormat.NewlineDelimited)
            {
                // For NDJSON, create a new writer for each item
                await using var ndjsonWriter = new Utf8JsonWriter(writer, new JsonWriterOptions
                {
                    Indented = false,
                });

                WriteItem(ndjsonWriter, item, valueGetters, propertyNames, useMapper);
                await ndjsonWriter.FlushAsync(cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(utf8Encoding.GetBytes("\n"), cancellationToken).ConfigureAwait(false);
            }
            else
                WriteItem(jsonWriter!, item, valueGetters, propertyNames, useMapper);
        }

        // Write end array for JSON Array format
        if (config.Format == JsonFormat.Array && jsonWriter is not null)
        {
            jsonWriter.WriteEndArray();
            await jsonWriter.FlushAsync(cancellationToken).ConfigureAwait(false);
            await jsonWriter.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static void WriteItem(
        Utf8JsonWriter jsonWriter,
        T item,
        Func<T, object?>[] valueGetters,
        string[] propertyNames,
        bool useMapper)
    {
        if (!useMapper)
        {
            // For primitive types or when no mapper is available, write the value directly
            WritePrimitiveValue(jsonWriter, item);
            return;
        }

        // Write object with properties
        jsonWriter.WriteStartObject();

        for (var i = 0; i < valueGetters.Length; i++)
        {
            var value = valueGetters[i](item);
            var propertyName = propertyNames[i];

            jsonWriter.WritePropertyName(propertyName);
            WriteValue(jsonWriter, value);
        }

        jsonWriter.WriteEndObject();
    }

    private static void WriteValue(Utf8JsonWriter jsonWriter, object? value)
    {
        if (value is null)
        {
            jsonWriter.WriteNullValue();
            return;
        }

        switch (value)
        {
            case string s:
                jsonWriter.WriteStringValue(s);
                break;

            case bool b:
                jsonWriter.WriteBooleanValue(b);
                break;

            case int i:
                jsonWriter.WriteNumberValue(i);
                break;

            case long l:
                jsonWriter.WriteNumberValue(l);
                break;

            case short s16:
                jsonWriter.WriteNumberValue(s16);
                break;

            case uint ui:
                jsonWriter.WriteNumberValue(ui);
                break;

            case ulong ul:
                jsonWriter.WriteNumberValue(ul);
                break;

            case ushort us:
                jsonWriter.WriteNumberValue(us);
                break;

            case float f:
                jsonWriter.WriteNumberValue(f);
                break;

            case double d:
                jsonWriter.WriteNumberValue(d);
                break;

            case decimal m:
                jsonWriter.WriteNumberValue(m);
                break;

            case byte b8:
                jsonWriter.WriteNumberValue(b8);
                break;

            case sbyte sb:
                jsonWriter.WriteNumberValue(sb);
                break;

            case DateTime dt:
                jsonWriter.WriteStringValue(dt);
                break;

            case DateTimeOffset dto:
                jsonWriter.WriteStringValue(dto);
                break;

            case Guid g:
                jsonWriter.WriteStringValue(g);
                break;

            case Enum e:
                jsonWriter.WriteStringValue(e.ToString());
                break;

            default:
                // For other types, try to serialize as string
                jsonWriter.WriteStringValue(value.ToString() ?? string.Empty);
                break;
        }
    }

    private static void WritePrimitiveValue(Utf8JsonWriter jsonWriter, T item)
    {
        if (item is null)
        {
            jsonWriter.WriteNullValue();
            return;
        }

        WriteValue(jsonWriter, item);
    }
}
