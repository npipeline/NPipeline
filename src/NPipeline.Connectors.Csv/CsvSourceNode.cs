using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using CsvHelper;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Exceptions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Csv;

/// <summary>
///     Source node that reads CSV data using a pluggable <see cref="IStorageProvider" />.
/// </summary>
/// <typeparam name="T">Type emitted for each CSV row.</typeparam>
public sealed class CsvSourceNode<T> : SourceNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => StorageProviderFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly CsvConfiguration _csvConfiguration;
    private readonly Encoding _encoding;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly Func<CsvRow, T> _rowMapper;
    private readonly StorageUri _uri;

    private CsvSourceNode(
        StorageUri uri,
        CsvConfiguration? configuration,
        Encoding? encoding,
        Func<CsvRow, T> rowMapper)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(rowMapper);
        _uri = uri;

        _csvConfiguration = configuration ?? new CsvConfiguration(CultureInfo.InvariantCulture);

        // Set DetectDelimiter on the underlying CsvHelper configuration
        _csvConfiguration.HelperConfiguration.DetectDelimiter = true;

        _encoding = encoding ?? new UTF8Encoding(false);
        _rowMapper = rowMapper;
    }

    /// <summary>
    ///     Construct a CSV source that resolves a storage provider from a resolver at execution time.
    /// </summary>
    /// <param name="uri">The URI of the CSV file to read from.</param>
    /// <param name="resolver">
    ///     The storage resolver used to obtain the storage provider. If <c>null</c>, a default resolver
    ///     created by <see cref="StorageProviderFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="rowMapper">Row mapper used to construct <typeparamref name="T" /> from a <see cref="CsvRow" />.</param>
    /// <param name="configuration">Optional configuration for CSV reading. If <c>null</c>, default configuration is used.</param>
    /// <param name="encoding">Optional text encoding. If <c>null</c>, UTF-8 without BOM is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    public CsvSourceNode(
        StorageUri uri,
        Func<CsvRow, T> rowMapper,
        IStorageResolver? resolver = null,
        CsvConfiguration? configuration = null,
        Encoding? encoding = null)
        : this(uri, configuration, encoding, rowMapper)
    {
        _resolver = resolver;
    }

    /// <summary>
    ///     Construct a CSV source that uses a specific storage provider.
    /// </summary>
    public CsvSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        Func<CsvRow, T> rowMapper,
        CsvConfiguration? configuration = null,
        Encoding? encoding = null)
        : this(uri, configuration, encoding, rowMapper)
    {
        ArgumentNullException.ThrowIfNull(provider);
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

        var stream = Read(provider, _uri, _csvConfiguration, _encoding, cancellationToken);
        return new StreamingDataPipe<T>(stream, $"CsvSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<T> Read(
        IStorageProvider provider,
        StorageUri uri,
        CsvConfiguration cfg,
        Encoding encoding,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Open the stream per-enumeration so disposal is bound to consumer lifetime
        await using var stream = await provider.OpenReadAsync(uri, cancellationToken).ConfigureAwait(false);
        using var reader = new StreamReader(stream, encoding, true);
        using var csv = new CsvReader(reader, cfg.HelperConfiguration);

        var hasHeaders = cfg.HelperConfiguration.HasHeaderRecord;
        var headers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        if (hasHeaders)
        {
            if (!await csv.ReadAsync().ConfigureAwait(false))
                yield break;

            _ = csv.ReadHeader();
            var headerRecord = csv.HeaderRecord ?? [];

            for (var i = 0; i < headerRecord.Length; i++)
            {
                var header = headerRecord[i];

                if (!string.IsNullOrWhiteSpace(header))
                    headers[header] = i;
            }
        }

        while (await csv.ReadAsync().ConfigureAwait(false))
        {
            T? record;
            var row = new CsvRow(csv, headers, hasHeaders);

            try
            {
                record = _rowMapper(row);
            }
            catch (Exception ex)
            {
                var handler = cfg.RowErrorHandler;

                if (handler is not null && handler(ex, row))
                    continue; // handler opted to swallow

                throw;
            }

            if (record is not null)
                yield return record;
        }
    }
}

/// <summary>
///     Represents the current CSV row and provides typed accessors.
/// </summary>
/// <param name="reader">The CSV reader used to access field values.</param>
/// <param name="headers">Header lookup map.</param>
/// <param name="hasHeaders">Whether header records are present.</param>
public readonly struct CsvRow(
    CsvReader reader,
    IReadOnlyDictionary<string, int> headers,
    bool hasHeaders)
{
    private readonly CsvReader _reader = reader;
    private readonly IReadOnlyDictionary<string, int> _headers = headers;
    private readonly bool _hasHeaders = hasHeaders;

    /// <summary>
    ///     Try to read a field by header name and convert it to <typeparamref name="T" />.
    /// </summary>
    /// <param name="name">Header name.</param>
    /// <param name="value">Converted value or <paramref name="defaultValue" />.</param>
    /// <param name="defaultValue">Value used when the field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns><c>true</c> when a value is present and converted; otherwise <c>false</c>.</returns>
    public bool TryGet<T>(string name, out T value, T defaultValue = default!)
    {
        value = defaultValue!;

        if (!_hasHeaders || !_headers.TryGetValue(name, out _))
            return false;

        try
        {
            var field = _reader.GetField<T>(name);

            if (field is null)
                return false;

            value = field;
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    ///     Read a field by header name and return a converted value or <paramref name="defaultValue" />.
    /// </summary>
    /// <param name="name">Header name.</param>
    /// <param name="defaultValue">Value used when the field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>Converted value or <paramref name="defaultValue" />.</returns>
    public T Get<T>(string name, T defaultValue = default!)
    {
        return TryGet(name, out var value, defaultValue)
            ? value
            : defaultValue;
    }

    /// <summary>
    ///     Read a field by index and return a converted value or <paramref name="defaultValue" />.
    /// </summary>
    /// <param name="index">Field index.</param>
    /// <param name="defaultValue">Value used when the field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>Converted value or <paramref name="defaultValue" />.</returns>
    public T GetByIndex<T>(int index, T defaultValue = default!)
    {
        try
        {
            var field = _reader.GetField<T>(index);

            return field is null
                ? defaultValue
                : field;
        }
        catch
        {
            return defaultValue;
        }
    }
}
