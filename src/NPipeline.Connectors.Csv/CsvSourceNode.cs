using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using CsvHelper;
using NPipeline.Connectors.Csv.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

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
    ///     Construct a CSV source that uses attribute-based mapping.
    ///     Properties are mapped using CsvColumnAttribute or convention (PascalCase to lowercase).
    /// </summary>
    /// <param name="uri">The URI of the CSV file to read from.</param>
    /// <param name="resolver">
    ///     The storage resolver used to obtain the storage provider. If <c>null</c>, a default resolver
    ///     created by <see cref="StorageProviderFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="configuration">Optional configuration for CSV reading. If <c>null</c>, default configuration is used.</param>
    /// <param name="encoding">Optional text encoding. If <c>null</c>, UTF-8 without BOM is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    public CsvSourceNode(
        StorageUri uri,
        IStorageResolver? resolver = null,
        CsvConfiguration? configuration = null,
        Encoding? encoding = null)
        : this(uri, configuration, encoding, CsvMapperBuilder.Build<T>())
    {
        _resolver = resolver;
    }

    /// <summary>
    ///     Construct a CSV source that uses a specific storage provider with attribute-based mapping.
    ///     Properties are mapped using CsvColumnAttribute or convention (PascalCase to lowercase).
    /// </summary>
    public CsvSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        CsvConfiguration? configuration = null,
        Encoding? encoding = null)
        : this(uri, configuration, encoding, CsvMapperBuilder.Build<T>())
    {
        ArgumentNullException.ThrowIfNull(provider);
        _provider = provider;
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
