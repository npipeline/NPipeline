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
/// <typeparam name="T">Record type to deserialize each CSV row to.</typeparam>
public sealed class CsvSourceNode<T> : SourceNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => StorageProviderFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly CsvConfiguration _csvConfiguration;
    private readonly Encoding _encoding;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly StorageUri _uri;

    private CsvSourceNode(
        StorageUri uri,
        CsvConfiguration? configuration,
        Encoding? encoding)
    {
        ArgumentNullException.ThrowIfNull(uri);
        _uri = uri;

        _csvConfiguration = configuration ?? new CsvConfiguration(CultureInfo.InvariantCulture);

        // Set DetectDelimiter on the underlying CsvHelper configuration
        _csvConfiguration.HelperConfiguration.DetectDelimiter = true;

        _encoding = encoding ?? new UTF8Encoding(false);
    }

    /// <summary>
    ///     Construct a CSV source that resolves a storage provider from a resolver at execution time.
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
        : this(uri, configuration, encoding)
    {
        _resolver = resolver;
    }

    /// <summary>
    ///     Construct a CSV source that uses a specific storage provider.
    /// </summary>
    public CsvSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        CsvConfiguration? configuration = null,
        Encoding? encoding = null)
        : this(uri, configuration, encoding)
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

    private static async IAsyncEnumerable<T> Read(
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

        await foreach (var record in csv.GetRecordsAsync<T>(cancellationToken))
        {
            if (record is not null)
                yield return record;
        }
    }
}
