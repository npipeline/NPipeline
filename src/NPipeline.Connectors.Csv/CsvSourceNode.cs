using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
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
        _uri = uri ?? throw new ArgumentNullException(nameof(uri));

        _csvConfiguration = configuration ?? new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            DetectDelimiter = true,
        };

        _encoding = encoding ?? new UTF8Encoding(false);
    }

    /// <summary>
    ///     Construct a CSV source that resolves a storage provider from a resolver at execution time.
    /// </summary>
    public CsvSourceNode(
        StorageUri uri,
        IStorageResolver resolver,
        CsvConfiguration? configuration = null,
        Encoding? encoding = null)
        : this(uri, configuration, encoding)
    {
        _resolver = resolver ?? throw new ArgumentNullException(nameof(resolver));
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
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
    }

    public override IDataPipe<T> Execute(PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = _provider ?? StorageProviderFactory.GetProviderOrThrow(
            _resolver ?? throw new InvalidOperationException("No storage resolver configured for CsvSourceNode."),
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
        using var csv = new CsvReader(reader, cfg);

        await foreach (var record in csv.GetRecordsAsync<T>(cancellationToken))
        {
            if (record is not null)
                yield return record;
        }
    }
}
