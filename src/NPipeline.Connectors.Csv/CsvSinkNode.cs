using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Exceptions;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Csv;

/// <summary>
///     Sink node that writes items to CSV using a pluggable <see cref="IStorageProvider" />.
/// </summary>
/// <typeparam name="T">Record type to serialize for each CSV row.</typeparam>
public sealed class CsvSinkNode<T> : SinkNode<T>
{
    private readonly CsvConfiguration _csvConfiguration;
    private readonly Encoding _encoding;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly StorageUri _uri;

    /// <summary>
    ///     Construct a CSV sink node that resolves a storage provider from a resolver at execution time.
    /// </summary>
    public CsvSinkNode(
        StorageUri uri,
        IStorageResolver? resolver = null,
        CsvConfiguration? configuration = null,
        Encoding? encoding = null)
    {
        _uri = uri ?? throw new ArgumentNullException(nameof(uri));
        _resolver = resolver;
        _csvConfiguration = configuration ?? new CsvConfiguration(CultureInfo.InvariantCulture);
        _encoding = encoding ?? new UTF8Encoding(false);
    }

    /// <summary>
    ///     Construct a CSV sink node that uses a specific storage provider instance.
    /// </summary>
    public CsvSinkNode(
        IStorageProvider provider,
        StorageUri uri,
        CsvConfiguration? configuration = null,
        Encoding? encoding = null)
        : this(uri, null, configuration, encoding)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
    }

    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = _provider ?? StorageProviderFactory.GetProviderOrThrow(
            _resolver ?? StorageProviderFactory.CreateResolver(),
            _uri);

        if (provider is IStorageProviderMetadataProvider metaProvider)
        {
            var meta = metaProvider.GetMetadata();

            if (!meta.SupportsWrite)
                throw new UnsupportedStorageCapabilityException(_uri, "write", meta.Name);
        }

        await using var stream = await provider.OpenWriteAsync(_uri, cancellationToken).ConfigureAwait(false);
        await using var writer = new StreamWriter(stream, _encoding, 1024, false);
        await using var csv = new CsvWriter(writer, _csvConfiguration);

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            if (item is null)
                continue;

            csv.WriteRecord(item);
            await csv.NextRecordAsync();
        }

        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}
