using System.Globalization;
using System.Text;
using CsvHelper;
using NPipeline.Connectors.Csv.Mapping;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Csv;

/// <summary>
///     Sink node that writes items to CSV using a pluggable <see cref="IStorageProvider" />.
/// </summary>
/// <typeparam name="T">Record type to serialize for each CSV row.</typeparam>
public sealed class CsvSinkNode<T> : SinkNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver =
        new(() => StorageProviderFactory.CreateResolver());

    private readonly CsvConfiguration _csvConfiguration;
    private readonly Encoding _encoding;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly StorageUri _uri;
    private readonly Action<CsvWriter, T>? _writerMapper;

    private CsvSinkNode(
        StorageUri uri,
        CsvConfiguration? configuration,
        Encoding? encoding,
        Action<CsvWriter, T>? writerMapper = null)
    {
        ArgumentNullException.ThrowIfNull(uri);
        _uri = uri;
        _csvConfiguration = configuration ?? new CsvConfiguration(CultureInfo.InvariantCulture);
        _encoding = encoding ?? new UTF8Encoding(false);
        _writerMapper = writerMapper;
    }

    /// <summary>
    ///     Construct a CSV sink node that uses attribute-based mapping.
    ///     Properties are mapped using CsvColumnAttribute or convention (PascalCase to lowercase).
    /// </summary>
    public CsvSinkNode(
        StorageUri uri,
        IStorageResolver? resolver = null,
        CsvConfiguration? configuration = null,
        Encoding? encoding = null)
        : this(uri, configuration, encoding, CsvWriterMapperBuilder.Build<T>())
    {
        _resolver = resolver ?? DefaultResolver.Value;
    }

    /// <summary>
    ///     Construct a CSV sink node that uses a specific storage provider with attribute-based mapping.
    ///     Properties are mapped using CsvColumnAttribute or convention (PascalCase to lowercase).
    /// </summary>
    public CsvSinkNode(
        IStorageProvider provider,
        StorageUri uri,
        CsvConfiguration? configuration = null,
        Encoding? encoding = null)
        : this(uri, configuration, encoding, CsvWriterMapperBuilder.Build<T>())
    {
        ArgumentNullException.ThrowIfNull(provider);
        _provider = provider;
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = _provider ?? StorageProviderFactory.GetProviderOrThrow(
            _resolver ?? throw new InvalidOperationException("No storage resolver configured for CsvSinkNode."),
            _uri);

        if (provider is IStorageProviderMetadataProvider metaProvider)
        {
            var meta = metaProvider.GetMetadata();

            if (!meta.SupportsWrite)
                throw new UnsupportedStorageCapabilityException(_uri, "write", meta.Name);
        }

        await using var stream = await provider.OpenWriteAsync(_uri, cancellationToken).ConfigureAwait(false);
        await using var writer = new StreamWriter(stream, _encoding, _csvConfiguration.BufferSize, false);
        await using var csv = new CsvWriter(writer, _csvConfiguration.HelperConfiguration);

        var type = typeof(T);

        var shouldWriteHeader = _csvConfiguration.HelperConfiguration.HasHeaderRecord
                                && ShouldWriteHeader(type);

        // For primitive types, use CsvHelper's built-in WriteRecord instead of the mapper
        var useMapper = _writerMapper is not null && !type.IsPrimitive && type != typeof(string);

        if (shouldWriteHeader)
        {
            if (useMapper)
            {
                var columnNames = CsvWriterMapperBuilder.GetColumnNames<T>();

                foreach (var columnName in columnNames)
                {
                    csv.WriteField(columnName);
                }

                await csv.NextRecordAsync();
            }
            else
            {
                csv.WriteHeader(type);
                await csv.NextRecordAsync();
            }
        }

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            if (item is null)
                continue;

            if (useMapper)
                _writerMapper!(csv, item);
            else
                csv.WriteRecord(item);

            await csv.NextRecordAsync();
        }

        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static bool ShouldWriteHeader(Type type)
    {
        // Avoid writing headers for primitives/strings where the header would be meaningless (e.g., "Int32").
        if (type.IsPrimitive || type.IsEnum || type == typeof(string))
            return false;

        return true;
    }
}
