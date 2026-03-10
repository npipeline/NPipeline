using System.Runtime.CompilerServices;
using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.Connectors.Parquet;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake;

/// <summary>
///     Source node that reads data from a Data Lake table.
///     Reads the manifest, resolves data file URIs, and streams row groups across all files.
///     Supports time travel via the <c>asOf</c> parameter.
/// </summary>
/// <typeparam name="T">The record type being read.</typeparam>
public sealed class DataLakeTableSourceNode<T> : SourceNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver =
        new(() => StorageProviderFactory.CreateResolver());

    private readonly DateTimeOffset? _asOf;
    private readonly ParquetConfiguration _configuration;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly string? _snapshotId;
    private readonly StorageUri _tableBasePath;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class for reading the latest snapshot.
    /// </summary>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="resolver">The storage resolver.</param>
    /// <param name="configuration">Optional Parquet configuration.</param>
    public DataLakeTableSourceNode(
        StorageUri tableBasePath,
        IStorageResolver? resolver = null,
        ParquetConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(tableBasePath);

        _tableBasePath = tableBasePath;
        _resolver = resolver;
        _configuration = configuration ?? new ParquetConfiguration();
        _configuration.Validate();
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class with a specific provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="configuration">Optional Parquet configuration.</param>
    public DataLakeTableSourceNode(
        IStorageProvider provider,
        StorageUri tableBasePath,
        ParquetConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(tableBasePath);

        _provider = provider;
        _tableBasePath = tableBasePath;
        _configuration = configuration ?? new ParquetConfiguration();
        _configuration.Validate();
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class for time travel.
    /// </summary>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="asOf">The timestamp for time travel (returns data as of this point in time).</param>
    /// <param name="resolver">The storage resolver.</param>
    /// <param name="configuration">Optional Parquet configuration.</param>
    public DataLakeTableSourceNode(
        StorageUri tableBasePath,
        DateTimeOffset asOf,
        IStorageResolver? resolver = null,
        ParquetConfiguration? configuration = null)
        : this(tableBasePath, resolver, configuration)
    {
        _asOf = asOf;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class for time travel with provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="asOf">The timestamp for time travel.</param>
    /// <param name="configuration">Optional Parquet configuration.</param>
    public DataLakeTableSourceNode(
        IStorageProvider provider,
        StorageUri tableBasePath,
        DateTimeOffset asOf,
        ParquetConfiguration? configuration = null)
        : this(provider, tableBasePath, configuration)
    {
        _asOf = asOf;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class for a specific snapshot.
    /// </summary>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="snapshotId">The snapshot ID to read.</param>
    /// <param name="resolver">The storage resolver.</param>
    /// <param name="configuration">Optional Parquet configuration.</param>
    public DataLakeTableSourceNode(
        StorageUri tableBasePath,
        string snapshotId,
        IStorageResolver? resolver = null,
        ParquetConfiguration? configuration = null)
        : this(tableBasePath, resolver, configuration)
    {
        ArgumentNullException.ThrowIfNull(snapshotId);
        _snapshotId = snapshotId;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class for a specific snapshot with provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="snapshotId">The snapshot ID to read.</param>
    /// <param name="configuration">Optional Parquet configuration.</param>
    public DataLakeTableSourceNode(
        IStorageProvider provider,
        StorageUri tableBasePath,
        string snapshotId,
        ParquetConfiguration? configuration = null)
        : this(provider, tableBasePath, configuration)
    {
        ArgumentNullException.ThrowIfNull(snapshotId);
        _snapshotId = snapshotId;
    }

    /// <inheritdoc />
    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = _provider ?? StorageProviderFactory.GetProviderOrThrow(
            _resolver ?? DefaultResolver.Value,
            _tableBasePath);

        var stream = ReadAllAsync(provider, cancellationToken);
        return new DataStream<T>(stream, $"DataLakeTableSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<T> ReadAllAsync(
        IStorageProvider provider,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var manifestReader = new ManifestReader(provider, _tableBasePath);

        // Get manifest entries based on the query type
        IReadOnlyList<ManifestEntry> entries;

        if (_snapshotId is not null)
        {
            entries = await manifestReader.ReadBySnapshotAsync(_snapshotId, cancellationToken)
                .ConfigureAwait(false);
        }
        else if (_asOf.HasValue)
        {
            entries = await manifestReader.ReadAsOfAsync(_asOf.Value, cancellationToken)
                .ConfigureAwait(false);
        }
        else
            entries = await manifestReader.ReadAllAsync(cancellationToken).ConfigureAwait(false);

        if (entries.Count == 0)
            yield break;

        // Deduplicate by path (keep latest version)
        var deduplicatedEntries = entries
            .GroupBy(e => e.Path)
            .Select(g => g.OrderByDescending(e => e.WrittenAt).First())
            .OrderBy(e => e.Path, StringComparer.OrdinalIgnoreCase)
            .ToList();

        // Stream data from each file
        foreach (var entry in deduplicatedEntries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fileUri = BuildFileUri(entry.Path);

            await foreach (var item in ReadFileAsync(provider, fileUri, cancellationToken))
            {
                yield return item;
            }
        }
    }

    private async IAsyncEnumerable<T> ReadFileAsync(
        IStorageProvider provider,
        StorageUri fileUri,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Use ParquetSourceNode to read the file
        var sourceNode = new ParquetSourceNode<T>(provider, fileUri, _configuration);

        var dataStream = sourceNode.OpenStream(PipelineContext.Default, cancellationToken);

        await foreach (var item in dataStream.WithCancellation(cancellationToken))
        {
            yield return item;
        }
    }

    private StorageUri BuildFileUri(string relativePath)
    {
        var basePath = _tableBasePath.Path?.TrimStart('/') ?? string.Empty;

        var fullPath = string.IsNullOrEmpty(basePath)
            ? $"/{relativePath.TrimStart('/')}"
            : $"/{basePath}/{relativePath.TrimStart('/')}";

        return StorageUri.Parse($"{_tableBasePath.Scheme}://{_tableBasePath.Host}{fullPath}");
    }
}
