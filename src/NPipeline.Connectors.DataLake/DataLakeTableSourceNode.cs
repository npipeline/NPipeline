using System.Runtime.CompilerServices;
using NPipeline.Connectors.DataLake.Manifest;
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
    private readonly DateTimeOffset? _asOf;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly string? _snapshotId;
    private readonly StorageUri _tableBasePath;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class for reading the latest snapshot.
    /// </summary>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="resolver">The storage resolver.</param>
    public DataLakeTableSourceNode(
        StorageUri tableBasePath,
        IStorageResolver? resolver = null)
    {
        ArgumentNullException.ThrowIfNull(tableBasePath);

        _tableBasePath = tableBasePath;
        _resolver = resolver;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class with a specific provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="tableBasePath">The base path of the table.</param>
    public DataLakeTableSourceNode(
        IStorageProvider provider,
        StorageUri tableBasePath)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(tableBasePath);

        _provider = provider;
        _tableBasePath = tableBasePath;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class for time travel.
    /// </summary>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="asOf">The timestamp for time travel (returns data as of this point in time).</param>
    /// <param name="resolver">The storage resolver.</param>
    public DataLakeTableSourceNode(
        StorageUri tableBasePath,
        DateTimeOffset asOf,
        IStorageResolver? resolver = null)
        : this(tableBasePath, resolver)
    {
        _asOf = asOf;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class for time travel with provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="asOf">The timestamp for time travel.</param>
    public DataLakeTableSourceNode(
        IStorageProvider provider,
        StorageUri tableBasePath,
        DateTimeOffset asOf)
        : this(provider, tableBasePath)
    {
        _asOf = asOf;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataLakeTableSourceNode{T}" /> class for a specific snapshot.
    /// </summary>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="snapshotId">The snapshot ID to read.</param>
    /// <param name="resolver">The storage resolver.</param>
    public DataLakeTableSourceNode(
        StorageUri tableBasePath,
        string snapshotId,
        IStorageResolver? resolver = null)
        : this(tableBasePath, resolver)
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
    public DataLakeTableSourceNode(
        IStorageProvider provider,
        StorageUri tableBasePath,
        string snapshotId)
        : this(provider, tableBasePath)
    {
        ArgumentNullException.ThrowIfNull(snapshotId);
        _snapshotId = snapshotId;
    }

    /// <inheritdoc />
    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = _provider ?? (_resolver ?? StorageResolver.Default).Resolve(_tableBasePath);

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

            await foreach (var item in ReadFileAsync(provider, fileUri, cancellationToken).ConfigureAwait(false))
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
        var sourceNode = DataLakeParquetOptions.Source<T>(provider, fileUri);

        var dataStream = sourceNode.OpenStream(PipelineContext.CreateDefault(), cancellationToken);

        await foreach (var item in dataStream.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    // Combine keeps the table URI's parameters (credentials, region) on each file's URI.
    private StorageUri BuildFileUri(string relativePath) => _tableBasePath.Combine(relativePath);
}
