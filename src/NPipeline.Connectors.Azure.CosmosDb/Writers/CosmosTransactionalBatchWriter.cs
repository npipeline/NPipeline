using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Azure.CosmosDb.Writers;

/// <summary>
///     Cosmos DB writer that writes items using transactional batches.
///     All items in the same partition are written atomically.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class CosmosTransactionalBatchWriter<T> : IDatabaseWriter<T>
{
    // Cached reflection lookups - static per T to avoid per-call overhead in hot write paths
    private static readonly PropertyInfo? CachedPartitionKeyProperty =
        typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(p => p.IsDefined(typeof(CosmosPartitionKeyAttribute), true));

    private static readonly PropertyInfo? CachedIdProperty =
        typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(p => p.CanWrite && string.Equals(p.Name, "id", StringComparison.OrdinalIgnoreCase));

    private readonly CosmosConfiguration _configuration;
    private readonly Container _container;
    private readonly Func<T, string>? _idSelector;
    private readonly ConcurrentDictionary<string, PartitionBuffer> _partitionBuffers = new();
    private readonly Func<T, PartitionKey>? _partitionKeySelector;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosTransactionalBatchWriter{T}" /> class.
    /// </summary>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="idSelector">Optional function to extract document ID from item.</param>
    /// <param name="partitionKeySelector">Optional function to extract partition key from item.</param>
    /// <param name="configuration">The Cosmos DB configuration.</param>
    public CosmosTransactionalBatchWriter(
        Container container,
        Func<T, string>? idSelector,
        Func<T, PartitionKey>? partitionKeySelector,
        CosmosConfiguration configuration)
    {
        _container = container;
        _idSelector = idSelector;
        _partitionKeySelector = partitionKeySelector;
        _configuration = configuration;
    }

    /// <summary>
    ///     Writes a single item to the buffer.
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var partitionKey = GetPartitionKey(item);
        var partitionKeyString = partitionKey.ToString() ?? "_none";

        // Get or create the partition buffer atomically - buffer and lock are always created together
        var partitionBuffer = _partitionBuffers.GetOrAdd(partitionKeyString, _ => new PartitionBuffer());

        List<T>? bufferToFlush = null;

        lock (partitionBuffer.Lock)
        {
            partitionBuffer.Buffer.Add(item);

            if (partitionBuffer.Buffer.Count >= _configuration.WriteBatchSize)
            {
                // Take a copy of the buffer for flushing and clear the original
                bufferToFlush = new List<T>(partitionBuffer.Buffer);
                partitionBuffer.Buffer.Clear();
            }
        }

        // Flush outside the lock to avoid holding the lock during I/O
        if (bufferToFlush != null)
            await FlushPartitionAsync(partitionKey, bufferToFlush, cancellationToken);
    }

    /// <summary>
    ///     Writes a batch of items to Cosmos DB using transactional batches.
    /// </summary>
    /// <param name="items">The items to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        foreach (var item in items)
        {
            await WriteAsync(item, cancellationToken);
        }
    }

    /// <summary>
    ///     Flushes all buffered data to Cosmos DB.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var tasks = new List<Task>();

        foreach (var kvp in _partitionBuffers)
        {
            var partitionKeyString = kvp.Key;
            var partitionBuffer = kvp.Value;

            List<T>? bufferToFlush = null;

            lock (partitionBuffer.Lock)
            {
                if (partitionBuffer.Buffer.Count > 0)
                {
                    bufferToFlush = new List<T>(partitionBuffer.Buffer);
                    partitionBuffer.Buffer.Clear();
                }
            }

            if (bufferToFlush != null && bufferToFlush.Count > 0)
            {
                var partitionKey = partitionKeyString == "_none"
                    ? PartitionKey.None
                    : new PartitionKey(partitionKeyString);

                tasks.Add(FlushPartitionAsync(partitionKey, bufferToFlush, cancellationToken));
            }
        }

        await Task.WhenAll(tasks);
        _partitionBuffers.Clear();
    }

    /// <summary>
    ///     Disposes the writer.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            await FlushAsync();
            _disposed = true;
        }
    }

    private async Task FlushPartitionAsync(PartitionKey partitionKey, List<T> items, CancellationToken cancellationToken)
    {
        if (items.Count == 0)
            return;

        // Create a copy for processing - do not mutate the input list
        var batchItems = items.ToList();

        // Transactional batches support up to 100 operations
        const int maxBatchSize = 100;

        for (var i = 0; i < batchItems.Count; i += maxBatchSize)
        {
            var batchSlice = batchItems.GetRange(i, Math.Min(maxBatchSize, batchItems.Count - i));
            await ExecuteTransactionalBatchAsync(partitionKey, batchSlice, cancellationToken);
        }
    }

    private async Task ExecuteTransactionalBatchAsync(PartitionKey partitionKey, List<T> items, CancellationToken cancellationToken)
    {
        var batch = _container.CreateTransactionalBatch(partitionKey);

        foreach (var item in items)
        {
            var itemWithId = EnsureId(item);

            if (ShouldUseUpsert())
                batch.UpsertItem(itemWithId);
            else
                batch.CreateItem(itemWithId);
        }

        try
        {
            using var response = await batch.ExecuteAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new CosmosException(
                    $"Transactional batch failed with status {response.StatusCode}",
                    response.StatusCode,
                    0,
                    response.ActivityId,
                    response.RequestCharge);
            }
        }
        catch (CosmosException) when (_configuration.ContinueOnError)
        {
            // Transaction failed - continue if configured
        }
    }

    private PartitionKey GetPartitionKey(T item)
    {
        if (_partitionKeySelector != null)
            return _partitionKeySelector(item);

        if (CachedPartitionKeyProperty != null)
        {
            var value = CachedPartitionKeyProperty.GetValue(item);

            return value != null
                ? new PartitionKey(value.ToString())
                : PartitionKey.None;
        }

        return PartitionKey.None;
    }

    private T EnsureId(T item)
    {
        if (_idSelector != null)
        {
            var id = _idSelector(item);
            CachedIdProperty?.SetValue(item, id);
        }

        return item;
    }

    private bool ShouldUseUpsert()
    {
        return _configuration.WriteStrategy switch
        {
            CosmosWriteStrategy.Insert => false,
            CosmosWriteStrategy.Upsert => true,
            _ => _configuration.UseUpsert,
        };
    }

    /// <summary>
    ///     Holds the buffer and lock together to ensure atomic access.
    /// </summary>
    private sealed class PartitionBuffer
    {
        public List<T> Buffer { get; } = [];
        public object Lock { get; } = new();
    }
}
