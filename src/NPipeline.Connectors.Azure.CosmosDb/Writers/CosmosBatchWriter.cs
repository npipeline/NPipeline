using System.Reflection;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Azure.CosmosDb.Writers;

/// <summary>
///     Cosmos DB writer that writes items in batches using concurrent tasks.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class CosmosBatchWriter<T> : IDatabaseWriter<T>
{
    // LoggerMessage delegates for performance
    private static readonly Action<ILogger, int, string, Exception?> LogBatchWriteSuccess =
        LoggerMessage.Define<int, string>(LogLevel.Debug, new EventId(1, nameof(LogBatchWriteSuccess)),
            "Batch write completed successfully for {Count} items of type {ItemType}");

    private static readonly Action<ILogger, int, int, string, Exception?> LogBatchWritePartialFailure =
        LoggerMessage.Define<int, int, string>(LogLevel.Warning, new EventId(2, nameof(LogBatchWritePartialFailure)),
            "Batch write encountered {FailureCount} failures out of {TotalCount} items for type {ItemType} (ContinueOnError enabled)");

    // Cached reflection lookups - static per T to avoid per-call overhead in hot write paths
    private static readonly PropertyInfo? CachedPartitionKeyProperty =
        typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(p => p.IsDefined(typeof(CosmosPartitionKeyAttribute), true));

    private static readonly PropertyInfo? CachedIdProperty =
        typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(p => p.CanWrite && string.Equals(p.Name, "id", StringComparison.OrdinalIgnoreCase));

    private readonly List<T> _buffer = [];
    private readonly CosmosConfiguration _configuration;

    private readonly Container _container;
    private readonly Func<T, string>? _idSelector;
    private readonly ILogger? _logger;
    private readonly Func<T, PartitionKey>? _partitionKeySelector;
    private bool _disposed;
    private int _failedWriteCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosBatchWriter{T}" /> class.
    /// </summary>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="idSelector">Optional function to extract document ID from item.</param>
    /// <param name="partitionKeySelector">Optional function to extract partition key from item.</param>
    /// <param name="configuration">The Cosmos DB configuration.</param>
    /// <param name="logger">Optional logger for diagnostics.</param>
    public CosmosBatchWriter(
        Container container,
        Func<T, string>? idSelector,
        Func<T, PartitionKey>? partitionKeySelector,
        CosmosConfiguration configuration,
        ILogger? logger = null)
    {
        _container = container;
        _idSelector = idSelector;
        _partitionKeySelector = partitionKeySelector;
        _configuration = configuration;
        _logger = logger;
    }

    /// <summary>
    ///     Gets the number of failed writes when ContinueOnError is enabled.
    /// </summary>
    public int FailedWriteCount => _failedWriteCount;

    /// <summary>
    ///     Writes a single item to the buffer.
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _buffer.Add(item);

        if (_buffer.Count >= _configuration.WriteBatchSize)
            await FlushAsync(cancellationToken);
    }

    /// <summary>
    ///     Writes a batch of items to Cosmos DB.
    /// </summary>
    /// <param name="items">The items to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var itemList = items.ToList();
        var tasks = new List<Task>(itemList.Count);

        foreach (var item in itemList)
        {
            var partitionKey = GetPartitionKey(item);
            var itemWithId = EnsureId(item);

            if (ShouldUseUpsert())
                tasks.Add(_container.UpsertItemAsync(itemWithId, partitionKey, cancellationToken: cancellationToken));
            else
                tasks.Add(_container.CreateItemAsync(itemWithId, partitionKey, cancellationToken: cancellationToken));
        }

        try
        {
            await Task.WhenAll(tasks);

            if (_logger is not null)
                LogBatchWriteSuccess(_logger, itemList.Count, typeof(T).Name, null);
        }
        catch (Exception ex) when (_configuration.ContinueOnError && ex is not OperationCanceledException)
        {
            // await Task.WhenAll unwraps AggregateException — catch Exception to reliably handle any failure
            var failureCount = tasks.Count(t => t.IsFaulted);

            var actualFailureCount = failureCount > 0
                ? failureCount
                : 1;

            Interlocked.Add(ref _failedWriteCount, actualFailureCount);

            if (_logger is not null)
                LogBatchWritePartialFailure(_logger, actualFailureCount, itemList.Count, typeof(T).Name, ex);
        }
    }

    /// <summary>
    ///     Flushes any buffered data to Cosmos DB.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_buffer.Count == 0)
            return;

        var items = _buffer.ToList();
        _buffer.Clear();

        await WriteBatchAsync(items, cancellationToken);
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
}
