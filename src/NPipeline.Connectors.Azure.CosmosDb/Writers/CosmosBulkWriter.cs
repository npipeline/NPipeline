using System.Reflection;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Azure.CosmosDb.Writers;

/// <summary>
///     Cosmos DB writer that uses bulk execution for high-throughput writes.
///     Requires AllowBulkExecution = true on the CosmosClient.
///     <para>
///         <b>Important:</b> This writer uses fire-and-forget semantics for individual writes.
///         You MUST call <see cref="FlushAsync" /> or <see cref="DisposeAsync" /> to ensure all
///         pending operations complete. Failure to do so may result in data loss.
///     </para>
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class CosmosBulkWriter<T> : IDatabaseWriter<T>
{
    // LoggerMessage delegates for performance
    private static readonly Action<ILogger, string, Exception?> LogBulkWriteFailed =
        LoggerMessage.Define<string>(LogLevel.Warning, new EventId(1, nameof(LogBulkWriteFailed)),
            "Bulk write operation failed for item type {ItemType}");

    private static readonly Action<ILogger, string, Exception?> LogFlushNoPending =
        LoggerMessage.Define<string>(LogLevel.Debug, new EventId(2, nameof(LogFlushNoPending)),
            "FlushAsync called with no pending operations for item type {ItemType}");

    private static readonly Action<ILogger, int, string, Exception?> LogFlushStarting =
        LoggerMessage.Define<int, string>(LogLevel.Debug, new EventId(3, nameof(LogFlushStarting)),
            "Flushing {Count} pending bulk operations for item type {ItemType}");

    private static readonly Action<ILogger, int, string, Exception?> LogFlushSuccess =
        LoggerMessage.Define<int, string>(LogLevel.Debug, new EventId(4, nameof(LogFlushSuccess)),
            "Successfully flushed {Count} bulk operations for item type {ItemType}");

    private static readonly Action<ILogger, int, int, string, Exception?> LogFlushPartialFailure =
        LoggerMessage.Define<int, int, string>(LogLevel.Warning, new EventId(5, nameof(LogFlushPartialFailure)),
            "FlushAsync completed with {FailureCount} failures out of {TotalCount} operations for item type {ItemType} (ContinueOnError enabled)");

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
    private readonly object _lockObj = new();
    private readonly ILogger? _logger;
    private readonly Func<T, PartitionKey>? _partitionKeySelector;
    private readonly List<Task> _pendingTasks = [];
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosBulkWriter{T}" /> class.
    /// </summary>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="idSelector">Optional function to extract document ID from item.</param>
    /// <param name="partitionKeySelector">Optional function to extract partition key from item.</param>
    /// <param name="configuration">The Cosmos DB configuration.</param>
    /// <param name="logger">Optional logger for diagnostics.</param>
    public CosmosBulkWriter(
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
    ///     Gets the number of pending operations that have not yet completed.
    /// </summary>
    public int PendingCount { get; private set; }

    /// <summary>
    ///     Gets the number of successfully completed operations.
    /// </summary>
    public int CompletedCount { get; private set; }

    /// <summary>
    ///     Gets the number of failed operations when ContinueOnError is enabled.
    /// </summary>
    public int FailedCount { get; private set; }

    /// <summary>
    ///     Writes a single item using bulk execution.
    ///     <para>
    ///         <b>Warning:</b> This method returns before the write completes. Call <see cref="FlushAsync" />
    ///         to wait for all pending operations to complete.
    ///     </para>
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the write has been queued (not when it completes).</returns>
    public Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var partitionKey = GetPartitionKey(item);
        var itemWithId = EnsureId(item);

        Task task;

        if (ShouldUseUpsert())
            task = _container.UpsertItemAsync(itemWithId, partitionKey, cancellationToken: cancellationToken);
        else
            task = _container.CreateItemAsync(itemWithId, partitionKey, cancellationToken: cancellationToken);

        lock (_lockObj)
        {
            _pendingTasks.Add(task);
            PendingCount++;
        }

        // Track completion for observability
        _ = task.ContinueWith(t =>
        {
            lock (_lockObj)
            {
                PendingCount--;

                if (t.IsFaulted)
                {
                    FailedCount++;

                    if (_logger is not null)
                        LogBulkWriteFailed(_logger, typeof(T).Name, t.Exception);
                }
                else
                    CompletedCount++;
            }
        }, TaskContinuationOptions.ExecuteSynchronously);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Writes a batch of items using bulk execution.
    ///     <para>
    ///         <b>Warning:</b> This method returns before all writes complete. Call <see cref="FlushAsync" />
    ///         to wait for all pending operations to complete.
    ///     </para>
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
    ///     Waits for all pending bulk operations to complete.
    ///     <para>
    ///         This method MUST be called after writing all items to ensure data is persisted.
    ///         Alternatively, dispose the writer which will automatically flush.
    ///     </para>
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        List<Task> tasksToAwait;

        lock (_lockObj)
        {
            if (_pendingTasks.Count == 0)
            {
                if (_logger is not null)
                    LogFlushNoPending(_logger, typeof(T).Name, null);

                return;
            }

            tasksToAwait = [.. _pendingTasks];
            _pendingTasks.Clear();
        }

        var totalCount = tasksToAwait.Count;

        if (_logger is not null)
            LogFlushStarting(_logger, totalCount, typeof(T).Name, null);

        try
        {
            await Task.WhenAll(tasksToAwait);

            if (_logger is not null)
                LogFlushSuccess(_logger, totalCount, typeof(T).Name, null);
        }
        catch (Exception ex) when (_configuration.ContinueOnError && ex is not OperationCanceledException)
        {
            // await Task.WhenAll unwraps AggregateException — catch Exception to reliably handle any failure
            var failureCount = tasksToAwait.Count(t => t.IsFaulted);

            if (_logger is not null)
            {
                LogFlushPartialFailure(_logger, failureCount > 0
                    ? failureCount
                    : 1, totalCount, typeof(T).Name, ex);
            }
        }
    }

    /// <summary>
    ///     Disposes the writer, flushing any pending operations.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            await FlushAsync();
            _disposed = true;
            GC.SuppressFinalize(this);
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
