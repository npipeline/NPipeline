using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Api.Sql;

/// <summary>
///     SQL sink executor using Cosmos item APIs.
/// </summary>
internal sealed class CosmosSqlSinkExecutor<T> : ICosmosSinkExecutor<T>
{
    private readonly CosmosConfiguration _configuration;
    private readonly Container _container;
    private readonly Func<T, string>? _idSelector;
    private readonly Func<T, PartitionKey>? _partitionKeySelector;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosSqlSinkExecutor{T}" />.
    /// </summary>
    public CosmosSqlSinkExecutor(
        CosmosClient client,
        CosmosConfiguration configuration,
        Func<T, string>? idSelector,
        Func<T, PartitionKey>? partitionKeySelector = null)
    {
        if (string.IsNullOrWhiteSpace(configuration.DatabaseId))
            throw new InvalidOperationException("DatabaseId is required for SQL sink execution.");

        if (string.IsNullOrWhiteSpace(configuration.ContainerId))
            throw new InvalidOperationException("ContainerId is required for SQL sink execution.");

        _container = client.GetContainer(configuration.DatabaseId, configuration.ContainerId);
        _configuration = configuration;
        _idSelector = idSelector;
        _partitionKeySelector = partitionKeySelector;
    }

    /// <inheritdoc />
    public async Task WriteAsync(IEnumerable<T> items, CosmosWriteStrategy strategy, CancellationToken cancellationToken = default)
    {
        var itemList = items.ToList();

        if (itemList.Count == 0)
            return;

        switch (strategy)
        {
            case CosmosWriteStrategy.PerRow:
                foreach (var item in itemList)
                {
                    var partitionKey = GetPartitionKey(item);
                    await _container.CreateItemAsync(EnsureId(item), partitionKey, cancellationToken: cancellationToken);
                }

                break;

            case CosmosWriteStrategy.Upsert:
                foreach (var item in itemList)
                {
                    var partitionKey = GetPartitionKey(item);
                    await _container.UpsertItemAsync(EnsureId(item), partitionKey, cancellationToken: cancellationToken);
                }

                break;

            case CosmosWriteStrategy.Batch:
            case CosmosWriteStrategy.Bulk:
                using (var semaphore = new SemaphoreSlim(_configuration.MaxConcurrentOperations))
                {
                    var tasks = itemList.Select(async item =>
                    {
                        await semaphore.WaitAsync(cancellationToken);

                        try
                        {
                            var partitionKey = GetPartitionKey(item);
                            await _container.UpsertItemAsync(EnsureId(item), partitionKey, cancellationToken: cancellationToken);
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    });

                    await Task.WhenAll(tasks);
                }

                break;

            case CosmosWriteStrategy.TransactionalBatch:
                // Transactional batch supports max 100 operations and requires all items in same partition
                if (itemList.Count > 100)
                {
                    throw new ArgumentException(
                        $"TransactionalBatch supports a maximum of 100 items, but {itemList.Count} were provided. " +
                        $"Use {nameof(CosmosWriteStrategy.Bulk)} or {nameof(CosmosWriteStrategy.Batch)} for larger datasets.");
                }

                // Validate all items have the same partition key
                var partitionKeys = itemList.Select(GetPartitionKey).Distinct().ToList();

                if (partitionKeys.Count > 1)
                {
                    throw new ArgumentException(
                        $"TransactionalBatch requires all items to have the same partition key. " +
                        $"Found {partitionKeys.Count} different partition keys. " +
                        $"Use {nameof(CosmosWriteStrategy.Bulk)} or {nameof(CosmosWriteStrategy.Batch)} for cross-partition writes.");
                }

                // For transactional batch, use the consistent partition key
                var batchPartitionKey = partitionKeys.FirstOrDefault(PartitionKey.None);
                var batch = _container.CreateTransactionalBatch(batchPartitionKey);

                foreach (var item in itemList)
                {
                    batch.UpsertItem(EnsureId(item));
                }

                using (var response = await batch.ExecuteAsync(cancellationToken))
                {
                    if (!response.IsSuccessStatusCode)
                        throw new InvalidOperationException($"Transactional batch failed with status {response.StatusCode}.");
                }

                break;

            default:
                throw new NotSupportedException($"Write strategy '{strategy}' is not supported.");
        }
    }

    private PartitionKey GetPartitionKey(T item)
    {
        if (_partitionKeySelector != null)
            return _partitionKeySelector(item);

        return PartitionKey.None;
    }

    private T EnsureId(T item)
    {
        if (_idSelector == null)
            return item;

        var id = _idSelector(item);

        var property = typeof(T).GetProperties()
            .FirstOrDefault(p => string.Equals(p.Name, "id", StringComparison.OrdinalIgnoreCase) && p.CanWrite);

        property?.SetValue(item, id);
        return item;
    }
}
