using System.Net;
using System.Reflection;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Azure.CosmosDb.Writers;

/// <summary>
///     Cosmos DB writer that writes items one at a time using CreateItemAsync or UpsertItemAsync.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class CosmosPerRowWriter<T> : IDatabaseWriter<T>
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
    private readonly Func<T, PartitionKey>? _partitionKeySelector;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosPerRowWriter{T}" /> class.
    /// </summary>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="idSelector">Optional function to extract document ID from item.</param>
    /// <param name="partitionKeySelector">Optional function to extract partition key from item.</param>
    /// <param name="configuration">The Cosmos DB configuration.</param>
    public CosmosPerRowWriter(
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
    ///     Writes a single item to Cosmos DB.
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var partitionKey = GetPartitionKey(item);
        var itemWithId = EnsureId(item);

        try
        {
            if (ShouldUseUpsert())
                await _container.UpsertItemAsync(itemWithId, partitionKey, cancellationToken: cancellationToken);
            else
                await _container.CreateItemAsync(itemWithId, partitionKey, cancellationToken: cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict && _configuration.ContinueOnError)
        {
            // Document already exists - continue if configured
        }
    }

    /// <summary>
    ///     Writes a batch of items to Cosmos DB.
    /// </summary>
    /// <param name="items">The items to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
        {
            await WriteAsync(item, cancellationToken);
        }
    }

    /// <summary>
    ///     Flushes any buffered data. No-op for per-row writer.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A completed task.</returns>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Disposes the writer.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
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

        // Fall back to None partition key
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
