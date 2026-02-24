using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Abstractions;

/// <summary>
///     Unified sink executor abstraction across Cosmos SQL, Mongo, and Cassandra APIs.
/// </summary>
/// <typeparam name="T">Type of item being written.</typeparam>
public interface ICosmosSinkExecutor<T>
{
    /// <summary>
    ///     Writes a set of items using the provided strategy.
    /// </summary>
    /// <param name="items">Items to write.</param>
    /// <param name="strategy">Write strategy to apply.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task WriteAsync(
        IEnumerable<T> items,
        CosmosWriteStrategy strategy,
        CancellationToken cancellationToken = default);
}
