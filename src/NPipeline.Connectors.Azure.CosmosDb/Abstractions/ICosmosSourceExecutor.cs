namespace NPipeline.Connectors.Azure.CosmosDb.Abstractions;

/// <summary>
///     Unified source executor abstraction across Cosmos SQL, Mongo, and Cassandra APIs.
/// </summary>
public interface ICosmosSourceExecutor
{
    /// <summary>
    ///     Executes a query and returns rows as dictionaries.
    /// </summary>
    /// <param name="query">Query text (SQL/CQL) or JSON filter (Mongo).</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Materialized rows represented as dictionaries.</returns>
    Task<IReadOnlyList<IDictionary<string, object?>>> QueryAsync(
        string query,
        CancellationToken cancellationToken = default);
}
