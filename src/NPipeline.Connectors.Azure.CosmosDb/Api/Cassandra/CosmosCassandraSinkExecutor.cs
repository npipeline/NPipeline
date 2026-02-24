using Cassandra;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;

/// <summary>
///     Cassandra sink executor using CQL statements.
/// </summary>
internal sealed class CosmosCassandraSinkExecutor<T> : ICosmosSinkExecutor<T>
{
    private readonly CosmosConfiguration _configuration;
    private readonly ISession _session;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosCassandraSinkExecutor{T}" />.
    /// </summary>
    public CosmosCassandraSinkExecutor(ISession session, CosmosConfiguration configuration)
    {
        _session = session;
        _configuration = configuration;
    }

    /// <inheritdoc />
    public async Task WriteAsync(IEnumerable<T> items, CosmosWriteStrategy strategy, CancellationToken cancellationToken = default)
    {
        var materialized = items.ToList();

        if (materialized.Count == 0)
            return;

        switch (strategy)
        {
            case CosmosWriteStrategy.Upsert:
            case CosmosWriteStrategy.PerRow:
                foreach (var item in materialized)
                {
                    await ExecuteOneAsync(item, cancellationToken);
                }

                break;

            case CosmosWriteStrategy.Batch:
            {
                var batch = new BatchStatement();

                foreach (var item in materialized)
                {
                    batch.Add(ToStatement(item));
                }

                await _session.ExecuteAsync(batch).WaitAsync(cancellationToken);
                break;
            }

            case CosmosWriteStrategy.Bulk:
                using (var semaphore = new SemaphoreSlim(_configuration.MaxConcurrentOperations))
                {
                    var tasks = materialized.Select(async item =>
                    {
                        await semaphore.WaitAsync(cancellationToken);

                        try
                        {
                            await ExecuteOneAsync(item, cancellationToken);
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
                throw new NotSupportedException("TransactionalBatch is not supported by Cassandra adapter.");

            default:
                throw new NotSupportedException($"Write strategy '{strategy}' is not supported.");
        }
    }

    private async Task ExecuteOneAsync(T item, CancellationToken cancellationToken)
    {
        var statement = ToStatement(item);
        await _session.ExecuteAsync(statement).WaitAsync(cancellationToken);
    }

    private static Statement ToStatement(T item)
    {
        return item switch
        {
            CassandraStatementRequest request when request.Parameters.Length == 0 => new SimpleStatement(request.Cql),
            CassandraStatementRequest request => new SimpleStatement(request.Cql, request.Parameters),
            string cql => new SimpleStatement(cql),
            _ => throw new NotSupportedException(
                $"Unsupported Cassandra sink item type '{typeof(T).FullName}'. Use '{nameof(CassandraStatementRequest)}' or raw CQL string."),
        };
    }
}
