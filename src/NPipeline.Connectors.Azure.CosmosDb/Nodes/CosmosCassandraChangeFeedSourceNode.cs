using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.CosmosDb.Nodes;

/// <summary>
///     Placeholder source node for Cassandra change feed.
/// </summary>
/// <typeparam name="T">The output type.</typeparam>
/// <remarks>
///     Cosmos DB Cassandra API does not expose a native change feed equivalent.
/// </remarks>
public sealed class CosmosCassandraChangeFeedSourceNode<T> : SourceNode<T>
{
    /// <inheritdoc />
    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        throw new NotSupportedException(
            "Cassandra change feed is not supported by Azure Cosmos DB Cassandra API in this connector. " +
            "Use query-based polling with CosmosCassandraSourceNode or integrate external CDC tooling.");
    }
}
