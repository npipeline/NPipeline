using Cassandra;

namespace NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;

/// <summary>
///     Holds Cassandra cluster/session resources.
/// </summary>
public sealed class CassandraClientContext : IAsyncDisposable
{
    /// <summary>
    ///     Initializes a new instance of <see cref="CassandraClientContext" />.
    /// </summary>
    public CassandraClientContext(ICluster cluster, ISession session)
    {
        Cluster = cluster;
        Session = session;
    }

    /// <summary>
    ///     Gets the Cassandra cluster.
    /// </summary>
    public ICluster Cluster { get; }

    /// <summary>
    ///     Gets the Cassandra session.
    /// </summary>
    public ISession Session { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await Session.ShutdownAsync();
        Cluster.Dispose();
    }
}
