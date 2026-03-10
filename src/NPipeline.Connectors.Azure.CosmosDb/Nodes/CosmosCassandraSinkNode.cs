using NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.CosmosDb.Nodes;

/// <summary>
///     First-class sink node for Cosmos Cassandra API.
/// </summary>
/// <typeparam name="T">The item type consumed by the sink.</typeparam>
/// <remarks>
///     Sink item type should typically be <see cref="CassandraStatementRequest" /> or raw CQL string.
/// </remarks>
public sealed class CosmosCassandraSinkNode<T> : SinkNode<T>
{
    private readonly CosmosConfiguration _configuration;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosCassandraSinkNode{T}" />.
    /// </summary>
    /// <param name="contactPoint">Cassandra contact point host.</param>
    /// <param name="keyspace">Cassandra keyspace.</param>
    /// <param name="writeStrategy">Write strategy.</param>
    /// <param name="port">Cassandra port (Cosmos default: 10350).</param>
    /// <param name="username">Optional username.</param>
    /// <param name="password">Optional password.</param>
    /// <param name="configuration">Optional base configuration overrides.</param>
    public CosmosCassandraSinkNode(
        string contactPoint,
        string keyspace,
        CosmosWriteStrategy writeStrategy = CosmosWriteStrategy.Batch,
        int port = 10350,
        string? username = null,
        string? password = null,
        CosmosConfiguration? configuration = null)
    {
        if (string.IsNullOrWhiteSpace(contactPoint))
            throw new ArgumentNullException(nameof(contactPoint));

        if (string.IsNullOrWhiteSpace(keyspace))
            throw new ArgumentNullException(nameof(keyspace));

        _configuration = configuration?.Clone() ?? new CosmosConfiguration();
        _configuration.ApiType = CosmosApiType.Cassandra;
        _configuration.CassandraContactPoint = contactPoint;
        _configuration.CassandraPort = port;
        _configuration.CassandraUsername ??= username;
        _configuration.CassandraPassword ??= password;
        _configuration.DatabaseId = keyspace;
        _configuration.WriteStrategy = writeStrategy;
        _configuration.Validate();
    }

    /// <inheritdoc />
    public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var adapter = new CosmosCassandraApiAdapter();
        var client = await adapter.CreateClientAsync(_configuration, cancellationToken);

        try
        {
            var sink = adapter.CreateSinkExecutor<T>(client, _configuration);
            var items = new List<T>();

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                items.Add(item);
            }

            await sink.WriteAsync(items, _configuration.WriteStrategy, cancellationToken);
        }
        finally
        {
            switch (client)
            {
                case IAsyncDisposable asyncDisposable:
                    await asyncDisposable.DisposeAsync();
                    break;
                case IDisposable disposable:
                    disposable.Dispose();
                    break;
            }
        }
    }
}
