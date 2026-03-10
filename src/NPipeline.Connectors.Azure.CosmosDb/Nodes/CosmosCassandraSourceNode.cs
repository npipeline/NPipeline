using System.Runtime.CompilerServices;
using NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.CosmosDb.Nodes;

/// <summary>
///     First-class source node for Cosmos Cassandra API.
/// </summary>
/// <typeparam name="T">The item type emitted by the source.</typeparam>
public sealed class CosmosCassandraSourceNode<T> : SourceNode<T>
{
    private readonly CosmosConfiguration _configuration;
    private readonly Func<CosmosRow, T>? _mapper;
    private readonly string _query;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosCassandraSourceNode{T}" />.
    /// </summary>
    /// <param name="contactPoint">Cassandra contact point host.</param>
    /// <param name="keyspace">Cassandra keyspace.</param>
    /// <param name="query">CQL query to execute.</param>
    /// <param name="port">Cassandra port (Cosmos default: 10350).</param>
    /// <param name="username">Optional username.</param>
    /// <param name="password">Optional password.</param>
    /// <param name="mapper">Optional row mapper.</param>
    /// <param name="configuration">Optional base configuration overrides.</param>
    public CosmosCassandraSourceNode(
        string contactPoint,
        string keyspace,
        string query,
        int port = 10350,
        string? username = null,
        string? password = null,
        Func<CosmosRow, T>? mapper = null,
        CosmosConfiguration? configuration = null)
    {
        if (string.IsNullOrWhiteSpace(contactPoint))
            throw new ArgumentNullException(nameof(contactPoint));

        if (string.IsNullOrWhiteSpace(keyspace))
            throw new ArgumentNullException(nameof(keyspace));

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration?.Clone() ?? new CosmosConfiguration();
        _configuration.ApiType = CosmosApiType.Cassandra;
        _configuration.CassandraContactPoint = contactPoint;
        _configuration.CassandraPort = port;
        _configuration.CassandraUsername ??= username;
        _configuration.CassandraPassword ??= password;
        _configuration.DatabaseId = keyspace;
        _configuration.Validate();

        _query = query;
        _mapper = mapper;
    }

    /// <inheritdoc />
    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var stream = ReadAsync(cancellationToken);
        return new DataStream<T>(stream, $"{GetType().Name}<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<T> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var adapter = new CosmosCassandraApiAdapter();
        var client = await adapter.CreateClientAsync(_configuration, cancellationToken);

        try
        {
            var executor = adapter.CreateSourceExecutor(client, _configuration);
            var rows = await executor.QueryAsync(_query, cancellationToken);

            foreach (var row in rows)
            {
                var mapped = CosmosAdapterNodeMapper<T>.Map(row, _mapper, _configuration.ContinueOnError);

                if (mapped != null)
                    yield return mapped;
            }
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
