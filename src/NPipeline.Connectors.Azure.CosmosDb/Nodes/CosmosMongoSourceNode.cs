using System.Runtime.CompilerServices;
using NPipeline.Connectors.Azure.CosmosDb.Api.Mongo;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.CosmosDb.Nodes;

/// <summary>
///     First-class source node for Cosmos Mongo API.
/// </summary>
/// <typeparam name="T">The item type emitted by the source.</typeparam>
public sealed class CosmosMongoSourceNode<T> : SourceNode<T>
{
    private readonly CosmosConfiguration _configuration;
    private readonly Func<CosmosRow, T>? _mapper;
    private readonly string _query;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosMongoSourceNode{T}" />.
    /// </summary>
    /// <param name="connectionString">Mongo connection string for Cosmos Mongo API.</param>
    /// <param name="databaseId">Database identifier.</param>
    /// <param name="containerId">Collection identifier.</param>
    /// <param name="query">JSON filter document (default: empty filter).</param>
    /// <param name="mapper">Optional row mapper.</param>
    /// <param name="configuration">Optional base configuration overrides.</param>
    public CosmosMongoSourceNode(
        string connectionString,
        string databaseId,
        string containerId,
        string query = "{}",
        Func<CosmosRow, T>? mapper = null,
        CosmosConfiguration? configuration = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(databaseId))
            throw new ArgumentNullException(nameof(databaseId));

        if (string.IsNullOrWhiteSpace(containerId))
            throw new ArgumentNullException(nameof(containerId));

        _configuration = configuration?.Clone() ?? new CosmosConfiguration();
        _configuration.ApiType = CosmosApiType.Mongo;

        _configuration.MongoConnectionString = string.IsNullOrWhiteSpace(_configuration.MongoConnectionString)
            ? connectionString
            : _configuration.MongoConnectionString;

        _configuration.ConnectionString = string.IsNullOrWhiteSpace(_configuration.ConnectionString)
            ? connectionString
            : _configuration.ConnectionString;

        _configuration.DatabaseId = databaseId;
        _configuration.ContainerId = containerId;
        _configuration.Validate();

        _query = string.IsNullOrWhiteSpace(query)
            ? "{}"
            : query;

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
        var adapter = new CosmosMongoApiAdapter();
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
