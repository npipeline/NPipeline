using NPipeline.Connectors.Azure.CosmosDb.Api.Mongo;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.CosmosDb.Nodes;

/// <summary>
///     First-class sink node for Cosmos Mongo API.
/// </summary>
/// <typeparam name="T">The item type consumed by the sink.</typeparam>
public sealed class CosmosMongoSinkNode<T> : SinkNode<T>
{
    private readonly CosmosConfiguration _configuration;
    private readonly Func<T, string>? _idSelector;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosMongoSinkNode{T}" />.
    /// </summary>
    /// <param name="connectionString">Mongo connection string for Cosmos Mongo API.</param>
    /// <param name="databaseId">Database identifier.</param>
    /// <param name="containerId">Collection identifier.</param>
    /// <param name="writeStrategy">Write strategy.</param>
    /// <param name="idSelector">Optional id selector mapped to Mongo `_id`.</param>
    /// <param name="configuration">Optional base configuration overrides.</param>
    public CosmosMongoSinkNode(
        string connectionString,
        string databaseId,
        string containerId,
        CosmosWriteStrategy writeStrategy = CosmosWriteStrategy.Bulk,
        Func<T, string>? idSelector = null,
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
        _configuration.WriteStrategy = writeStrategy;
        _configuration.Validate();

        _idSelector = idSelector;
    }

    /// <inheritdoc />
    public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var adapter = new CosmosMongoApiAdapter();
        var client = await adapter.CreateClientAsync(_configuration, cancellationToken);

        try
        {
            var sink = adapter.CreateSinkExecutor(client, _configuration, _idSelector);
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
