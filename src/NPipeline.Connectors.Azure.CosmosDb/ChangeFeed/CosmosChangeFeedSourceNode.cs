using System.Net;
using System.Runtime.CompilerServices;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using ChangeFeedStartFrom = Microsoft.Azure.Cosmos.ChangeFeedStartFrom;

namespace NPipeline.Connectors.Azure.CosmosDb.ChangeFeed;

/// <summary>
///     Cosmos DB Change Feed source node for real-time streaming data ingestion.
///     Continuously reads changes from a Cosmos DB container using the Change Feed API.
/// </summary>
/// <typeparam name="T">The type of objects emitted by source.</typeparam>
public class CosmosChangeFeedSourceNode<T> : SourceNode<T>, IAsyncDisposable
{
    private readonly IChangeFeedCheckpointStore _checkpointStore;
    private readonly CosmosClient _client;
    private readonly ChangeFeedConfiguration _configuration;
    private readonly string _containerId;
    private readonly bool _continueOnError;
    private readonly Func<CosmosRow, T> _conventionMapper;
    private readonly string _databaseId;
    private readonly Func<CosmosRow, T>? _mapper;
    private readonly bool _ownsClient;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosChangeFeedSourceNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The Cosmos DB connection string.</param>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="configuration">The change feed configuration.</param>
    /// <param name="checkpointStore">The checkpoint store for persisting continuation tokens.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    public CosmosChangeFeedSourceNode(
        string connectionString,
        string databaseId,
        string containerId,
        ChangeFeedConfiguration? configuration = null,
        IChangeFeedCheckpointStore? checkpointStore = null,
        Func<CosmosRow, T>? mapper = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(databaseId))
            throw new ArgumentNullException(nameof(databaseId));

        if (string.IsNullOrWhiteSpace(containerId))
            throw new ArgumentNullException(nameof(containerId));

        _client = new CosmosClient(connectionString);
        _ownsClient = true;
        _databaseId = databaseId;
        _containerId = containerId;
        _configuration = configuration ?? new ChangeFeedConfiguration();
        _configuration.Validate();
        _checkpointStore = checkpointStore ?? _configuration.CheckpointStore ?? new InMemoryChangeFeedCheckpointStore();
        _mapper = mapper;
        _conventionMapper = CosmosMapperBuilder.Build<T>();
        _continueOnError = _configuration.ContinueOnError;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosChangeFeedSourceNode{T}" /> class with a CosmosClient.
    /// </summary>
    /// <param name="client">The Cosmos DB client.</param>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="configuration">The change feed configuration.</param>
    /// <param name="checkpointStore">The checkpoint store for persisting continuation tokens.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    public CosmosChangeFeedSourceNode(
        CosmosClient client,
        string databaseId,
        string containerId,
        ChangeFeedConfiguration? configuration = null,
        IChangeFeedCheckpointStore? checkpointStore = null,
        Func<CosmosRow, T>? mapper = null)
    {
        ArgumentNullException.ThrowIfNull(client);

        if (string.IsNullOrWhiteSpace(databaseId))
            throw new ArgumentNullException(nameof(databaseId));

        if (string.IsNullOrWhiteSpace(containerId))
            throw new ArgumentNullException(nameof(containerId));

        _client = client;
        _ownsClient = false;
        _databaseId = databaseId;
        _containerId = containerId;
        _configuration = configuration ?? new ChangeFeedConfiguration();
        _configuration.Validate();
        _checkpointStore = checkpointStore ?? _configuration.CheckpointStore ?? new InMemoryChangeFeedCheckpointStore();
        _mapper = mapper;
        _conventionMapper = CosmosMapperBuilder.Build<T>();
        _continueOnError = _configuration.ContinueOnError;
    }

    /// <summary>
    ///     Disposes the change feed source node and releases associated resources.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            await base.DisposeAsync();
            return;
        }

        if (_ownsClient)
            _client.Dispose();

        _disposed = true;
        GC.SuppressFinalize(this);
        await base.DisposeAsync();
    }

    /// <summary>
    ///     Initializes the source node and returns a streaming data pipe.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A streaming data pipe containing the change feed data.</returns>
    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var stream = StreamChangeFeedAsync(cancellationToken);
        return new DataStream<T>(stream, $"{GetType().Name}");
    }

    /// <summary>
    ///     Streams changes from the Cosmos DB Change Feed.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An async enumerable of change feed items.</returns>
    private async IAsyncEnumerable<T> StreamChangeFeedAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var container = _client.GetContainer(_databaseId, _containerId);

        // Get the continuation token from checkpoint store
        var continuationToken = await _checkpointStore.GetTokenAsync(_databaseId, _containerId, cancellationToken);

        // Build the Change Feed iterator
        var changeFeedStartFrom = BuildChangeFeedStartFrom(continuationToken);
        var changeFeedMode = BuildChangeFeedMode();

        var iterator = container.GetChangeFeedStreamIterator(
            changeFeedStartFrom,
            changeFeedMode,
            new ChangeFeedRequestOptions
            {
                PageSizeHint = _configuration.MaxItemCount,
            });

        while (!cancellationToken.IsCancellationRequested)
        {
            List<CosmosRow> items;
            string? newContinuationToken = null;
            var shouldWait = false;

            try
            {
                var response = await iterator.ReadNextAsync(cancellationToken);

                if (response.StatusCode == HttpStatusCode.NotModified)
                {
                    // No new changes, wait before polling again
                    shouldWait = true;
                    items = [];
                }
                else
                {
                    // Process the changes
                    using var stream = response.Content;
                    items = await ParseChangeFeedResponse(stream, cancellationToken);
                    newContinuationToken = response.ContinuationToken;
                }
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotModified)
            {
                // No new changes, wait before polling again
                shouldWait = true;
                items = [];
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
            catch (Exception) when (_continueOnError)
            {
                shouldWait = true;
                items = [];
            }

            if (shouldWait)
            {
                await Task.Delay(_configuration.PollingInterval, cancellationToken);
                continue;
            }

            // Yield items outside the try-catch
            foreach (var item in items)
            {
                if (cancellationToken.IsCancellationRequested)
                    yield break;

                if (_mapper != null)
                {
                    T mapped;
                    var mapSuccess = true;

                    try
                    {
                        mapped = _mapper(item);
                    }
                    catch
                    {
                        if (!_continueOnError)
                            throw;

                        mapSuccess = false;
                        mapped = default!;
                    }

                    if (mapSuccess && mapped != null)
                        yield return mapped;
                }
                else
                {
                    var mapped = MapConventionBased(item);

                    if (mapped != null)
                        yield return mapped;
                }
            }

            // Save the continuation token
            if (newContinuationToken != null)
            {
                await _checkpointStore.SaveTokenAsync(
                    _databaseId,
                    _containerId,
                    newContinuationToken,
                    cancellationToken);
            }
        }
    }

    private ChangeFeedStartFrom BuildChangeFeedStartFrom(string? continuationToken)
    {
        if (!string.IsNullOrWhiteSpace(continuationToken))
            return ChangeFeedStartFrom.ContinuationToken(continuationToken);

        return _configuration.StartFrom switch
        {
            Configuration.ChangeFeedStartFrom.Beginning => ChangeFeedStartFrom.Beginning(),
            Configuration.ChangeFeedStartFrom.PointInTime => ChangeFeedStartFrom.Time(_configuration.StartTime ?? DateTime.UtcNow),
            Configuration.ChangeFeedStartFrom.Now => ChangeFeedStartFrom.Now(),
            Configuration.ChangeFeedStartFrom.ContinuationToken => ChangeFeedStartFrom.Beginning(),
            _ => ChangeFeedStartFrom.Beginning(),
        };
    }

    private static ChangeFeedMode BuildChangeFeedMode()
    {
        // Use latest version mode - gets the most recent version of each item
        return ChangeFeedMode.LatestVersion;
    }

    private static async Task<List<CosmosRow>> ParseChangeFeedResponse(Stream stream, CancellationToken cancellationToken)
    {
        var items = new List<CosmosRow>();

        using var reader = new StreamReader(stream);
        var content = await reader.ReadToEndAsync(cancellationToken);

        // Parse the JSON response
        // The response format is: { "Documents": [...], "_rid": "...", "_count": N }
        try
        {
            JArray? documents;
            var token = JToken.Parse(content);

            if (token is JArray rawArray)
                documents = rawArray;
            else
            {
                var json = token as JObject;
                documents = json?["Documents"] as JArray;
            }

            if (documents != null)
            {
                foreach (var doc in documents)
                {
                    var rowData = new Dictionary<string, object?>();

                    foreach (var prop in doc.Children<JProperty>())
                    {
                        rowData[prop.Name] = prop.Value.ToObject<object>();
                    }

                    items.Add(new CosmosRow(rowData));
                }
            }
        }
        catch (JsonException)
        {
            // Return empty list if parsing fails
        }

        return items;
    }

    private T? MapConventionBased(CosmosRow row)
    {
        try
        {
            var type = typeof(T);

            if (type == typeof(CosmosRow))
                return (T)(object)row;

            if (type == typeof(Dictionary<string, object?>))
                return (T)(object)row.ToDictionary();

            // Use the mapper builder for complex types
            return _conventionMapper(row);
        }
        catch
        {
            if (!_continueOnError)
                throw;

            return default;
        }
    }
}
