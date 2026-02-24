using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.Configuration;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Configuration;

/// <summary>
///     Configuration settings for Cosmos DB operations.
/// </summary>
public class CosmosConfiguration
{
    /// <summary>
    ///     Validates the configuration and throws if invalid.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when configuration is invalid.</exception>
    public void Validate()
    {
        var hasSqlConnectionHints =
            !string.IsNullOrWhiteSpace(ConnectionString) ||
            !string.IsNullOrWhiteSpace(AccountEndpoint) ||
            !string.IsNullOrWhiteSpace(AccountKey) ||
            !string.IsNullOrWhiteSpace(NamedConnection);

        if (ApiType == CosmosApiType.Sql && hasSqlConnectionHints && string.IsNullOrEmpty(NamedConnection))
        {
            if (AuthenticationMode == CosmosAuthenticationMode.ConnectionString && string.IsNullOrEmpty(ConnectionString))
                throw new InvalidOperationException("ConnectionString is required when AuthenticationMode is ConnectionString.");

            if (AuthenticationMode == CosmosAuthenticationMode.AccountEndpointAndKey)
            {
                if (string.IsNullOrEmpty(AccountEndpoint))
                    throw new InvalidOperationException("AccountEndpoint is required when AuthenticationMode is AccountEndpointAndKey.");

                if (string.IsNullOrEmpty(AccountKey))
                    throw new InvalidOperationException("AccountKey is required when AuthenticationMode is AccountEndpointAndKey.");
            }

            if (AuthenticationMode == CosmosAuthenticationMode.AzureAdCredential && string.IsNullOrEmpty(AccountEndpoint))
                throw new InvalidOperationException("AccountEndpoint is required when AuthenticationMode is AzureAdCredential.");
        }

        if (ApiType == CosmosApiType.Mongo)
        {
            if (string.IsNullOrWhiteSpace(MongoConnectionString) && string.IsNullOrWhiteSpace(ConnectionString))
                throw new InvalidOperationException("MongoConnectionString or ConnectionString is required when ApiType is Mongo.");
        }

        if (ApiType == CosmosApiType.Cassandra)
        {
            var hasContactPoint = !string.IsNullOrWhiteSpace(CassandraContactPoint) || !string.IsNullOrWhiteSpace(AccountEndpoint);

            if (!hasContactPoint)
                throw new InvalidOperationException("CassandraContactPoint or AccountEndpoint is required when ApiType is Cassandra.");

            if (CassandraPort <= 0)
                throw new InvalidOperationException("CassandraPort must be greater than 0.");
        }

        if (string.IsNullOrEmpty(DatabaseId))
            throw new InvalidOperationException("DatabaseId is required.");

        if (BatchSize <= 0 || BatchSize > MaxBatchSize)
            throw new InvalidOperationException($"BatchSize must be between 1 and {MaxBatchSize}.");

        if (MaxConcurrentOperations <= 0)
            throw new InvalidOperationException("MaxConcurrentOperations must be greater than 0.");
    }

    /// <summary>
    ///     Creates a deep copy of this configuration.
    /// </summary>
    /// <returns>A new <see cref="CosmosConfiguration" /> instance with copied values.</returns>
    public CosmosConfiguration Clone()
    {
        return new CosmosConfiguration
        {
            ConnectionString = ConnectionString,
            ApiType = ApiType,
            AccountEndpoint = AccountEndpoint,
            AccountKey = AccountKey,
            DatabaseId = DatabaseId,
            ContainerId = ContainerId,
            MongoConnectionString = MongoConnectionString,
            CassandraContactPoint = CassandraContactPoint,
            CassandraPort = CassandraPort,
            CassandraUsername = CassandraUsername,
            CassandraPassword = CassandraPassword,
            AuthenticationMode = AuthenticationMode,
            NamedConnection = NamedConnection,
            ConnectionTimeout = ConnectionTimeout,
            RequestTimeout = RequestTimeout,
            ConsistencyLevel = ConsistencyLevel,
            PreferredRegions = [.. PreferredRegions],
            UseGatewayMode = UseGatewayMode,
            WriteStrategy = WriteStrategy,
            BatchSize = BatchSize,
            MaxBatchSize = MaxBatchSize,
            UseTransactionalBatch = UseTransactionalBatch,
            UseIfMatchEtag = UseIfMatchEtag,
            EnableContentResponseOnWrite = EnableContentResponseOnWrite,
            Throughput = Throughput,
            AutoCreateContainer = AutoCreateContainer,
            AllowBulkExecution = AllowBulkExecution,
            MaxConcurrentOperations = MaxConcurrentOperations,
            MaxRetryAttempts = MaxRetryAttempts,
            MaxRetryWaitTime = MaxRetryWaitTime,
            StreamResults = StreamResults,
            MaxItemCount = MaxItemCount,
            EnableCrossPartitionQuery = EnableCrossPartitionQuery,
            ContinuationToken = ContinuationToken,
            ContinueOnError = ContinueOnError,
            PartitionKeyPath = PartitionKeyPath,
            PartitionKeyHandling = PartitionKeyHandling,
            DeliverySemantic = DeliverySemantic,
            CheckpointStrategy = CheckpointStrategy,
        };
    }

    #region Connection Settings

    /// <summary>
    ///     Gets or sets the Cosmos API type.
    ///     Default is <see cref="CosmosApiType.Sql" />.
    /// </summary>
    public CosmosApiType ApiType { get; set; } = CosmosApiType.Sql;

    /// <summary>
    ///     Gets or sets the Cosmos DB connection string.
    ///     Format: AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=xxx;
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the Cosmos DB account endpoint URL.
    ///     Used when <see cref="AuthenticationMode" /> is <see cref="CosmosAuthenticationMode.AccountEndpointAndKey" />
    ///     or <see cref="CosmosAuthenticationMode.AzureAdCredential" />.
    /// </summary>
    public string AccountEndpoint { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the Cosmos DB account key.
    ///     Used when <see cref="AuthenticationMode" /> is <see cref="CosmosAuthenticationMode.AccountEndpointAndKey" />.
    /// </summary>
    public string AccountKey { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the database identifier.
    /// </summary>
    public string DatabaseId { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the logical container/collection/table identifier for source and sink operations.
    /// </summary>
    public string? ContainerId { get; set; }

    /// <summary>
    ///     Gets or sets the Mongo API connection string.
    ///     If null, <see cref="ConnectionString" /> is used.
    /// </summary>
    public string? MongoConnectionString { get; set; }

    /// <summary>
    ///     Gets or sets the Cassandra contact point host.
    /// </summary>
    public string? CassandraContactPoint { get; set; }

    /// <summary>
    ///     Gets or sets the Cassandra port.
    /// </summary>
    public int CassandraPort { get; set; } = 10350;

    /// <summary>
    ///     Gets or sets the Cassandra username.
    /// </summary>
    public string? CassandraUsername { get; set; }

    /// <summary>
    ///     Gets or sets the Cassandra password.
    /// </summary>
    public string? CassandraPassword { get; set; }

    /// <summary>
    ///     Gets or sets the authentication mode for Cosmos DB connections.
    ///     Default is <see cref="CosmosAuthenticationMode.ConnectionString" />.
    /// </summary>
    public CosmosAuthenticationMode AuthenticationMode { get; set; } = CosmosAuthenticationMode.ConnectionString;

    /// <summary>
    ///     Gets or sets the named connection to use from the connection pool.
    ///     If specified, connection string/endpoint/key are ignored.
    /// </summary>
    public string? NamedConnection { get; set; }

    #endregion

    #region Timeout Settings

    /// <summary>
    ///     Gets or sets the connection timeout in seconds.
    ///     Default is 30 seconds.
    /// </summary>
    public int ConnectionTimeout { get; set; } = 30;

    /// <summary>
    ///     Gets or sets the request timeout in seconds.
    ///     Default is 60 seconds.
    /// </summary>
    public int RequestTimeout { get; set; } = 60;

    #endregion

    #region Consistency Settings

    /// <summary>
    ///     Gets or sets the consistency level for operations.
    ///     If null, uses the account default consistency level.
    /// </summary>
    public ConsistencyLevel? ConsistencyLevel { get; set; }

    /// <summary>
    ///     Gets or sets the list of preferred regions for geo-replicated accounts.
    ///     Operations will be routed to these regions in order of preference.
    /// </summary>
    public IList<string> PreferredRegions { get; set; } = [];

    /// <summary>
    ///     Gets or sets whether to use gateway mode instead of direct mode.
    ///     Default is false (direct mode).
    /// </summary>
    public bool UseGatewayMode { get; set; }

    /// <summary>
    ///     Gets or sets a factory function that produces a custom <see cref="System.Net.Http.HttpClient" />
    ///     to be used by the Cosmos SDK for all outbound requests.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         This is primarily useful when connecting to the Azure Cosmos DB Emulator, which
    ///         uses a self-signed TLS certificate.  Supply a factory that creates an
    ///         <see cref="System.Net.Http.HttpClient" /> backed by an
    ///         <see cref="System.Net.Http.HttpClientHandler" /> with
    ///         <c>ServerCertificateCustomValidationCallback</c> set to
    ///         <see cref="System.Net.Http.HttpClientHandler.DangerousAcceptAnyServerCertificateValidator" />.
    ///     </para>
    ///     <para>
    ///         ⚠ Do NOT use certificate bypass in production environments.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Development / emulator only
    /// var config = new CosmosConfiguration
    /// {
    ///     UseGatewayMode = true, // required when using a custom HttpClient
    ///     HttpClientFactory = () => new HttpClient(new HttpClientHandler
    ///     {
    ///         ServerCertificateCustomValidationCallback =
    ///             HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
    ///     })
    /// };
    ///     </code>
    /// </example>
    public Func<HttpClient>? HttpClientFactory { get; set; }

    #endregion

    #region Write Settings

    /// <summary>
    ///     Gets or sets the write strategy for sink operations.
    ///     Default is <see cref="CosmosWriteStrategy.Upsert" />.
    /// </summary>
    public CosmosWriteStrategy WriteStrategy { get; set; } = CosmosWriteStrategy.Upsert;

    /// <summary>
    ///     Gets or sets whether to use upsert instead of insert for write operations.
    ///     When true, existing documents are replaced. When false, duplicate IDs cause conflicts.
    ///     Default is true (use upsert).
    /// </summary>
    public bool UseUpsert { get; set; } = true;

    /// <summary>
    ///     Gets or sets the batch size for batch and bulk operations.
    ///     Default is 100 items.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    ///     Gets or sets the write batch size. Alias for BatchSize.
    /// </summary>
    public int WriteBatchSize
    {
        get => BatchSize;
        set => BatchSize = value;
    }

    /// <summary>
    ///     Gets or sets the maximum batch size for transactional batch operations.
    ///     Cosmos DB limit is 100 operations per transactional batch.
    /// </summary>
    public int MaxBatchSize { get; set; } = 100;

    /// <summary>
    ///     Gets or sets the command timeout in seconds.
    ///     Default is 60 seconds.
    /// </summary>
    public int CommandTimeout
    {
        get => RequestTimeout;
        set => RequestTimeout = value;
    }

    /// <summary>
    ///     Gets or sets whether to use transactional batch for writes.
    ///     If true, all writes within the same partition are batched atomically.
    /// </summary>
    public bool UseTransactionalBatch { get; set; }

    /// <summary>
    ///     Gets or sets whether to enable optimistic concurrency using ETag.
    ///     If true, writes will fail if the item has been modified (412 Precondition Failed).
    /// </summary>
    public bool UseIfMatchEtag { get; set; }

    /// <summary>
    ///     Gets or sets whether to enable content response on write operations.
    ///     Disabling can improve performance by reducing network payload.
    /// </summary>
    public bool EnableContentResponseOnWrite { get; set; }

    #endregion

    #region Throughput Settings

    /// <summary>
    ///     Gets or sets the provisioned throughput (RU/s) for container creation.
    ///     Only used when creating new containers.
    /// </summary>
    public int? Throughput { get; set; }

    /// <summary>
    ///     Gets or sets whether to automatically create the container if it doesn't exist.
    ///     Default is false.
    /// </summary>
    public bool AutoCreateContainer { get; set; }

    /// <summary>
    ///     Gets or sets whether to allow bulk execution.
    ///     Default is true. Required for <see cref="CosmosWriteStrategy.Bulk" />.
    /// </summary>
    public bool AllowBulkExecution { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum number of concurrent operations for bulk execution.
    ///     Default is 32.
    /// </summary>
    public int MaxConcurrentOperations { get; set; } = 32;

    /// <summary>
    ///     Gets or sets the maximum concurrency for parallel operations.
    ///     Alias for MaxConcurrentOperations for compatibility.
    /// </summary>
    public int MaxConcurrency
    {
        get => MaxConcurrentOperations;
        set => MaxConcurrentOperations = value;
    }

    #endregion

    #region Retry Settings

    /// <summary>
    ///     Gets or sets the retry configuration for Azure operations.
    ///     If specified, individual retry properties are ignored in favor of this configuration.
    /// </summary>
    public AzureRetryConfiguration? RetryConfiguration { get; set; }

    /// <summary>
    ///     Gets or sets the maximum number of retry attempts for transient errors.
    ///     Default is 9 retries.
    ///     Note: This property is superseded by <see cref="RetryConfiguration" /> if set.
    /// </summary>
    public int MaxRetryAttempts
    {
        get => RetryConfiguration?.MaxRetryAttempts ?? _maxRetryAttempts;
        set => _maxRetryAttempts = value;
    }

    private int _maxRetryAttempts = 9;

    /// <summary>
    ///     Gets or sets the maximum total time to wait for retries.
    ///     Default is 30 seconds.
    ///     Note: This property is superseded by <see cref="RetryConfiguration" /> if set.
    /// </summary>
    public TimeSpan MaxRetryWaitTime
    {
        get => RetryConfiguration?.MaxRetryWaitTime ?? _maxRetryWaitTime;
        set => _maxRetryWaitTime = value;
    }

    private TimeSpan _maxRetryWaitTime = TimeSpan.FromSeconds(30);

    #endregion

    #region Read Settings

    /// <summary>
    ///     Gets or sets whether to stream results instead of buffering.
    ///     Default is true for memory efficiency.
    /// </summary>
    public bool StreamResults { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum number of items to return per query request.
    ///     Default is 1000 items.
    /// </summary>
    public int MaxItemCount { get; set; } = 1000;

    /// <summary>
    ///     Gets or sets the fetch size for streaming results.
    ///     Alias for MaxItemCount for compatibility with base class patterns.
    ///     Default is 1000 items.
    /// </summary>
    public int FetchSize
    {
        get => MaxItemCount;
        set => MaxItemCount = value;
    }

    /// <summary>
    ///     Gets or sets whether to enable cross-partition queries.
    ///     Default is true.
    /// </summary>
    public bool EnableCrossPartitionQuery { get; set; } = true;

    /// <summary>
    ///     Gets or sets the continuation token for resuming queries.
    /// </summary>
    public string? ContinuationToken { get; set; }

    /// <summary>
    ///     Gets or sets whether to continue processing on individual item errors.
    ///     Default is false.
    /// </summary>
    public bool ContinueOnError { get; set; }

    /// <summary>
    ///     Gets or sets whether to throw on mapping errors.
    ///     Default is true.
    /// </summary>
    public bool ThrowOnMappingError { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to cache mapping metadata and compiled delegates.
    ///     Default is true.
    /// </summary>
    public bool CacheMappingMetadata { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to perform case-insensitive property matching.
    ///     Default is true.
    /// </summary>
    public bool CaseInsensitiveMapping { get; set; } = true;

    #endregion

    #region Partition Key Settings

    /// <summary>
    ///     Gets or sets the partition key path for container creation.
    ///     Example: "/customerId"
    /// </summary>
    public string? PartitionKeyPath { get; set; }

    /// <summary>
    ///     Gets or sets how partition keys are handled for write operations.
    ///     Default is <see cref="PartitionKeyHandling.Auto" />.
    /// </summary>
    public PartitionKeyHandling PartitionKeyHandling { get; set; } = PartitionKeyHandling.Auto;

    #endregion

    #region Delivery Semantics

    /// <summary>
    ///     Gets or sets the delivery semantic for the source node.
    ///     Default is <see cref="Connectors.Configuration.DeliverySemantic.AtLeastOnce" />.
    /// </summary>
    public DeliverySemantic DeliverySemantic { get; set; } = DeliverySemantic.AtLeastOnce;

    /// <summary>
    ///     Gets or sets the checkpoint strategy for the source node.
    ///     Default is <see cref="Connectors.Configuration.CheckpointStrategy.None" />.
    /// </summary>
    public CheckpointStrategy CheckpointStrategy { get; set; } = CheckpointStrategy.None;

    #endregion
}
