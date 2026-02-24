using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.ChangeFeed;

namespace NPipeline.Connectors.Azure.CosmosDb.Configuration;

/// <summary>
///     Configuration settings for Cosmos DB Change Feed operations.
/// </summary>
public class ChangeFeedConfiguration
{
    /// <summary>
    ///     Gets or sets where to start reading from the change feed.
    ///     Default is <see cref="ChangeFeedStartFrom.Beginning" />.
    /// </summary>
    public ChangeFeedStartFrom StartFrom { get; set; } = ChangeFeedStartFrom.Beginning;

    /// <summary>
    ///     Gets or sets the start time for change feed when <see cref="StartFrom" /> is
    ///     <see cref="ChangeFeedStartFrom.PointInTime" />.
    /// </summary>
    public DateTime? StartTime { get; set; }

    /// <summary>
    ///     Gets or sets the time to wait between polling for changes when no new changes are available.
    ///     Default is 1 second.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets the maximum number of items to return per change feed request.
    ///     Default is 100 items.
    /// </summary>
    public int MaxItemCount { get; set; } = 100;

    /// <summary>
    ///     Gets or sets whether to include full document content in change feed results.
    ///     Default is true. Set to false for metadata-only processing.
    /// </summary>
    public bool IncludeFullDocuments { get; set; } = true;

    /// <summary>
    ///     Gets or sets an optional partition key to scope changes to a single partition.
    ///     If null, changes from all partitions are returned.
    /// </summary>
    public PartitionKey? PartitionKey { get; set; }

    /// <summary>
    ///     Gets or sets the custom checkpoint store for persisting continuation tokens.
    ///     If null, an in-memory checkpoint store is used (not persistent across restarts).
    /// </summary>
    public IChangeFeedCheckpointStore? CheckpointStore { get; set; }

    /// <summary>
    ///     Gets or sets whether to handle 429 rate limiting automatically.
    ///     Default is true.
    /// </summary>
    public bool HandleRateLimiting { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum time to wait when rate limited before throwing.
    ///     Default is 30 seconds.
    /// </summary>
    public TimeSpan MaxRateLimitWaitTime { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Gets or sets whether to continue processing on individual item errors.
    ///     Default is false.
    /// </summary>
    public bool ContinueOnError { get; set; }

    /// <summary>
    ///     Validates the configuration and throws if invalid.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when configuration is invalid.</exception>
    public void Validate()
    {
        if (StartFrom == ChangeFeedStartFrom.PointInTime && !StartTime.HasValue)
            throw new InvalidOperationException("StartTime is required when StartFrom is PointInTime.");

        if (MaxItemCount <= 0)
            throw new InvalidOperationException("MaxItemCount must be greater than 0.");

        if (PollingInterval < TimeSpan.Zero)
            throw new InvalidOperationException("PollingInterval must be non-negative.");

        if (MaxRateLimitWaitTime < TimeSpan.Zero)
            throw new InvalidOperationException("MaxRateLimitWaitTime must be non-negative.");
    }

    /// <summary>
    ///     Creates a deep copy of this configuration.
    /// </summary>
    /// <returns>A new <see cref="ChangeFeedConfiguration" /> instance with copied values.</returns>
    public ChangeFeedConfiguration Clone()
    {
        return new ChangeFeedConfiguration
        {
            StartFrom = StartFrom,
            StartTime = StartTime,
            PollingInterval = PollingInterval,
            MaxItemCount = MaxItemCount,
            IncludeFullDocuments = IncludeFullDocuments,
            PartitionKey = PartitionKey,
            CheckpointStore = CheckpointStore, // Not deep copied - same instance
            HandleRateLimiting = HandleRateLimiting,
            MaxRateLimitWaitTime = MaxRateLimitWaitTime,
            ContinueOnError = ContinueOnError,
        };
    }
}
