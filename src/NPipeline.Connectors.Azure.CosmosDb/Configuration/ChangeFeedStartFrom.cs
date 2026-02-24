namespace NPipeline.Connectors.Azure.CosmosDb.Configuration;

/// <summary>
///     Defines where to start reading from the Cosmos DB Change Feed.
/// </summary>
public enum ChangeFeedStartFrom
{
    /// <summary>
    ///     Start from the beginning of time, processing all historical changes.
    ///     Use for initial data synchronization or full replays.
    /// </summary>
    Beginning = 0,

    /// <summary>
    ///     Start from the current time, processing only new changes.
    ///     Use for real-time streaming without historical data.
    /// </summary>
    Now = 1,

    /// <summary>
    ///     Start from a specific point in time.
    ///     Requires <see cref="ChangeFeedConfiguration.StartTime" /> to be set.
    /// </summary>
    PointInTime = 2,

    /// <summary>
    ///     Start from a specific time. Alias for PointInTime.
    /// </summary>
    Time = PointInTime,

    /// <summary>
    ///     Start from a saved continuation token.
    ///     Requires <see cref="ChangeFeedConfiguration.CheckpointStore" /> to be configured.
    /// </summary>
    ContinuationToken = 3,
}
