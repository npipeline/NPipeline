namespace Sample_06_AdvancedErrorHandling;

/// <summary>
///     Represents a work item to be processed in the pipeline.
/// </summary>
/// <param name="Id">The unique identifier for the work item.</param>
/// <param name="Description">A description of the work item.</param>
/// <param name="ProcessingTimeMs">The processing time in milliseconds.</param>
public record WorkItem(
    int Id,
    string Description,
    int ProcessingTimeMs
);

/// <summary>
///     Represents a failed work item with error information.
/// </summary>
/// <param name="WorkItem">The original work item that failed.</param>
/// <param name="Exception">The exception that caused the failure.</param>
/// <param name="Timestamp">The timestamp when the failure occurred.</param>
/// <param name="RetryCount">The number of retry attempts made.</param>
public record FailedWorkItem(
    WorkItem WorkItem,
    Exception Exception,
    DateTime Timestamp,
    int RetryCount
);

/// <summary>
///     Represents source data for processing in the pipeline.
/// </summary>
/// <param name="Id">The unique identifier for the source data.</param>
/// <param name="Content">The content of the source data.</param>
/// <param name="Timestamp">The timestamp when the data was created.</param>
public record SourceData(
    string Id,
    string Content,
    DateTime Timestamp
);

/// <summary>
///     Utility class for generating test data for the pipeline.
/// </summary>
public static class TestDataGenerator
{
    /// <summary>
    ///     Creates a new SourceData instance for testing purposes.
    /// </summary>
    /// <param name="id">The ID for the source data.</param>
    /// <param name="content">The content for the source data.</param>
    /// <param name="timestamp">The timestamp for the source data.</param>
    /// <returns>A new SourceData instance.</returns>
    public static SourceData CreateSourceData(string id, string content, DateTime timestamp)
    {
        return new SourceData(id, content, timestamp);
    }

    /// <summary>
    ///     Creates a new WorkItem instance for testing purposes.
    /// </summary>
    /// <param name="id">The ID for the work item.</param>
    /// <param name="description">The description for the work item.</param>
    /// <param name="processingTimeMs">The processing time in milliseconds.</param>
    /// <returns>A new WorkItem instance.</returns>
    public static WorkItem CreateWorkItem(int id, string description, int processingTimeMs)
    {
        return new WorkItem(id, description, processingTimeMs);
    }
}
