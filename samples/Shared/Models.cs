namespace Shared;

/// <summary>
///     Represents basic data structure for pipeline processing.
/// </summary>
/// <param name="Id">The unique identifier for the data record.</param>
/// <param name="Content">The content of the data record.</param>
/// <param name="Timestamp">The timestamp when the data was created.</param>
public record SourceData(string Id, string Content, DateTime Timestamp);

/// <summary>
///     Represents data after processing through the pipeline.
/// </summary>
/// <param name="Id">The unique identifier for the data record.</param>
/// <param name="Content">The content of the data record.</param>
/// <param name="ProcessedBy">The identifier of what processed this data.</param>
/// <param name="ProcessedAt">The timestamp when the data was processed.</param>
/// <param name="OriginalTimestamp">The original timestamp from the source data.</param>
public record ProcessedData(string Id, string Content, string ProcessedBy, DateTime ProcessedAt, DateTime OriginalTimestamp);
