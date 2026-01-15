namespace Sample_HttpPost.Models;

/// <summary>
///     Represents webhook data after processing by the pipeline.
///     This record contains the final processed data ready for output.
/// </summary>
/// <param name="Id">The unique identifier for the webhook event.</param>
/// <param name="EventType">The type of event that was processed.</param>
/// <param name="Summary">A human-readable summary of the processed payload.</param>
/// <param name="ProcessedAt">The timestamp when the data was processed.</param>
public record ProcessedData(
    string Id,
    string EventType,
    string Summary,
    DateTime ProcessedAt
);
