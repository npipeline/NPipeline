namespace Sample_HttpPost.Models;

/// <summary>
///     Represents webhook data after validation by the ValidationTransform node.
///     This record contains validated data that has passed all validation checks.
/// </summary>
/// <param name="Id">The unique identifier for the webhook event.</param>
/// <param name="EventType">The type of event that was validated.</param>
/// <param name="Payload">The validated event payload containing additional data as key-value pairs.</param>
/// <param name="Timestamp">The timestamp when the webhook was received.</param>
/// <param name="ValidatedAt">The timestamp when validation was completed.</param>
public record ValidatedWebhookData(
    string Id,
    string EventType,
    Dictionary<string, object> Payload,
    DateTime Timestamp,
    DateTime ValidatedAt
);
