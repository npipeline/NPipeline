namespace Sample_HttpPost.Models;

/// <summary>
///     Represents incoming webhook data from HTTP POST requests.
///     This record contains the raw data received from external systems.
/// </summary>
/// <param name="Id">The unique identifier for the webhook event.</param>
/// <param name="EventType">The type of event being sent (e.g., "user.created", "payment.completed").</param>
/// <param name="Payload">The event payload containing additional data as key-value pairs.</param>
/// <param name="Timestamp">The timestamp when the webhook was received.</param>
public record WebhookData(
    string Id,
    string EventType,
    Dictionary<string, object> Payload,
    DateTime Timestamp
);
