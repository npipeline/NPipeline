namespace Sample_BranchNode.Models;

/// <summary>
///     Represents a notification event generated from order processing for customer communication
/// </summary>
public class NotificationEvent
{
    /// <summary>
    ///     Unique identifier for the order that triggered this notification
    /// </summary>
    public required string OrderId { get; init; }

    /// <summary>
    ///     Unique identifier for the customer to receive the notification
    /// </summary>
    public required string CustomerId { get; init; }

    /// <summary>
    ///     The notification message content
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    ///     Type of notification (Email, SMS, Push, InApp)
    /// </summary>
    public required string NotificationType { get; init; }

    /// <summary>
    ///     Subject line for email notifications
    /// </summary>
    public string? Subject { get; init; }

    /// <summary>
    ///     Recipient address (email, phone number, device token, etc.)
    /// </summary>
    public string? RecipientAddress { get; init; }

    /// <summary>
    ///     Priority level for sending the notification
    /// </summary>
    public int Priority { get; init; } = 1;

    /// <summary>
    ///     Timestamp when the notification event was generated
    /// </summary>
    public DateTime Timestamp { get; init; }

    /// <summary>
    ///     Scheduled time for sending the notification (null for immediate)
    /// </summary>
    public DateTime? ScheduledTime { get; init; }

    /// <summary>
    ///     Template used for generating the notification
    /// </summary>
    public string? TemplateName { get; init; }

    /// <summary>
    ///     Additional data for template rendering
    /// </summary>
    public Dictionary<string, object> TemplateData { get; init; } = new();

    /// <summary>
    ///     Whether this notification requires confirmation of receipt
    /// </summary>
    public bool RequiresConfirmation { get; init; }
}
