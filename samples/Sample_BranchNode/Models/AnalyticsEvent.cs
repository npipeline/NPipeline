namespace Sample_BranchNode.Models;

/// <summary>
///     Represents an analytics event generated from order processing for business intelligence
/// </summary>
public class AnalyticsEvent
{
    /// <summary>
    ///     Unique identifier for the order that generated this analytics event
    /// </summary>
    public required string OrderId { get; init; }

    /// <summary>
    ///     Unique identifier for the customer
    /// </summary>
    public required string CustomerId { get; init; }

    /// <summary>
    ///     Unique identifier for the product
    /// </summary>
    public required string ProductId { get; init; }

    /// <summary>
    ///     Price of the product at the time of purchase
    /// </summary>
    public decimal Price { get; init; }

    /// <summary>
    ///     Type of analytics event (Purchase, View, CartAdd, etc.)
    /// </summary>
    public required string EventType { get; init; }

    /// <summary>
    ///     Category of the product for analytics grouping
    /// </summary>
    public string? ProductCategory { get; init; }

    /// <summary>
    ///     Customer segment for analytics
    /// </summary>
    public string? CustomerSegment { get; init; }

    /// <summary>
    ///     Geographic region of the customer
    /// </summary>
    public string? Region { get; init; }

    /// <summary>
    ///     Timestamp when the analytics event was generated
    /// </summary>
    public DateTime Timestamp { get; init; }

    /// <summary>
    ///     Additional metadata for analytics processing
    /// </summary>
    public Dictionary<string, object> Metadata { get; init; } = new();

    /// <summary>
    ///     Session identifier if available
    /// </summary>
    public string? SessionId { get; init; }
}
