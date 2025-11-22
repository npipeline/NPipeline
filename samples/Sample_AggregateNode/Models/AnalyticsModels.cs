using NPipeline.DataFlow;

namespace Sample_AggregateNode.Models;

/// <summary>
///     Represents an analytics event from a real-time data stream
/// </summary>
public record AnalyticsEvent(
    string EventId,
    string EventType,
    string Category,
    decimal Value,
    DateTimeOffset Timestamp,
    string UserId,
    Dictionary<string, object> Properties
) : ITimestamped
{
    /// <summary>
    ///     Gets the timestamp for event-time processing
    /// </summary>
    public DateTimeOffset EventTimestamp => Timestamp;
}

/// <summary>
///     Represents aggregated event counts by type within a time window
/// </summary>
public record EventCountMetrics(
    string EventType,
    int Count,
    DateTime WindowStart,
    DateTime WindowEnd,
    TimeSpan WindowDuration
)
{
    /// <summary>
    ///     Gets the events per second rate for this window
    /// </summary>
    public double EventsPerSecond => WindowDuration.TotalSeconds > 0
        ? Count / WindowDuration.TotalSeconds
        : 0;
}

/// <summary>
///     Represents aggregated value sums by category within a time window
/// </summary>
public record EventSumMetrics(
    string Category,
    decimal TotalValue,
    int EventCount,
    decimal AverageValue,
    DateTime WindowStart,
    DateTime WindowEnd,
    TimeSpan WindowDuration
)
{
    /// <summary>
    ///     Gets the value per second rate for this window
    /// </summary>
    public double ValuePerSecond => WindowDuration.TotalSeconds > 0
        ? (double)TotalValue / WindowDuration.TotalSeconds
        : 0;
}

/// <summary>
///     Represents a filtered analytics event after processing
/// </summary>
public record FilteredAnalyticsEvent(
    AnalyticsEvent OriginalEvent,
    bool IsRelevant,
    string FilterReason
);

/// <summary>
///     Represents dashboard metrics for display
/// </summary>
public record DashboardMetrics(
    DateTime GeneratedAt,
    List<EventCountMetrics> CountMetrics,
    List<EventSumMetrics> SumMetrics,
    int TotalEventsProcessed,
    int EventsFilteredOut,
    TimeSpan ProcessingDuration
);

/// <summary>
///     Constants for analytics event types and categories
/// </summary>
public static class AnalyticsConstants
{
    // Event types
    public const string PageView = "page_view";
    public const string Click = "click";
    public const string Purchase = "purchase";
    public const string AddToCart = "add_to_cart";
    public const string Search = "search";
    public const string Login = "login";
    public const string Logout = "logout";

    // Categories
    public const string UserEngagement = "user_engagement";
    public const string ECommerce = "e_commerce";
    public const string SearchActivity = "search_activity";
    public const string Authentication = "authentication";
    public const string Navigation = "navigation";

    // Filter reasons
    public const string RelevantEvent = "relevant_event";
    public const string IrrelevantEventType = "irrelevant_event_type";
    public const string LowValueEvent = "low_value_event";
    public const string TestEvent = "test_event";
}
