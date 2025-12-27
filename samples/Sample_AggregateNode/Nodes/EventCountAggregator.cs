using NPipeline.Configuration;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;
using Sample_AggregateNode.Models;

namespace Sample_AggregateNode.Nodes;

/// <summary>
///     AggregateNode that counts analytics events by type using tumbling windows.
///     This node demonstrates time-based aggregation with non-overlapping windows for event counting.
/// </summary>
public class EventCountAggregator : AggregateNode<FilteredAnalyticsEvent, string, EventCountMetrics>
{
    /// <summary>
    ///     Initializes a new instance of the EventCountAggregator with 1-minute tumbling windows.
    ///     Tumbling windows are non-overlapping and provide discrete time period aggregations.
    /// </summary>
    public EventCountAggregator()
        : base(new AggregateNodeConfiguration<FilteredAnalyticsEvent>(
            WindowAssigner.Tumbling(TimeSpan.FromMinutes(1))))
    {
        Console.WriteLine("EventCountAggregator: Initialized with 1-minute tumbling windows");
        Console.WriteLine("EventCountAggregator: Will count events by type in non-overlapping time windows");
    }

    /// <summary>
    ///     Extracts the event type key from a filtered analytics event for grouping.
    ///     Only relevant events are included in the aggregation.
    /// </summary>
    /// <param name="item">The FilteredAnalyticsEvent item.</param>
    /// <returns>The EventType for grouping, or empty string for filtered events.</returns>
    public override string GetKey(FilteredAnalyticsEvent item)
    {
        // Only aggregate relevant events
        if (!item.IsRelevant)
            return string.Empty; // Filtered events get empty key

        var eventType = item.OriginalEvent.EventType;
        Console.WriteLine($"EventCountAggregator: Grouping by EventType: {eventType} for event {item.OriginalEvent.EventId}");
        return eventType;
    }

    /// <summary>
    ///     Creates an initial accumulator value for a new event type group.
    ///     This creates a default EventCountMetrics that will be updated as events are accumulated.
    /// </summary>
    /// <returns>The initial accumulator with default values.</returns>
    public override EventCountMetrics CreateAccumulator()
    {
        return new EventCountMetrics(
            string.Empty, // Will be set when first event is processed
            0,
            DateTime.MinValue, // Will be set when first event is processed
            DateTime.MinValue, // Will be set when first event is processed
            TimeSpan.FromMinutes(1) // Fixed window duration
        );
    }

    /// <summary>
    ///     Accumulates a filtered analytics event into the event count metrics.
    ///     This method updates the count and timing information for each event type within the window.
    /// </summary>
    /// <param name="accumulator">The current event count metrics.</param>
    /// <param name="item">The FilteredAnalyticsEvent to accumulate.</param>
    /// <returns>The updated event count metrics.</returns>
    public override EventCountMetrics Accumulate(EventCountMetrics accumulator, FilteredAnalyticsEvent item)
    {
        // Skip filtered events
        if (!item.IsRelevant)
            return accumulator;

        var originalEvent = item.OriginalEvent;
        var newCount = accumulator.Count + 1;

        // For the first event in the group, set the event type and window timing
        if (accumulator.Count == 0)
        {
            // Calculate window boundaries based on event timestamp
            var eventTime = originalEvent.Timestamp;

            var windowStart = new DateTime(eventTime.Year, eventTime.Month, eventTime.Day,
                eventTime.Hour, eventTime.Minute, 0, DateTimeKind.Utc);

            var windowEnd = windowStart.AddMinutes(1);

            Console.WriteLine(
                $"EventCountAggregator: Starting new window for {originalEvent.EventType} " +
                $"({windowStart:HH:mm:ss} - {windowEnd:HH:mm:ss})");

            return new EventCountMetrics(
                originalEvent.EventType,
                newCount,
                windowStart,
                windowEnd,
                TimeSpan.FromMinutes(1)
            );
        }

        // For subsequent events, update only the count
        Console.WriteLine(
            $"EventCountAggregator: Accumulating {originalEvent.EventType} count: {accumulator.Count} -> {newCount}");

        return accumulator with { Count = newCount };
    }

    /// <summary>
    ///     Gets metrics about the aggregator's operation.
    /// </summary>
    /// <returns>A tuple containing metrics about windows processed, closed, and maximum concurrency.</returns>
    public new (long TotalWindowsProcessed, long TotalWindowsClosed, long MaxConcurrentWindows) GetMetrics()
    {
        var metrics = base.GetMetrics();

        Console.WriteLine($"EventCountAggregator Metrics: {metrics.TotalWindowsProcessed} windows processed, " +
                          $"{metrics.TotalWindowsClosed} windows closed, {metrics.MaxConcurrentWindows} max concurrent");

        return metrics;
    }

    /// <summary>
    ///     Gets the current number of active windows being tracked.
    /// </summary>
    /// <returns>The current number of active windows.</returns>
    public new int GetActiveWindowCount()
    {
        var count = base.GetActiveWindowCount();
        Console.WriteLine($"EventCountAggregator: Currently tracking {count} active windows");
        return count;
    }
}
