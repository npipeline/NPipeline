using NPipeline.Configuration;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;
using Sample_AggregateNode.Models;

namespace Sample_AggregateNode.Nodes;

/// <summary>
///     AggregateNode that sums analytics event values by category using sliding windows.
///     This node demonstrates overlapping window processing for continuous value aggregation.
/// </summary>
public class EventSumAggregator : AggregateNode<FilteredAnalyticsEvent, string, EventSumMetrics>
{
    /// <summary>
    ///     Initializes a new instance of the EventSumAggregator with sliding windows.
    ///     Uses 30-second windows sliding every 10 seconds for continuous value aggregation.
    ///     Sliding windows provide overlapping views of the data for more granular analytics.
    /// </summary>
    public EventSumAggregator()
        : base(new AggregateNodeConfiguration<FilteredAnalyticsEvent>(
            WindowAssigner.Sliding(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(10))))
    {
        Console.WriteLine("EventSumAggregator: Initialized with 30-second sliding windows every 10 seconds");
        Console.WriteLine("EventSumAggregator: Will sum event values by category in overlapping time windows");
    }

    /// <summary>
    ///     Extracts the category key from a filtered analytics event for grouping.
    ///     Only relevant events are included in the aggregation.
    /// </summary>
    /// <param name="item">The FilteredAnalyticsEvent item.</param>
    /// <returns>The Category for grouping, or empty string for filtered events.</returns>
    public override string GetKey(FilteredAnalyticsEvent item)
    {
        // Only aggregate relevant events
        if (!item.IsRelevant)
            return string.Empty; // Filtered events get empty key

        var category = item.OriginalEvent.Category;
        Console.WriteLine($"EventSumAggregator: Grouping by Category: {category} for event {item.OriginalEvent.EventId}");
        return category;
    }

    /// <summary>
    ///     Creates an initial accumulator value for a new category group.
    ///     This creates a default EventSumMetrics that will be updated as events are accumulated.
    /// </summary>
    /// <returns>The initial accumulator with default values.</returns>
    public override EventSumMetrics CreateAccumulator()
    {
        return new EventSumMetrics(
            string.Empty, // Will be set when first event is processed
            0m,
            0,
            0m,
            DateTime.MinValue, // Will be set when first event is processed
            DateTime.MinValue, // Will be set when first event is processed
            TimeSpan.FromSeconds(30) // Fixed window duration
        );
    }

    /// <summary>
    ///     Accumulates a filtered analytics event into the event sum metrics.
    ///     This method updates the total value, count, and average for each category within the sliding window.
    /// </summary>
    /// <param name="accumulator">The current event sum metrics.</param>
    /// <param name="item">The FilteredAnalyticsEvent to accumulate.</param>
    /// <returns>The updated event sum metrics.</returns>
    public override EventSumMetrics Accumulate(EventSumMetrics accumulator, FilteredAnalyticsEvent item)
    {
        // Skip filtered events
        if (!item.IsRelevant)
            return accumulator;

        var originalEvent = item.OriginalEvent;
        var newTotalValue = accumulator.TotalValue + originalEvent.Value;
        var newEventCount = accumulator.EventCount + 1;

        var newAverageValue = newEventCount > 0
            ? newTotalValue / newEventCount
            : 0m;

        // For the first event in the group, set the category and window timing
        if (accumulator.EventCount == 0)
        {
            // For sliding windows, we need to calculate the window boundaries
            // The window starts 30 seconds before the event time and lasts 30 seconds
            var eventTime = originalEvent.Timestamp;

            var windowEnd = new DateTime(eventTime.Year, eventTime.Month, eventTime.Day,
                eventTime.Hour, eventTime.Minute, eventTime.Second, DateTimeKind.Utc);

            var windowStart = windowEnd.AddSeconds(-30);

            Console.WriteLine(
                $"EventSumAggregator: Starting new sliding window for {originalEvent.Category} " +
                $"({windowStart:HH:mm:ss} - {windowEnd:HH:mm:ss})");

            return new EventSumMetrics(
                originalEvent.Category,
                newTotalValue,
                newEventCount,
                newAverageValue,
                windowStart,
                windowEnd,
                TimeSpan.FromSeconds(30)
            );
        }

        // For subsequent events, update the aggregation values
        Console.WriteLine(
            $"EventSumAggregator: Accumulating {originalEvent.Category} " +
            $"value: {originalEvent.Value:C}, total: {accumulator.TotalValue:C} -> {newTotalValue:C}, " +
            $"avg: {newAverageValue:C}");

        return new EventSumMetrics(
            accumulator.Category,
            newTotalValue,
            newEventCount,
            newAverageValue,
            accumulator.WindowStart,
            accumulator.WindowEnd,
            accumulator.WindowDuration
        );
    }

    /// <summary>
    ///     Gets metrics about the aggregator's operation.
    /// </summary>
    /// <returns>A tuple containing metrics about windows processed, closed, and maximum concurrency.</returns>
    public new (long TotalWindowsProcessed, long TotalWindowsClosed, long MaxConcurrentWindows) GetMetrics()
    {
        var metrics = base.GetMetrics();

        Console.WriteLine($"EventSumAggregator Metrics: {metrics.TotalWindowsProcessed} windows processed, " +
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
        Console.WriteLine($"EventSumAggregator: Currently tracking {count} active sliding windows");
        return count;
    }

    /// <summary>
    ///     Calculates the sliding window overlap factor for monitoring purposes.
    ///     This helps understand the efficiency of the sliding window configuration.
    /// </summary>
    /// <returns>The overlap factor (window size / slide interval).</returns>
    public double GetOverlapFactor()
    {
        // For our configuration: 30-second window / 10-second slide = 3x overlap
        const double overlapFactor = 30.0 / 10.0;
        Console.WriteLine($"EventSumAggregator: Sliding window overlap factor: {overlapFactor}x");
        return overlapFactor;
    }
}
