using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_AggregateNode.Models;

namespace Sample_AggregateNode.Nodes;

/// <summary>
///     Transform node that filters analytics events based on relevance criteria.
///     This node demonstrates data filtering patterns in streaming analytics pipelines.
/// </summary>
public class EventFilterTransform : TransformNode<AnalyticsEvent, FilteredAnalyticsEvent>
{
    private readonly decimal _minimumValueThreshold;
    private readonly HashSet<string> _relevantEventTypes;
    private readonly HashSet<string> _testUserIds;
    private int _eventsFilteredOut;
    private int _totalEventsProcessed;

    /// <summary>
    ///     Initializes a new instance of the EventFilterTransform.
    /// </summary>
    public EventFilterTransform()
    {
        // Define which event types are considered relevant for analytics
        _relevantEventTypes = new HashSet<string>
        {
            AnalyticsConstants.PageView,
            AnalyticsConstants.Click,
            AnalyticsConstants.Purchase,
            AnalyticsConstants.AddToCart,
            AnalyticsConstants.Search,
        };

        // Minimum value threshold for events (filter out very low-value events)
        _minimumValueThreshold = 0.5m;

        // Test user IDs to filter out (for cleaner analytics data)
        _testUserIds = new HashSet<string>
        {
            "user_001", "user_002", // Mark these as test users
        };

        Console.WriteLine("EventFilterTransform: Initialized with filtering criteria");
        Console.WriteLine($"EventFilterTransform: Relevant event types: {string.Join(", ", _relevantEventTypes)}");
        Console.WriteLine($"EventFilterTransform: Minimum value threshold: {_minimumValueThreshold}");
        Console.WriteLine($"EventFilterTransform: Test users to exclude: {string.Join(", ", _testUserIds)}");
    }

    /// <summary>
    ///     Processes a single analytics event and determines if it should be included in the analytics.
    /// </summary>
    /// <param name="item">The analytics event to filter.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A FilteredAnalyticsEvent indicating whether the event is relevant and why.</returns>
    public override async Task<FilteredAnalyticsEvent> ExecuteAsync(AnalyticsEvent item, PipelineContext context, CancellationToken cancellationToken)
    {
        _totalEventsProcessed++;

        // Check if event type is relevant
        if (!_relevantEventTypes.Contains(item.EventType))
        {
            _eventsFilteredOut++;
            var result = new FilteredAnalyticsEvent(item, false, AnalyticsConstants.IrrelevantEventType);
            await LogFilterDecision(item, result);
            return result;
        }

        // Check if event meets minimum value threshold
        if (item.Value < _minimumValueThreshold)
        {
            _eventsFilteredOut++;
            var result = new FilteredAnalyticsEvent(item, false, AnalyticsConstants.LowValueEvent);
            await LogFilterDecision(item, result);
            return result;
        }

        // Check if event is from a test user
        if (_testUserIds.Contains(item.UserId))
        {
            _eventsFilteredOut++;
            var result = new FilteredAnalyticsEvent(item, false, AnalyticsConstants.TestEvent);
            await LogFilterDecision(item, result);
            return result;
        }

        // Event passed all filters - it's relevant
        var relevantResult = new FilteredAnalyticsEvent(item, true, AnalyticsConstants.RelevantEvent);
        await LogFilterDecision(item, relevantResult);
        return relevantResult;
    }

    /// <summary>
    ///     Logs the filtering decision for debugging and monitoring purposes.
    /// </summary>
    /// <param name="originalEvent">The original analytics event.</param>
    /// <param name="filteredEvent">The filtering result.</param>
    /// <returns>A task representing the logging operation.</returns>
    private async Task LogFilterDecision(AnalyticsEvent originalEvent, FilteredAnalyticsEvent filteredEvent)
    {
        await Task.CompletedTask; // Simulate async logging

        if (!filteredEvent.IsRelevant)
        {
            Console.WriteLine(
                $"EventFilterTransform: Filtered out event {originalEvent.EventId} " +
                $"(type: {originalEvent.EventType}, user: {originalEvent.UserId}) " +
                $"- Reason: {filteredEvent.FilterReason}");
        }
        else
        {
            Console.WriteLine(
                $"EventFilterTransform: Passed event {originalEvent.EventId} " +
                $"(type: {originalEvent.EventType}, value: {originalEvent.Value:C})");
        }
    }

    /// <summary>
    ///     Gets the current filtering statistics.
    /// </summary>
    /// <returns>A tuple containing total processed and filtered out counts.</returns>
    public (int TotalProcessed, int FilteredOut) GetStatistics()
    {
        return (_totalEventsProcessed, _eventsFilteredOut);
    }
}
