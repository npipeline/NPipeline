using NPipeline.Pipeline;
using Sample_AggregateNode.Models;
using Sample_AggregateNode.Nodes;

namespace Sample_AggregateNode;

/// <summary>
///     Real-time analytics dashboard pipeline demonstrating AggregateNode usage for streaming analytics.
///     This pipeline showcases both tumbling and sliding window aggregations for comprehensive analytics.
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern and demonstrates:
///     1. Real-time analytics event generation with realistic timing
///     2. Event filtering to focus on relevant analytics data
///     3. Tumbling window aggregation for event counting by type (1-minute windows)
///     4. Sliding window aggregation for value summation by category (30-second windows sliding every 10 seconds)
///     5. Real-time dashboard display with formatted metrics
///     6. Watermark and late data handling for event-time processing
///     7. Key-based aggregation patterns for different analytics dimensions
///     The pipeline flow:
///     EventSource -> EventFilterTransform -> [Branch] -> EventCountAggregator -> ConsoleMetricsSink
///     -> [Branch] -> EventSumAggregator -> ConsoleMetricsSink
/// </remarks>
public class AggregateNodePipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a branching pipeline flow that demonstrates both aggregation types:
    ///     1. EventSource generates realistic analytics events with various types and values
    ///     2. EventFilterTransform filters out irrelevant events to focus analytics on meaningful data
    ///     3. EventCountAggregator counts events by type using 1-minute tumbling windows (non-overlapping)
    ///     4. EventSumAggregator sums values by category using 30-second sliding windows (overlapping)
    ///     5. ConsoleMetricsSink displays results in a real-time dashboard format
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that generates analytics events
        var source = builder.AddSource<EventSource, AnalyticsEvent>("analytics-event-source");

        // Add the filter transform to process only relevant events
        var filter = builder.AddTransform<EventFilterTransform, AnalyticsEvent, FilteredAnalyticsEvent>(
            "event-filter-transform"
        );

        // Add aggregation nodes for different analytics dimensions
        var eventCountAggregator = builder.AddAggregate<EventCountAggregator, FilteredAnalyticsEvent, string, EventCountMetrics>(
            "event-count-aggregator"
        );

        var eventSumAggregator = builder.AddAggregate<EventSumAggregator, FilteredAnalyticsEvent, string, EventSumMetrics>(
            "event-sum-aggregator"
        );

        // Add console sinks for displaying the aggregated metrics
        var countMetricsSink = builder.AddSink<ConsoleMetricsSink, EventCountMetrics>("count-metrics-sink");
        var sumMetricsSink = builder.AddSink<ConsoleMetricsSink, EventSumMetrics>("sum-metrics-sink");

        // Connect the nodes in a branching flow:
        // source -> filter -> [branch] -> event count aggregator -> count metrics sink
        //                   -> [branch] -> event sum aggregator -> sum metrics sink
        builder.Connect(source, filter);

        // Branch 1: Event counting by type
        builder.Connect(filter, eventCountAggregator);
        builder.Connect(eventCountAggregator, countMetricsSink);

        // Branch 2: Value summation by category
        builder.Connect(filter, eventSumAggregator);
        builder.Connect(eventSumAggregator, sumMetricsSink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"AggregateNode Pipeline Sample:

This sample demonstrates real-time analytics dashboard scenarios using NPipeline's AggregateNode:

Key Features:
- Tumbling Windows: 1-minute non-overlapping windows for event counting by type
- Sliding Windows: 30-second windows sliding every 10 seconds for value summation by category
- Event Filtering: Removes irrelevant events to focus analytics on meaningful data
- Real-Time Dashboard: Displays aggregated metrics in a formatted console dashboard
- Event-Time Processing: Handles late-arriving data with watermarks and timestamp extraction
- Key-Based Aggregation: Groups events by different dimensions (type and category)

Pipeline Architecture:
1. EventSource generates realistic analytics events with:
   - Multiple event types (page_view, click, purchase, add_to_cart, search)
   - Various categories (user_engagement, e_commerce, search_activity, authentication)
   - Realistic timing patterns with variable frequency (1-5 events/second)
   - Value assignments based on event type significance

2. EventFilterTransform processes events to:
   - Filter out irrelevant event types (login, logout)
   - Remove low-value events (< $0.50)
   - Exclude test user data for cleaner analytics
   - Provide filtering statistics and reasoning

3. EventCountAggregator performs tumbling window aggregation:
   - Groups events by EventType using 1-minute tumbling windows
   - Counts events per type within each discrete time window
   - Calculates events-per-second rates for each window
   - Demonstrates non-overlapping window processing

4. EventSumAggregator performs sliding window aggregation:
   - Groups events by Category using 30-second sliding windows
   - Sums event values and calculates averages for each window
   - Windows slide every 10 seconds providing overlapping views
   - Demonstrates continuous aggregation patterns

5. ConsoleMetricsSink displays real-time dashboard with:
   - Formatted metrics for both count and sum aggregations
   - Periodic dashboard summaries every 5 seconds
   - Window timing information and processing statistics
   - Clear visualization of aggregation results

AggregateNode Concepts Demonstrated:
- Time-based aggregation with different window strategies
- Key extraction and grouping for multi-dimensional analytics
- Accumulator creation and incremental updates
- Watermark handling for event-time processing
- Performance metrics and monitoring capabilities

This implementation provides a foundation for building real-time analytics dashboards
with NPipeline, demonstrating both basic and advanced aggregation patterns using AggregateNode.";
    }
}
