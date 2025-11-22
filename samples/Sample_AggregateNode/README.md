# Sample_AggregateNode: Real-Time Analytics Dashboard

This sample demonstrates how to use NPipeline's `AggregateNode` for building real-time analytics dashboards with time-based aggregations. It showcases both
tumbling and sliding window patterns for processing streaming analytics data.

## Overview

The sample simulates a real-time analytics pipeline that processes user interaction events (page views, clicks, purchases, etc.) and generates aggregated
metrics for a dashboard display. It demonstrates key concepts in stream processing including windowing, filtering, and multi-dimensional aggregations.

## Key Features Demonstrated

### Windowing Patterns

- **Tumbling Windows**: 1-minute non-overlapping windows for event counting by type
- **Sliding Windows**: 30-second windows sliding every 10 seconds for value summation by category

### Aggregation Patterns

- **Time-based Aggregation**: Events grouped within specific time windows
- **Key-based Aggregation**: Events grouped by different dimensions (EventType and Category)
- **Multi-dimensional Analytics**: Simultaneous counting and summation aggregations

### Stream Processing Concepts

- **Event Filtering**: Removes irrelevant events to focus on meaningful analytics data
- **Event-Time Processing**: Handles late-arriving data with watermarks and timestamp extraction
- **Real-time Dashboard**: Displays aggregated metrics in a formatted console interface

## Pipeline Architecture

```
EventSource
    ↓
EventFilterTransform
    ├── EventCountAggregator → ConsoleMetricsSink (EventCountMetrics)
    └── EventSumAggregator → ConsoleMetricsSink (EventSumMetrics)
```

### Components

1. **EventSource**: Generates realistic analytics events with various types and values
2. **EventFilterTransform**: Filters out irrelevant events (test users, low-value events)
3. **EventCountAggregator**: Counts events by type using tumbling windows
4. **EventSumAggregator**: Sums values by category using sliding windows
5. **ConsoleMetricsSink**: Displays aggregated metrics in a dashboard format

## Data Models

### AnalyticsEvent

Represents a real-time analytics event with:

- Event ID, type, and category
- Monetary value and timestamp
- User ID and event properties

### FilteredAnalyticsEvent

Wraps an AnalyticsEvent with filtering results:

- Original event data
- Relevance flag
- Filter reason

### EventCountMetrics

Aggregated event counts by type:

- Event type and count
- Time window boundaries
- Events-per-second rate

### EventSumMetrics

Aggregated value sums by category:

- Category and total value
- Event count and average value
- Time window boundaries
- Value-per-second rate

## AggregateNode Implementation

### EventCountAggregator (Tumbling Windows)

```csharp
public class EventCountAggregator : AggregateNode<FilteredAnalyticsEvent, string, EventCountMetrics>
{
    public EventCountAggregator()
        : base(WindowAssigner.Tumbling(TimeSpan.FromMinutes(1)))
    {
        // 1-minute non-overlapping windows
    }

    public override string GetKey(FilteredAnalyticsEvent item)
    {
        return item.IsRelevant ? item.OriginalEvent.EventType : string.Empty;
    }

    public override EventCountMetrics CreateAccumulator()
    {
        // Initialize default metrics
    }

    public override EventCountMetrics Accumulate(EventCountMetrics accumulator, FilteredAnalyticsEvent item)
    {
        // Increment count for each relevant event
    }
}
```

### EventSumAggregator (Sliding Windows)

```csharp
public class EventSumAggregator : AggregateNode<FilteredAnalyticsEvent, string, EventSumMetrics>
{
    public EventSumAggregator()
        : base(WindowAssigner.Sliding(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(10)))
    {
        // 30-second windows sliding every 10 seconds
    }

    public override string GetKey(FilteredAnalyticsEvent item)
    {
        return item.IsRelevant ? item.OriginalEvent.Category : string.Empty;
    }

    public override EventSumMetrics CreateAccumulator()
    {
        // Initialize default metrics
    }

    public override EventSumMetrics Accumulate(EventSumMetrics accumulator, FilteredAnalyticsEvent item)
    {
        // Sum values and calculate averages
    }
}
```

## Running the Sample

### Prerequisites

- .NET 8.0, 9.0, or 10.0
- NPipeline dependencies (automatically included)

### Execution

```bash
cd samples/Sample_AggregateNode
dotnet run
```

### Expected Output

The sample will display:

1. Event generation progress and statistics
2. Filtering decisions and statistics
3. Real-time aggregation results
4. Periodic dashboard summaries

Example dashboard output:

```
=== REAL-TIME ANALYTICS DASHBOARD ===
Last Updated: 2024-01-15 14:30:25 UTC
Total Metrics Processed: 47

Current Time Windows:
  Tumbling Window: 14:30:00 - 14:31:00
  Sliding Windows: Multiple 30s windows overlapping

[COUNT] PAGE_VIEW: 15 events (0.25/sec) Window: 14:30:00 - 14:31:00
[COUNT] CLICK: 8 events (0.13/sec) Window: 14:30:00 - 14:31:00
[SUM] E_COMMERCE: Total=$1,234.56 Avg=$154.32 (8 events) (5.15/sec) Window: 14:29:50 - 14:30:20
[SUM] USER_ENGAGEMENT: Total=$45.67 Avg=$3.05 (15 events) (0.19/sec) Window: 14:29:50 - 14:30:20
```

## Key Concepts Demonstrated

### Windowing Strategies

- **Tumbling Windows**: Discrete, non-overlapping time periods for clear-cut analytics
- **Sliding Windows**: Overlapping windows for continuous monitoring and trend detection

### Aggregation Patterns

- **Counting**: Simple event counting for volume metrics
- **Summation**: Value aggregation for business metrics
- **Rate Calculation**: Deriving per-second metrics from windowed data

### Stream Processing

- **Event Filtering**: Improving data quality by removing noise
- **Event-Time Processing**: Handling out-of-order data with watermarks
- **Real-time Display**: Immediate feedback on streaming results

## Performance Considerations

### Memory Usage

- Tumbling windows maintain one window per key per time period
- Sliding windows maintain multiple overlapping windows (3x in this sample)
- Consider window sizes based on memory constraints and analytics requirements

### Latency vs. Accuracy

- Smaller windows provide lower latency but may be less accurate
- Larger windows provide better accuracy but increase latency
- Sliding windows provide more frequent updates at the cost of higher memory usage

### Scaling

- AggregateNode is designed for horizontal scaling
- Consider partitioning strategies for high-volume scenarios
- Monitor window counts and processing metrics for performance tuning

## Extending the Sample

### Additional Aggregations

- Add more aggregators for different dimensions (user-based, geographic, etc.)
- Implement custom window assigners for specific business requirements
- Add session-based aggregations using custom windowing logic

### Enhanced Analytics

- Implement anomaly detection on aggregated metrics
- Add trend analysis and forecasting capabilities
- Integrate with external monitoring systems

### Production Considerations

- Add persistence for aggregated results
- Implement error handling and recovery mechanisms
- Add comprehensive logging and monitoring

## Related Samples

- `Sample_StreamingAnalytics`: Demonstrates more advanced windowing patterns
- `Sample_ComplexDataTransformations`: Shows multi-stream joins and complex aggregations
- `Sample_AdvancedErrorHandling`: Illustrates production-ready error handling patterns

## Summary

This sample provides a comprehensive introduction to using `AggregateNode` for real-time analytics scenarios. It demonstrates the core concepts of windowed
stream processing while maintaining simplicity and clarity. The patterns shown here form the foundation for building sophisticated analytics dashboards and
monitoring systems with NPipeline.
