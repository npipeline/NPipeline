---
title: Time-Windowed Joins
description: Learn how to perform joins on streams within specific time windows, a key technique for real-time and event-driven data processing.
sidebar_position: 8
---

# Time-Windowed Joins

In many real-time applications, you need to join streams of data that are aligned by time. For example, you might want to correlate user clicks with ad impressions that occurred within the same 5-minute window. NPipeline supports this through the `TimeWindowedJoinNode<TKey, TIn1, TIn2, TOut>` abstract base class.

This node extends `BaseJoinNode<TKey, TIn1, TIn2, TOut>` by adding the concepts of **windows**, **timestamps**, and **watermarks** to manage stateful joins over time.

## Core Concepts

### Windows

A window is a finite slice of time over which a join operation is performed. NPipeline supports different windowing strategies, such as:

* **Tumbling Windows**: Fixed-size, non-overlapping time intervals (e.g., every 1 minute).
* **Sliding Windows**: Fixed-size, overlapping time intervals (e.g., a 5-minute window that slides every 10 seconds).
* **Session Windows**: Dynamically sized windows based on periods of activity followed by a gap of inactivity.

You configure this using a `WindowAssigner` when creating the node.

### Timestamps

Each incoming item must have a timestamp associated with it so the node knows which window(s) it belongs to. You can provide a `TimestampExtractor` to tell the node how to get this timestamp from your data model. If not provided, the node will use the system time of arrival.

### Watermarks

Watermarks are a crucial mechanism in stream processing that represent the progress of event time. A watermark with a timestamp `T` signals to the node that no more items with a timestamp earlier than `T` are expected to arrive. This allows the node to safely close windows, process the joined results, and clean up its internal state (like unmatched items), preventing memory leaks in long-running pipelines.

The `TimeWindowedJoinNode` automatically handles watermark generation based on the incoming data stream.

## `TimeWindowedJoinNode<TKey, TIn1, TIn2, TOut>`

This class manages the complexity of storing items from each stream, matching them based on their key and window, and handling out-of-order data.

### Constructor

To create a time-windowed join, you need to provide:

* **windowAssigner** (`WindowAssigner`): The window assigner strategy to use for defining time windows (required).
* **timestampExtractor1** (`TimestampExtractor<TIn1>?`, optional): A function to extract timestamps from the first input type. If not provided, system time of arrival will be used.
* **timestampExtractor2** (`TimestampExtractor<TIn2>?`, optional): A function to extract timestamps from the second input type. If not provided, system time of arrival will be used.
* **maxOutOfOrderness** (`TimeSpan?`, optional): The maximum allowed lateness for out-of-order events. Events arriving later than this relative to the current watermark may be treated as late. Defaults to 5 minutes.
* **watermarkInterval** (`TimeSpan?`, optional): The frequency at which watermarks are emitted to advance event time and trigger window cleanup. Defaults to 30 seconds.

```csharp
// Example constructor usage
public class MyTimeWindowedJoinNode : TimeWindowedJoinNode<string, EventA, EventB, Result>
{
    public MyTimeWindowedJoinNode() : base(
        windowAssigner: new TumblingWindowAssigner(TimeSpan.FromMinutes(5)),
        timestampExtractor1: evt => evt.Timestamp,
        timestampExtractor2: evt => evt.Timestamp,
        maxOutOfOrderness: TimeSpan.FromSeconds(30),
        watermarkInterval: TimeSpan.FromSeconds(10))
    {
        JoinType = JoinType.Inner;
    }
    
    // Required method implementations...
}
```

### Timestamp Extraction Patterns

Timestamp extraction is crucial for time-windowed joins. You can provide custom timestamp extractors in several ways:

```csharp
// 1. Simple property access
timestampExtractor1: evt => evt.EventTime

// 2. Complex timestamp calculation
timestampExtractor2: evt => 
{
    // Calculate timestamp from multiple fields or business logic
    var baseTime = evt.BaseDate;
    var timeOfDay = TimeSpan.Parse(evt.TimeString);
    return baseTime.Date + timeOfDay;
}

// 3. Using external timestamp sources
timestampExtractor1: evt => 
{
    // Look up timestamp from external system
    return _timestampService.GetTimestamp(evt.EventId);
}

// 4. Default behavior (no extractor provided)
// If no extractor is provided, the system time of arrival is used
```

When providing timestamp extractors, ensure they return consistent, comparable `DateTime` or `DateTimeOffset` values that represent the actual event time, not the processing time.

### Example: Joining Click and Impression Streams

Let's imagine a conceptual example where we want to join a stream of `AdClickEvent`s with a stream of `AdImpressionEvent`s. We want to match them if they occur within the same 1-minute tumbling window.

```csharp
using NPipeline;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;

// Data models with timestamps
public sealed record AdClickEvent(string AdId, DateTime Timestamp, string UserId);
public sealed record AdImpressionEvent(string AdId, DateTime Timestamp, string CampaignId);
public sealed record CorrelatedAdEvent(string AdId, string UserId, string CampaignId);

// Timestamp extractors
public static class Extractors
{
    public static DateTime GetClickTimestamp(AdClickEvent evt) => evt.Timestamp;
    public static DateTime GetImpressionTimestamp(AdImpressionEvent evt) => evt.Timestamp;
}

// Join Node
[KeySelector(typeof(AdClickEvent), nameof(AdClickEvent.AdId))]
[KeySelector(typeof(AdImpressionEvent), nameof(AdImpressionEvent.AdId))]
public class AdCorrelationNode : TimeWindowedJoinNode<string, AdClickEvent, AdImpressionEvent, CorrelatedAdEvent>
{
    public AdCorrelationNode() : base(
        windowAssigner: new TumblingWindowAssigner(TimeSpan.FromMinutes(1)),
        timestampExtractor1: new TimestampExtractor<AdClickEvent>(Extractors.GetClickTimestamp),
        timestampExtractor2: new TimestampExtractor<AdImpressionEvent>(Extractors.GetImpressionTimestamp),
        maxOutOfOrderness: TimeSpan.FromSeconds(30))
    {
        JoinType = JoinType.Inner;
    }

    public override CorrelatedAdEvent CreateOutput(AdClickEvent item1, AdImpressionEvent item2)
    {
        return new CorrelatedAdEvent(item1.AdId, item1.UserId, item2.CampaignId);
    }
}

// Pipeline Setup
public static class Program
{
    public static async Task Main(string[] args)
    {
        var context = PipelineContext.Default;
        var runner = PipelineRunner.Create();
        await runner.RunAsync<AdCorrelationPipelineDefinition>(context);
    }
}

public sealed class AdCorrelationPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var clickSourceHandle = builder.AddSource<ClickStreamSource, AdClickEvent>("clicks");
        var impressionSourceHandle = builder.AddSource<ImpressionStreamSource, AdImpressionEvent>("impressions");
        var joinHandle = builder.AddJoin<AdCorrelationNode, AdClickEvent, AdImpressionEvent, CorrelatedAdEvent>("correlator");
        var sinkHandle = builder.AddSink<ConsoleSink<CorrelatedAdEvent>, CorrelatedAdEvent>("sink");

        builder.Connect(clickSourceHandle, joinHandle);
        builder.Connect(impressionSourceHandle, joinHandle);
        builder.Connect(joinHandle, sinkHandle);
    }
}
```

In this setup, the `AdCorrelationNode` will:

1. Receive clicks and impressions.
2. Use the provided extractors to get their event timestamps.
3. Assign each item to a 1-minute tumbling window.
4. Store items in memory, waiting for a match on `AdId` within the same window.
5. When a match is found, call `CreateOutput` and emit the `CorrelatedAdEvent`.
6. As watermarks advance past the end of a window, it will clean up any state for that window.

## Next Steps

* **[Node Types Index](index.md)**: Return to the node types overview.
* **[Pipeline Execution](../pipeline-execution/index.md)**: Learn about how pipelines are executed and managed.

