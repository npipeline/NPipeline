using NPipeline.Pipeline;
using Sample_07_StreamingAnalytics.Nodes;

namespace Sample_07_StreamingAnalytics;

/// <summary>
///     Streaming analytics pipeline demonstrating windowed processing and real-time aggregations.
///     This pipeline implements advanced stream processing concepts including tumbling windows, sliding windows,
///     and real-time analytics with late-arriving data handling.
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern and demonstrates:
///     1. Time-series data generation with realistic timing
///     2. Tumbling window processing (non-overlapping time windows)
///     3. Sliding window processing (overlapping time windows)
///     4. Real-time aggregations and statistical analysis
///     5. Late-arriving data handling
///     6. Performance optimization for streaming scenarios
///     The pipeline flow:
///     TimeSeriesSource -> TumblingWindowTransform -> AggregationTransform -> ConsoleSink
///     -> SlidingWindowTransform -> AggregationTransform -> ConsoleSink
/// </remarks>
public class StreamingAnalyticsPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a branching pipeline flow that demonstrates both tumbling and sliding windows:
    ///     1. TimeSeriesSource generates realistic time-series data with late-arriving data simulation
    ///     2. TumblingWindowTransform processes data in 5-second non-overlapping windows
    ///     3. SlidingWindowTransform processes data in 5-second windows sliding every 2 seconds
    ///     4. AggregationTransform enriches windowed results with statistical analysis
    ///     5. ConsoleSink outputs the final results with detailed formatting
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that generates time-series data
        var source = builder.AddSource<TimeSeriesSource, TimeSeriesData>("time-series-source");

        // Add tumbling window transform (5-second windows, 10-second allowed lateness)
        var tumblingWindow = builder.AddTransform<TumblingWindowTransform, TimeSeriesData, WindowedResult>(
            "tumbling-window-transform"
        );

        // Add sliding window transform (5-second windows, sliding every 2 seconds, 10-second allowed lateness)
        var slidingWindow = builder.AddTransform<SlidingWindowTransform, TimeSeriesData, WindowedResult>(
            "sliding-window-transform"
        );

        // Add aggregation transforms for both window types
        var tumblingAggregation = builder.AddTransform<AggregationTransform, WindowedResult, WindowedResult>(
            "tumbling-aggregation-transform"
        );

        var slidingAggregation = builder.AddTransform<AggregationTransform, WindowedResult, WindowedResult>(
            "sliding-aggregation-transform"
        );

        // Add console sinks for both window types
        var tumblingSink = builder.AddSink<ConsoleSink, WindowedResult>("tumbling-console-sink");
        var slidingSink = builder.AddSink<ConsoleSink, WindowedResult>("sliding-console-sink");

        // Connect the nodes in a branching flow:
        // source -> tumbling window -> tumbling aggregation -> tumbling sink
        //        -> sliding window -> sliding aggregation -> sliding sink
        builder.Connect(source, tumblingWindow);
        builder.Connect(tumblingWindow, tumblingAggregation);
        builder.Connect(tumblingAggregation, tumblingSink);

        builder.Connect(source, slidingWindow);
        builder.Connect(slidingWindow, slidingAggregation);
        builder.Connect(slidingAggregation, slidingSink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"Streaming Analytics Pipeline Sample:

This sample demonstrates advanced stream processing concepts with NPipeline:

Key Features:
- Tumbling Windows: Non-overlapping 5-second time windows
- Sliding Windows: 5-second windows sliding every 2 seconds
- Late-Arriving Data: Handling of out-of-order data with configurable lateness tolerance
- Real-Time Aggregations: Statistical analysis including trend detection and anomaly detection
- Performance Optimization: Efficient streaming processing with minimal memory overhead

Pipeline Architecture:
1. TimeSeriesSource generates realistic sensor data with:
   - Multiple data sources (Sensor-A, Sensor-B, Sensor-C, Sensor-D)
   - Daily cycle patterns with sinusoidal variations
   - Random noise and occasional late-arriving data (10% probability)

2. TumblingWindowTransform processes data in fixed 5-second windows:
   - Non-overlapping windows for discrete time periods
   - Configurable allowed lateness (10 seconds)
   - Automatic window advancement and data cleanup

3. SlidingWindowTransform processes data in overlapping windows:
   - 5-second windows sliding every 2 seconds
   - Maintains data continuity across window boundaries
   - Efficient data management with automatic cleanup

4. AggregationTransform enriches results with:
   - Trend analysis using linear regression
   - Anomaly detection based on statistical deviation
   - Performance metrics and source diversity analysis

5. ConsoleSink outputs detailed results with:
   - Formatted window statistics
   - Late data tracking
   - Source diversity metrics
   - Processing performance summary

This implementation demonstrates production-ready streaming analytics with:
- Proper error handling and resource management
- Efficient memory usage for high-throughput scenarios
- Configurable window parameters for different use cases
- Comprehensive logging and monitoring capabilities";
    }
}
