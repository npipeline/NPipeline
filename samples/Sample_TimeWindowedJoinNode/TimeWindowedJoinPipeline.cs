using NPipeline.Pipeline;
using Sample_TimeWindowedJoinNode.Nodes;

namespace Sample_TimeWindowedJoinNode;

/// <summary>
///     Time-Windowed Join pipeline demonstrating NPipeline's time-windowed join functionality.
///     This pipeline showcases how to join data streams based on time windows.
/// </summary>
/// <remarks>
///     This implementation demonstrates advanced NPipeline concepts including:
///     - Time-windowed joins using KeyedJoinNode with time-based correlation
///     - Data enrichment through time-windowed analysis
///     - Complex aggregations for time-based insights
///     - Real-time data processing with temporal correlation
/// </remarks>
public class TimeWindowedJoinPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a comprehensive time-windowed join pipeline flow:
    ///     1. SensorReadingSource and MaintenanceEventSource generate separate time-based data streams
    ///     2. SensorMaintenanceJoin joins streams based on DeviceId within time windows
    ///     3. SensorDataEnrichmentTransform enriches joined data with temporal analysis
    ///     4. MaintenanceEffectivenessAggregator generates time-based effectiveness metrics
    ///     5. ConsoleSink outputs the time-windowed analysis results
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add source nodes for different data streams
        var sensorSource = builder.AddSource<SensorReadingSource, SensorReading>("sensor-source");
        var maintenanceSource = builder.AddSource<MaintenanceEventSource, MaintenanceEvent>("maintenance-source");

        // Add join node to combine sensor readings with maintenance events within time windows
        var sensorMaintenanceJoin =
            builder.AddJoin<SensorMaintenanceJoinNode, SensorReading, MaintenanceEvent, SensorMaintenanceJoin>("sensor-maintenance-join");

        // Add transform node to enrich joined data with time-based analysis
        var enrichmentTransform = builder.AddTransform<SensorDataEnrichmentTransform, SensorMaintenanceJoin, EnrichedSensorData>("enrichment-transform");

        // Add aggregation node for maintenance effectiveness metrics
        var effectivenessAggregation =
            builder
                .AddAggregate<MaintenanceEffectivenessAggregator, EnrichedSensorData, string, MaintenanceEffectivenessReport, MaintenanceEffectivenessReport>(
                    "effectiveness-aggregation");

        // Add sink node for outputting results
        var consoleSink = builder.AddSink<ConsoleSink<MaintenanceEffectivenessReport>, MaintenanceEffectivenessReport>("console-sink");

        // Connect nodes in pipeline flow

        // Connect sources to join node inputs
        // SensorReadingSource connects to first input (SensorReading) of join
        builder.Connect(sensorSource, sensorMaintenanceJoin);

        // MaintenanceEventSource connects to second input (MaintenanceEvent) of join
        builder.Connect(maintenanceSource, sensorMaintenanceJoin);

        // Connect join output to enrichment transform
        builder.Connect<SensorMaintenanceJoin>(sensorMaintenanceJoin, enrichmentTransform);

        // Connect enrichment transform to aggregation
        builder.Connect<EnrichedSensorData>(enrichmentTransform, effectivenessAggregation);

        // Connect aggregation to sink
        builder.Connect<MaintenanceEffectivenessReport>(effectivenessAggregation, consoleSink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"Time-Windowed Join Node Sample:

This sample demonstrates NPipeline's time-windowed join functionality for correlating data streams based on time windows:

Key Concepts Demonstrated:
- Time-windowed joins using KeyedJoinNode with temporal correlation
- Multi-stream data processing with time-based correlation
- Data enrichment through time-windowed analysis
- Real-time aggregations for temporal business intelligence
- Handling of late-arriving data and watermarks
- Different time window types (tumbling, sliding, session)

Pipeline Flow:
1. SensorReadingSource generates time-stamped sensor data with DeviceId keys
2. MaintenanceEventSource generates time-stamped maintenance events with DeviceId keys
3. SensorMaintenanceJoin joins streams using DeviceId as the join key within time windows
4. SensorDataEnrichmentTransform enriches joined data with temporal analysis
5. MaintenanceEffectivenessAggregator generates time-based effectiveness metrics
6. ConsoleSink outputs the time-windowed analysis results

Time Window Strategies Demonstrated:
- Tumbling Windows: Fixed-size, non-overlapping time intervals
- Sliding Windows: Fixed-size, overlapping time intervals
- Session Windows: Dynamically sized windows based on activity periods
- Late Data Handling: Configurable allowed lateness for out-of-order data

Performance Considerations:
- Time-windowed joins maintain in-memory buffers for time windows
- Consider memory usage with large time windows or high data volumes
- Join performance depends on time distribution and key cardinality
- Use appropriate window sizes based on business requirements
- Configure watermarks to balance completeness and latency

Real-World Scenarios:
- IoT sensor data correlation with maintenance events
- Financial transaction matching with market data
- User activity analysis with session boundaries
- Log analysis with incident correlation
- Sensor fusion with calibration events

This implementation showcases production-ready patterns for:
- Real-time temporal data correlation and enrichment
- Time-based business intelligence generation
- Handling of out-of-order data in stream processing
- Multi-dimensional temporal analytics on joined data";
    }
}
