using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Pipeline;
using Sample_WatermarkHandling.Models;
using Sample_WatermarkHandling.Nodes;
using Sample_WatermarkHandling.Strategies;

namespace Sample_WatermarkHandling;

/// <summary>
///     Pipeline definition for WatermarkHandling sample demonstrating advanced event-time processing
///     for IoT manufacturing platforms with sophisticated watermark management and late data handling.
/// </summary>
/// <remarks>
///     This implementation follows IPipelineDefinition pattern and demonstrates:
///     1. Multi-stream IoT sensor data generation with different network characteristics
///     2. Adaptive watermark generation based on network conditions and device capabilities
///     3. Multi-stream watermark synchronization and alignment
///     4. Configurable late data handling with tolerance policies
///     5. Time-windowed processing with watermark-based window advancement
///     6. Comprehensive monitoring and metrics collection
///     7. Dynamic watermark adjustment based on system load and conditions
///     The pipeline flow:
///     Production Line A Source → AdaptiveWatermarkGenerator → WatermarkAligner → LateDataFilter → TimeWindowedAggregator → MonitoringSink
///     Production Line B Source → AdaptiveWatermarkGenerator → WatermarkAligner → LateDataFilter → TimeWindowedAggregator → MonitoringSink
///     Environmental Source → AdaptiveWatermarkGenerator → WatermarkAligner → LateDataFilter → TimeWindowedAggregator → MonitoringSink
/// </remarks>
public class WatermarkHandlingPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a sophisticated watermark handling pipeline flow that demonstrates:
    ///     1. Multiple IoT sensor sources with different network characteristics
    ///     2. Adaptive watermark generation with network-aware strategies
    ///     3. Multi-stream watermark synchronization and alignment
    ///     4. Late data filtering with configurable tolerance policies
    ///     5. Time-windowed aggregation with watermark-based window advancement
    ///     6. Comprehensive monitoring and metrics collection
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Create strategy instances
        var networkStrategy = new NetworkAwareWatermarkStrategy(
            NullLogger<NetworkAwareWatermarkStrategy>.Instance);

        var deviceStrategy = new DeviceSpecificLatenessStrategy(
            NullLogger<DeviceSpecificLatenessStrategy>.Instance);

        var dynamicStrategy = new DynamicAdjustmentStrategy(
            NullLogger<DynamicAdjustmentStrategy>.Instance);

        // Add IoT sensor sources with different characteristics
        var productionLineASource = builder.AddSource<ProductionLineASource, SensorReading>("production-line-a-source");
        var productionLineBSource = builder.AddSource<ProductionLineBSource, SensorReading>("production-line-b-source");
        var environmentalSource = builder.AddSource<EnvironmentalSource, SensorReading>("environmental-source");

        // Add adaptive watermark generator
        var watermarkGenerator = builder.AddTransform<AdaptiveWatermarkGenerator, SensorReading, SensorReading>("adaptive-watermark-generator");

        // Add watermark aligner for multi-stream synchronization
        var watermarkAligner = builder.AddTransform<WatermarkAligner, SensorReading, SensorReading>("watermark-aligner");

        // Add late data filter with configurable tolerance
        var lateDataFilter = builder.AddTransform<LateDataFilter, SensorReading, SensorReading>("late-data-filter");

        // Add time-windowed aggregator
        var timeWindowedAggregator = builder.AddTransform<TimeWindowedAggregator, SensorReading, ProcessingStats>("time-windowed-aggregator");

        // Add monitoring sink for comprehensive output
        var monitoringSink = builder.AddSink<MonitoringSink, ProcessingStats>("monitoring-sink");

        // Connect all sources to watermark generator
        builder.Connect(productionLineASource, watermarkGenerator);
        builder.Connect(productionLineBSource, watermarkGenerator);
        builder.Connect(environmentalSource, watermarkGenerator);

        // Connect watermark generator to watermark aligner
        builder.Connect(watermarkGenerator, watermarkAligner);

        // Connect watermark aligner to late data filter
        builder.Connect(watermarkAligner, lateDataFilter);

        // Connect late data filter to time-windowed aggregator
        builder.Connect(lateDataFilter, timeWindowedAggregator);

        // Connect time-windowed aggregator to monitoring sink
        builder.Connect(timeWindowedAggregator, monitoringSink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"WatermarkHandling Pipeline Sample:

This sample demonstrates advanced watermark handling capabilities in NPipeline for complex IoT manufacturing scenarios:

Key Features:
- Adaptive Watermark Generation: Dynamic watermark strategies based on network conditions
- Multi-Stream Synchronization: Coordinating watermarks across heterogeneous sensor networks
- Late Data Handling: Configurable tolerance policies for out-of-order data
- Event-Time Processing: Proper temporal windowing with watermark-based advancement
- Network-Aware Processing: Adapting processing strategies to network characteristics
- Dynamic Adjustment: Real-time watermark adjustment based on system load and conditions

Pipeline Architecture:
1. IoT Sensor Sources generate realistic manufacturing data:
   - Production Line A Source: WiFi-based sensors with GPS-disciplined clocks (±1ms accuracy)
   - Production Line B Source: LoRaWAN sensors with NTP synchronization (±10ms accuracy)
   - Environmental Source: Ethernet sensors with internal clocks and drift compensation
   - Realistic sensor data simulation with proper event timestamps and quality indicators

2. AdaptiveWatermarkGenerator performs sophisticated watermark generation:
   - Network-aware watermark generation based on connection type and conditions
   - Device-specific lateness tolerance configuration
   - Dynamic adjustment based on system load and performance metrics
   - Clock drift compensation and synchronization handling
   - Multi-stream coordination and alignment

3. WatermarkAligner synchronizes watermarks across multiple streams:
   - Multi-stream watermark alignment and coordination
   - Temporal synchronization between heterogeneous sensor networks
   - Conflict resolution for competing watermark strategies
   - Unified watermark propagation for downstream processing

4. LateDataFilter handles out-of-order data with configurable policies:
   - Configurable lateness tolerance windows per device type
   - Multiple handling strategies (accept, reject, side-output)
   - Late data analytics and reporting
   - Quality degradation handling and graceful degradation

5. TimeWindowedAggregator performs event-time windowed processing:
   - Watermark-based window advancement and completion
   - Event-time windowing with proper late data handling
   - Window result computation and emission
   - Performance optimization for high-throughput scenarios

6. MonitoringSink provides comprehensive metrics and alerting:
   - Real-time watermark progress tracking and performance metrics
   - Late data event recording and analysis
   - Overall pipeline statistics and health indicators
   - Alerting for unusual conditions and performance degradation

WatermarkHandling Concepts Demonstrated:
- Adaptive Watermark Generation: Dynamic watermark strategies based on network conditions
- Multi-Stream Coordination: Synchronizing watermarks across heterogeneous sensor networks
- Event-Time Processing: Accurate temporal processing with sophisticated watermark management
- Late Data Management: Proper handling of late data ensures data quality and completeness
- Dynamic Adaptation: Real-time adjustment to changing conditions improves system reliability

This implementation provides a foundation for building sophisticated IoT manufacturing systems
with NPipeline, demonstrating how WatermarkHandling enables advanced event-time processing
while maintaining data quality and performance in demanding industrial scenarios.";
    }
}
