using NPipeline.Pipeline;
using Sample_LookupNode.Models;
using Sample_LookupNode.Nodes;

namespace Sample_LookupNode;

/// <summary>
///     Pipeline definition demonstrating IoT sensor data enrichment using LookupNode.
///     This pipeline implements an IoT data processing flow:
///     1. SensorSource generates raw sensor readings
///     2. DeviceMetadataLookup enriches data with device information using LookupNode
///     3. CalibrationValidationNode validates sensor calibration status
///     4. RiskAssessmentNode calculates risk levels based on enriched data
///     5. EnrichedSink outputs enriched sensor data
///     6. AlertingSink handles high-priority alerts
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern, which allows the pipeline
///     structure to be defined once and reused multiple times. Each execution creates fresh
///     instances of all nodes, ensuring proper isolation between runs.
/// </remarks>
public class LookupNodePipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a complex pipeline flow with branching:
    ///     SensorSource -> DeviceMetadataLookup -> CalibrationValidationNode -> RiskAssessmentNode -> EnrichedSink
    ///     \-> AlertingSink
    ///     The pipeline processes IoT sensor data through these stages:
    ///     1. Source generates raw sensor readings from IoT devices
    ///     2. LookupNode enriches data with device metadata
    ///     3. Validation ensures calibration is current
    ///     4. Risk assessment calculates danger levels
    ///     5. Sink outputs enriched data for analysis
    ///     6. Alert sink handles critical issues
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that generates sensor readings
        var sensorSourceHandle = builder.AddSource<SensorSource, SensorReading>("SensorSource");

        // Add the lookup node that enriches sensor data with device metadata
        var deviceMetadataLookupHandle = builder.AddTransform<DeviceMetadataLookup, SensorReading, SensorReadingWithMetadata>("DeviceMetadataLookup");

        // Add the calibration validation node
        var calibrationValidationHandle =
            builder.AddTransform<CalibrationValidationNode, SensorReadingWithMetadata, SensorReadingWithCalibration>("CalibrationValidationNode");

        // Add the risk assessment node
        var riskAssessmentHandle = builder.AddTransform<RiskAssessmentNode, SensorReadingWithCalibration, EnrichedSensorReading>("RiskAssessmentNode");

        // Add the enriched data sink for detailed output
        var enrichedSinkHandle = builder.AddSink<EnrichedSink, EnrichedSensorReading>("EnrichedSink");

        // Add the alerting sink for high-priority issues
        var alertingSinkHandle = builder.AddSink<AlertingSink, EnrichedSensorReading>("AlertingSink");

        // Connect the nodes in sequence
        builder.Connect(
            sensorSourceHandle,
            deviceMetadataLookupHandle
        );

        builder.Connect(
            deviceMetadataLookupHandle,
            calibrationValidationHandle
        );

        builder.Connect(
            calibrationValidationHandle,
            riskAssessmentHandle
        );

        // Branch the output to both sinks
        builder.Connect(
            riskAssessmentHandle,
            enrichedSinkHandle
        );

        builder.Connect(
            riskAssessmentHandle,
            alertingSinkHandle
        );
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"IoT Sensor Data Enrichment with LookupNode Sample:

This sample demonstrates advanced NPipeline concepts for IoT scenarios:
- Using LookupNode for external data enrichment
- Processing IoT sensor data streams
- Device metadata lookup and validation
- Risk assessment and alerting
- Multi-sink output patterns
- Complex data transformation pipelines

The pipeline flow:
1. SensorSource generates raw IoT sensor readings
2. DeviceMetadataLookup enriches data using LookupNode pattern
3. CalibrationValidationNode validates sensor calibration status
4. RiskAssessmentNode calculates risk levels based on enriched data
5. EnrichedSink outputs processed sensor data for analysis
6. AlertingSink handles high-priority alerts separately

Key features demonstrated:
- LookupNode for efficient external data lookups
- Branching pipeline patterns for different output types
- IoT data modeling with records
- Async data processing with proper cancellation
- Error handling and validation patterns
- Multi-sink output for different data consumers

This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic";
    }
}
