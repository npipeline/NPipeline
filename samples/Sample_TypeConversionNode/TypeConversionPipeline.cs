using System;
using System.Text.Json;
using System.Threading;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_TypeConversionNode.Nodes;

namespace Sample_TypeConversionNode;

/// <summary>
///     Type Conversion pipeline demonstrating comprehensive TypeConversionNode functionality.
///     This pipeline showcases multiple conversion scenarios and real-world integration patterns.
/// </summary>
/// <remarks>
///     This implementation demonstrates advanced NPipeline concepts including:
///     - Multiple data source types with different formats
///     - TypeConversionNode with various mapping strategies
///     - Error handling and monitoring for conversions
///     - Complex transformation chains
///     - Business logic integration with type conversion
///     - Performance monitoring and validation
///     The pipeline processes data through multiple stages:
///     1. Raw data generation from various sources
///     2. Type conversion using TypeConversionNode
///     3. Business logic enrichment
///     4. Final formatting for different output requirements
///     5. Error collection and analysis
/// </remarks>
public sealed class TypeConversionPipeline : IPipelineDefinition
{
    private bool _enableErrorHandling;
    private int _recordCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="TypeConversionPipeline" /> class.
    /// </summary>
    /// <param name="enableErrorHandling">Whether to enable error handling demonstrations.</param>
    /// <param name="recordCount">The number of records to process per source.</param>
    public TypeConversionPipeline(bool enableErrorHandling = true, int recordCount = 10)
    {
        _enableErrorHandling = enableErrorHandling;
        _recordCount = recordCount;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="TypeConversionPipeline" /> class.
    /// </summary>
    public TypeConversionPipeline() : this(true)
    {
    }

    /// <summary>
    ///     Defines the pipeline structure with multiple conversion paths.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration.</param>
    /// <remarks>
    ///     This method creates a comprehensive pipeline with multiple parallel conversion paths:
    ///     - String data path: RawStringData -> SensorData -> SensorReading -> SensorDto
    ///     - JSON data path: JsonStringData -> SensorData -> SensorReading -> SensorDto
    ///     - Legacy data path: LegacySensorFormat -> CanonicalSensorData
    ///     Each path demonstrates different TypeConversionNode patterns and strategies.
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read configuration from context
        if (context.Parameters.TryGetValue("EnableErrorHandling", out var errorHandlingObj) && errorHandlingObj is bool enableErrorHandling)
            _enableErrorHandling = enableErrorHandling;

        if (context.Parameters.TryGetValue("RecordCount", out var recordCountObj) && recordCountObj is int recordCount)
            _recordCount = recordCount;

        // Store configuration for nodes
        context.Parameters["EnableErrorHandling"] = _enableErrorHandling;
        context.Parameters["RecordCount"] = _recordCount;

        // === STRING DATA CONVERSION PATH ===
        Console.WriteLine("📋 Setting up String Data Conversion Path...");

        // Source: Raw string data (simulating CSV/log files)
        var stringSource = builder.AddSource<RawStringDataSource, RawStringData>("string-source");

        // Configure string source with parameters
        builder.AddPreconfiguredNodeInstance(stringSource.Id, new RawStringDataSource(_recordCount, TimeSpan.FromMilliseconds(200)));

        // Conversion 1: String to SensorData with parsing and validation
        var stringToSensorData = builder.AddTransform<StringToSensorDataConverter, RawStringData, SensorData>("string-to-sensor");

        // Conversion 2: SensorData to SensorReading with enrichment
        var stringEnrichment = builder.AddTransform<SensorDataEnrichmentTransform, SensorData, SensorReading>("string-enrichment");

        // Conversion 3: SensorReading to DTO for API response
        var stringToDto = builder.AddTransform<SensorReadingToDtoConverter, SensorReading, SensorDto>("string-to-dto");

        // Output: Console sink for string path
        var stringSink = builder.AddSink<ConsoleSink<SensorDto>, SensorDto>("string-sink");

        // Error handling for string path
        IInputNodeHandle<ConversionError>? stringErrorSink = null;

        if (_enableErrorHandling)
            stringErrorSink = builder.AddSink<ErrorSink, ConversionError>("string-error-sink");

        // === JSON DATA CONVERSION PATH ===
        Console.WriteLine("📄 Setting up JSON Data Conversion Path...");

        // Source: JSON data (simulating API/message queue)
        var jsonSource = builder.AddSource<JsonDataSource, JsonStringData>("json-source");

        // Configure JSON source with parameters
        builder.AddPreconfiguredNodeInstance(jsonSource.Id, new JsonDataSource(_recordCount / 2, TimeSpan.FromMilliseconds(300)));

        // Conversion 1: JSON to SensorData with complex parsing
        var jsonToSensorData = builder.AddTransform<JsonToSensorDataConverter, JsonStringData, SensorData>("json-to-sensor");

        // Conversion 2: SensorData to SensorReading with enrichment
        var jsonEnrichment = builder.AddTransform<SensorDataEnrichmentTransform, SensorData, SensorReading>("json-enrichment");

        // Conversion 3: SensorReading to DTO for API response
        var jsonToDto = builder.AddTransform<SensorReadingToDtoConverter, SensorReading, SensorDto>("json-to-dto");

        // Output: Console sink for JSON path
        var jsonSink = builder.AddSink<ConsoleSink<SensorDto>, SensorDto>("json-sink");

        // Error handling for JSON path
        IInputNodeHandle<ConversionError>? jsonErrorSink = null;

        if (_enableErrorHandling)
            jsonErrorSink = builder.AddSink<ErrorSink, ConversionError>("json-error-sink");

        // === LEGACY DATA CONVERSION PATH ===
        Console.WriteLine("🏛️ Setting up Legacy Data Conversion Path...");

        // Source: Legacy format data (simulating old systems)
        var legacySource = builder.AddSource<LegacyDataSource, LegacySensorFormat>("legacy-source");

        // Configure legacy source with parameters
        builder.AddPreconfiguredNodeInstance(legacySource.Id, new LegacyDataSource(_recordCount / 3, TimeSpan.FromMilliseconds(400)));

        // Conversion: Legacy to Canonical format with naming convention changes
        var legacyToCanonical = builder.AddTransform<LegacyToCanonicalConverter, LegacySensorFormat, CanonicalSensorData>("legacy-to-canonical");

        // Output: Console sink for legacy path
        var legacySink = builder.AddSink<ConsoleSink<CanonicalSensorData>, CanonicalSensorData>("legacy-sink");

        // Error handling for legacy path
        IInputNodeHandle<ConversionError>? legacyErrorSink = null;

        if (_enableErrorHandling)
            legacyErrorSink = builder.AddSink<ErrorSink, ConversionError>("legacy-error-sink");

        // === CONNECT STRING PATH ===
        Console.WriteLine("🔗 Connecting String Data Path...");
        builder.Connect(stringSource, stringToSensorData);
        builder.Connect(stringToSensorData, stringEnrichment);
        builder.Connect(stringEnrichment, stringToDto);
        builder.Connect(stringToDto, stringSink);

        // === CONNECT JSON PATH ===
        Console.WriteLine("🔗 Connecting JSON Data Path...");
        builder.Connect(jsonSource, jsonToSensorData);
        builder.Connect(jsonToSensorData, jsonEnrichment);
        builder.Connect(jsonEnrichment, jsonToDto);
        builder.Connect(jsonToDto, jsonSink);

        // === CONNECT LEGACY PATH ===
        Console.WriteLine("🔗 Connecting Legacy Data Path...");
        builder.Connect(legacySource, legacyToCanonical);
        builder.Connect(legacyToCanonical, legacySink);

        // === ERROR HANDLING SETUP ===
        if (_enableErrorHandling)
        {
            Console.WriteLine("⚠️ Setting up Error Handling...");

            // For this sample, we'll create error scenarios by connecting error sources
            // In a real scenario, errors would come from failed transformations
            SetupErrorHandling(builder, context, stringErrorSink, jsonErrorSink, legacyErrorSink);
        }

        Console.WriteLine("✅ Pipeline configuration completed!");
    }

    /// <summary>
    ///     Sets up error handling demonstrations for the pipeline.
    /// </summary>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="stringErrorSink">The string path error sink.</param>
    /// <param name="jsonErrorSink">The JSON path error sink.</param>
    /// <param name="legacyErrorSink">The legacy path error sink.</param>
    private static void SetupErrorHandling(PipelineBuilder builder, PipelineContext context,
        IInputNodeHandle<ConversionError>? stringErrorSink, IInputNodeHandle<ConversionError>? jsonErrorSink,
        IInputNodeHandle<ConversionError>? legacyErrorSink)
    {
        // Create error sources for demonstration
        var errorSource = builder.AddSource<ErrorSource, ConversionError>("error-source");
        builder.AddPreconfiguredNodeInstance(errorSource.Id, new ErrorSource());

        // Connect error source to error sinks
        // Note: In a real implementation, errors would come from failed transformations
        // This is just for demonstration purposes
        if (stringErrorSink != null)
            builder.Connect(errorSource, stringErrorSink);

        if (jsonErrorSink != null)
            builder.Connect(errorSource, jsonErrorSink);

        if (legacyErrorSink != null)
            builder.Connect(errorSource, legacyErrorSink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"TypeConversionNode Sample:

This sample demonstrates comprehensive TypeConversionNode functionality for real-world data integration scenarios:

Key Concepts Demonstrated:
- TypeConversionNode with AutoMap() for automatic property mapping
- Custom mapping with complex transformation logic
- String parsing with validation and error handling
- JSON deserialization with nested structure extraction
- Legacy system integration with naming convention changes
- Business logic enrichment during type conversion
- API response formatting with naming convention conversion
- Error handling and monitoring for conversion failures
- Performance optimization through compiled expressions

Pipeline Flow:
The pipeline processes three parallel data paths, each demonstrating different conversion scenarios:

1. String Data Path (CSV/Log Integration):
   RawStringData -> SensorData -> SensorReading -> SensorDto
   Demonstrates string parsing, validation, and API formatting

2. JSON Data Path (API/Message Queue Integration):
   JsonStringData -> SensorData -> SensorReading -> SensorDto
   Demonstrates JSON deserialization, complex parsing, and business logic

3. Legacy Data Path (Legacy System Integration):
   LegacySensorFormat -> CanonicalSensorData
   Demonstrates naming convention changes and enterprise integration

TypeConversionNode Patterns Demonstrated:
- AutoMap() for automatic property matching
- Custom Map() with source-only converters
- Custom Map() with whole-input converters
- Custom factory functions for complex initialization
- Record type support with default values
- Error handling with fallback values
- Business logic integration during conversion

Real-World Scenarios:
- Data ingestion from CSV files and logs
- API integration with JSON payloads
- Legacy system modernization
- Data quality validation and enrichment
- API response formatting for external consumption
- Error monitoring and analysis for production systems

Performance Considerations:
- Expression compilation for high-performance conversions
- Minimal reflection through compiled accessors
- Cached type information for efficiency
- Error handling without performance impact
- Parallel processing of multiple data streams

This implementation showcases production-ready patterns for:
- Enterprise data integration
- Legacy system modernization
- API development and consumption
- Data quality management
- Error monitoring and troubleshooting";
    }
}

/// <summary>
///     Error source for demonstrating error handling in the pipeline.
/// </summary>
internal sealed class ErrorSource : SourceNode<ConversionError>
{
    public override IDataPipe<ConversionError> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        // For demonstration, return a few sample errors
        var errors = new[]
        {
            new ConversionError(
                "demo-error-1",
                "Sample parsing error for demonstration",
                new InvalidOperationException("Demo exception"),
                DateTime.UtcNow,
                "StringData"
            ),
            new ConversionError(
                "demo-error-2",
                "Sample JSON processing error",
                new JsonException("Demo JSON error"),
                DateTime.UtcNow,
                "JsonData"
            ),
            new ConversionError(
                "demo-error-3",
                "Sample legacy format error",
                new FormatException("Demo legacy error"),
                DateTime.UtcNow,
                "LegacyData"
            ),
        };

        return new ListDataPipe<ConversionError>(errors, "ErrorSource");
    }
}
