using System;
using System.Collections.Generic;
using NPipeline.Pipeline;
using Sample_BatchingNode.Nodes;

namespace Sample_BatchingNode;

/// <summary>
///     Batching pipeline demonstrating the BatchingNode functionality for efficient batch processing.
///     This pipeline implements a complete IoT sensor data processing flow with batching:
///     1. SensorSource generates individual sensor readings
///     2. BatchingNode collects readings into batches based on size and time
///     3. BatchProcessingTransform processes batches efficiently
///     4. DatabaseSink performs bulk database operations
/// </summary>
/// <remarks>
///     This implementation demonstrates different batching strategies and their performance benefits:
///     - Size-based batching: Collects items until reaching a specified batch size
///     - Time-based batching: Emits batches after a time threshold even if not full
///     - Hybrid batching: Combines both size and time thresholds for optimal processing
///     The pipeline shows how batching improves efficiency for:
///     - Computational operations (aggregations, calculations)
///     - Database operations (bulk inserts vs individual inserts)
///     - Network operations (batch API calls vs individual calls)
/// </remarks>
public class BatchingPipeline : IPipelineDefinition
{
    private int _batchSize;
    private TimeSpan _batchTimeout;
    private TimeSpan _sensorInterval;
    private int _sensorReadingCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="BatchingPipeline" /> class.
    /// </summary>
    /// <param name="batchSize">The maximum number of items in a batch.</param>
    /// <param name="batchTimeout">The maximum time to wait before emitting a batch.</param>
    /// <param name="sensorReadingCount">The number of sensor readings to generate.</param>
    /// <param name="sensorInterval">The interval between sensor readings.</param>
    public BatchingPipeline(
        int batchSize = 10,
        TimeSpan? batchTimeout = null,
        int sensorReadingCount = 50,
        TimeSpan? sensorInterval = null)
    {
        _batchSize = batchSize;
        _batchTimeout = batchTimeout ?? TimeSpan.FromSeconds(2);
        _sensorReadingCount = sensorReadingCount;
        _sensorInterval = sensorInterval ?? TimeSpan.FromMilliseconds(100);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="BatchingPipeline" /> class.
    /// </summary>
    public BatchingPipeline()
    {
        _batchSize = 10;
        _batchTimeout = TimeSpan.FromSeconds(2);
        _sensorReadingCount = 50;
        _sensorInterval = TimeSpan.FromMilliseconds(100);
    }

    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a linear pipeline flow with batching:
    ///     SensorSource -> BatchingNode -> BatchProcessingTransform -> DatabaseSink
    ///     The pipeline processes individual sensor readings through these stages:
    ///     1. Source generates individual sensor readings from multiple devices
    ///     2. BatchingNode collects individual readings into batches based on size and time
    ///     3. Transform processes batches efficiently with aggregations and calculations
    ///     4. Sink performs bulk database operations with the batched results
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read parameters from context if available
        if (context.Parameters.TryGetValue("BatchSize", out var batchSizeObj) && batchSizeObj is int batchSize)
            _batchSize = batchSize;

        if (context.Parameters.TryGetValue("BatchTimeout", out var batchTimeoutObj) && batchTimeoutObj is TimeSpan batchTimeout)
            _batchTimeout = batchTimeout;

        if (context.Parameters.TryGetValue("SensorReadingCount", out var sensorCountObj) && sensorCountObj is int sensorCount)
            _sensorReadingCount = sensorCount;

        if (context.Parameters.TryGetValue("SensorInterval", out var sensorIntervalObj) && sensorIntervalObj is TimeSpan sensorInterval)
            _sensorInterval = sensorInterval;

        // Add the source node that generates individual sensor readings
        var source = builder.AddSource<SensorSource, SensorReading>("sensor-source");

        // Add the batching node that collects individual readings into batches
        // This is the key component that demonstrates BatchingNode functionality
        var batching = builder.AddBatcher<SensorReading>("batching-node", _batchSize, _batchTimeout);

        // Add batch processing transform that processes batches efficiently
        var batchProcessing = builder.AddTransform<BatchProcessingTransform, IReadOnlyCollection<SensorReading>, BatchProcessingResult>(
            "batch-processing-transform");

        // Add the database sink that performs bulk operations
        var databaseSink = builder.AddSink<DatabaseSink, BatchProcessingResult>("database-sink");

        // Connect the nodes in a linear flow: source -> batching -> batchProcessing -> databaseSink
        builder.Connect(source, batching);
        builder.Connect(batching, batchProcessing);
        builder.Connect(batchProcessing, databaseSink);

        // Store pipeline configuration in context for access by nodes
        context.Parameters["BatchSize"] = _batchSize;
        context.Parameters["BatchTimeout"] = _batchTimeout;
        context.Parameters["SensorReadingCount"] = _sensorReadingCount;
        context.Parameters["SensorInterval"] = _sensorInterval;
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"BatchingNode Sample:

This sample demonstrates BatchingNode functionality for efficient batch processing:
- Individual item collection into batches
- Size-based and time-based batching strategies
- Efficient batch processing for computational operations
- Bulk database operations with batched results
- Performance benefits of batching over individual processing

The pipeline flow:
1. SensorSource generates individual sensor readings from multiple IoT devices
2. BatchingNode collects individual readings into batches based on size and time thresholds
3. BatchProcessingTransform processes batches efficiently with aggregations and calculations
4. DatabaseSink performs bulk database operations with the batched results

Key concepts demonstrated:
- BatchingNode configuration with size and time parameters
- Hybrid batching strategy (size OR time threshold triggers batch emission)
- Performance comparison between batched and individual processing
- Error handling in batched operations
- Testing batched pipelines with NPipeline.Extensions.Testing

This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions with configurable parameters
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic";
    }
}
