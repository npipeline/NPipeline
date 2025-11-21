using NPipeline.Pipeline;
using Sample_08_CustomNodeImplementation.Models;
using Sample_08_CustomNodeImplementation.Nodes;

namespace Sample_08_CustomNodeImplementation;

/// <summary>
///     Custom node implementation pipeline demonstrating advanced NPipeline concepts.
///     This pipeline implements a sophisticated sensor data processing flow:
///     1. SensorDataSource generates simulated sensor data
///     2. LifecycleMonitorNode tracks node lifecycle events
///     3. CachedTransform processes data with performance optimization through caching
///     4. BatchingSink outputs data in batches for improved performance
/// </summary>
/// <remarks>
///     This implementation demonstrates advanced custom node development patterns:
///     - Custom source node implementation with lifecycle management
///     - Advanced transform node with caching for performance optimization
///     - Custom sink node with batching capabilities
///     - Monitoring node that tracks lifecycle events
///     - Performance optimization through caching and batching
///     - Structured code for testability (without actual tests)
/// </remarks>
public class CustomNodePipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a sophisticated pipeline flow:
    ///     SensorDataSource -> LifecycleMonitorNode -> CachedTransform -> BatchingSink
    ///     The pipeline processes sensor data through these stages:
    ///     1. Source generates simulated sensor data with custom formatting
    ///     2. Monitor tracks lifecycle events for observability
    ///     3. Transform processes data with caching for performance optimization
    ///     4. Sink outputs data in batches for improved throughput
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that generates sensor data
        var source = builder.AddSource<SensorDataSource, SensorData>("sensor-data-source");

        // Add the monitoring node that tracks lifecycle events
        var monitor = builder.AddTransform<LifecycleMonitorNode, SensorData, SensorData>("lifecycle-monitor");

        // Add the transform node with caching capabilities
        var transform = builder.AddTransform<CachedTransform, SensorData, ProcessedSensorData>("cached-transform");

        // Add the sink node that batches output for performance
        var sink = builder.AddSink<BatchingSink, ProcessedSensorData>("batching-sink");

        // Connect the nodes in a linear flow: source -> monitor -> transform -> sink
        builder.Connect(source, monitor);
        builder.Connect(monitor, transform);
        builder.Connect(transform, sink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"Custom Node Implementation Sample:

This sample demonstrates advanced custom node development patterns in NPipeline:
- Custom source node implementation with lifecycle management
- Advanced transform node with caching for performance optimization
- Custom sink node with batching capabilities
- Monitoring node that tracks lifecycle events
- Performance optimization through caching and batching
- Structured code for testability (without actual tests)

The pipeline flow:
1. SensorDataSource generates simulated sensor data with custom formatting
2. LifecycleMonitorNode tracks node lifecycle events for observability
3. CachedTransform processes data with caching for performance optimization
4. BatchingSink outputs data in batches for improved throughput

Key concepts demonstrated:
- How to implement custom source nodes by inheriting from SourceNode<T>
- How to implement custom transform nodes with caching for performance optimization
- How to implement custom sink nodes with batching capabilities
- How to manage node lifecycle events (initialization, execution, disposal)
- How to optimize performance through caching and batching
- How to structure code for testability (even though we won't add actual tests)

This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic";
    }
}
