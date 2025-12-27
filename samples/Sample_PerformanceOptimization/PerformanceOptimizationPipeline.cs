using NPipeline.Pipeline;
using Sample_PerformanceOptimization.Nodes;

namespace Sample_PerformanceOptimization;

/// <summary>
///     Performance optimization pipeline demonstrating ValueTask optimization, memory allocation reduction,
///     synchronous fast paths, and performance measurement.
///     This pipeline showcases different optimization approaches and benchmarks their effectiveness.
/// </summary>
/// <remarks>
///     This implementation demonstrates advanced performance optimization techniques:
///     1. ValueTask vs Task comparison for allocation reduction
///     2. Synchronous fast paths for simple operations
///     3. Memory optimization through pooling and span usage
///     4. Comprehensive performance measurement and benchmarking
///     The pipeline creates test data with varying complexity and processes it through different
///     optimization approaches to compare their effectiveness.
/// </remarks>
public class PerformanceOptimizationPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the performance optimization pipeline structure.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This pipeline creates multiple parallel processing paths to compare different optimization approaches:
    ///     1. Task-based processing (baseline)
    ///     2. ValueTask-based processing (allocation optimization)
    ///     3. Synchronous fast path processing (async overhead reduction)
    ///     4. Memory-optimized processing (allocation reduction)
    ///     All paths converge to a single measurement node for comprehensive analysis.
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that generates performance test data
        var source = builder.AddSource<PerformanceDataSource, PerformanceDataItem>("performance-data-source");

        // Create multiple optimization paths for comparison

        // Path 1: Task-based processing (baseline)
        var taskBasedTransform = builder.AddTransform<TaskBasedTransform, PerformanceDataItem, ProcessedPerformanceItem>("task-based-transform");

        // Path 2: ValueTask-based processing
        var valueTaskTransform =
            builder.AddTransform<ValueTaskComparisonTransform, PerformanceDataItem, ProcessedPerformanceItem>("valuetask-comparison-transform");

        // Path 3: Synchronous fast path processing
        var syncFastPathTransform =
            builder.AddTransform<SynchronousFastPathTransform, PerformanceDataItem, ProcessedPerformanceItem>("synchronous-fast-path-transform");

        // Path 4: Memory-optimized processing
        var memoryOptimizedTransform =
            builder.AddTransform<MemoryOptimizedTransform, PerformanceDataItem, ProcessedPerformanceItem>("memory-optimized-transform");

        // Add the measurement sink that collects performance data from all paths
        var measurementSink = builder.AddSink<PerformanceMeasurementNode, ProcessedPerformanceItem>("performance-measurement-sink");

        // Connect source to all transform paths (fan-out pattern)
        builder.Connect(source, taskBasedTransform);
        builder.Connect(source, valueTaskTransform);
        builder.Connect(source, syncFastPathTransform);
        builder.Connect(source, memoryOptimizedTransform);

        // Connect all transform paths to the measurement sink (fan-in pattern)
        builder.Connect(taskBasedTransform, measurementSink);
        builder.Connect(valueTaskTransform, measurementSink);
        builder.Connect(syncFastPathTransform, measurementSink);
        builder.Connect(memoryOptimizedTransform, measurementSink);
    }

    /// <summary>
    ///     Gets a description of what this performance optimization pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and optimization techniques.</returns>
    public static string GetDescription()
    {
        return @"Performance Optimization Pipeline Sample:

This sample demonstrates advanced performance optimization techniques in NPipeline:

Core Optimization Concepts:
1. ValueTask Optimization
   - Reduces allocations for operations that may complete synchronously
   - Compares ValueTask vs Task performance characteristics
   - Shows when ValueTask provides benefits

2. Synchronous Fast Paths
   - Avoids async overhead for simple operations
   - Implements conditional sync/async execution based on complexity
   - Demonstrates performance impact of async overhead

3. Memory Allocation Reduction
   - Uses ArrayPool<T> for buffer pooling
   - Implements span-based operations for zero allocations
   - Demonstrates stack allocation for small buffers
   - Shows caching strategies to avoid recomputation

4. Performance Measurement
   - Comprehensive benchmarking framework
   - Memory allocation tracking
   - Execution time measurement
   - Comparative analysis between approaches

Pipeline Architecture:
- Source generates test data with varying complexity levels
- Four parallel processing paths demonstrate different optimizations
- Measurement sink collects and analyzes performance data
- Generates comprehensive performance reports and recommendations

Expected Insights:
- Quantitative performance improvements from each optimization
- Memory allocation reduction percentages
- Best practices for different scenarios
- When to apply specific optimization techniques

This sample provides practical guidance for optimizing NPipeline applications
in production environments where performance and memory efficiency are critical.";
    }
}
