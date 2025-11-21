using NPipeline.Extensions.Parallelism;
using NPipeline.Pipeline;
using Sample_05_ParallelProcessing.Nodes;

namespace Sample_05_ParallelProcessing;

/// <summary>
///     Parallel processing pipeline definition demonstrating NPipeline parallel execution capabilities.
///     This pipeline implements a CPU-intensive processing flow with configurable parallelism:
///     1. CpuIntensiveDataSource generates CPU-intensive work items
///     2. ParallelCpuTransform processes items in parallel with configurable degree of parallelism
///     3. PerformanceMonitoringTransform tracks execution metrics
///     4. ConsoleSinkWithMetrics outputs results and performance statistics
/// </summary>
/// <remarks>
///     This implementation demonstrates advanced NPipeline capabilities:
///     - Parallel execution strategies with different queue policies
///     - Configurable degree of parallelism
///     - Resource contention handling
///     - Performance metrics collection and reporting
///     - Thread safety considerations
/// </remarks>
public class ParallelProcessingPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes with parallel processing configuration.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a parallel processing pipeline flow:
    ///     CpuIntensiveDataSource -> ParallelCpuTransform -> PerformanceMonitoringTransform -> ConsoleSinkWithMetrics
    ///     The pipeline demonstrates:
    ///     1. Generation of CPU-intensive work items with varying complexity
    ///     2. Parallel processing with configurable degree of parallelism and queue policies
    ///     3. Performance monitoring and metrics collection
    ///     4. Thread-safe result aggregation and reporting
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that generates CPU-intensive work items
        var source = builder.AddSource<CpuIntensiveDataSource, CpuIntensiveWorkItem>("cpu-intensive-source");

        // Add the parallel transform node that performs CPU-bound operations
        var parallelTransform = builder.AddTransform<ParallelCpuTransform, CpuIntensiveWorkItem, ProcessedWorkItem>("parallel-cpu-transform")
            .WithBlockingParallelism(builder, Environment.ProcessorCount, 50);

        // Add the performance monitoring transform that tracks execution metrics
        var monitoringTransform = builder.AddTransform<PerformanceMonitoringTransform, ProcessedWorkItem, ProcessedWorkItem>("performance-monitoring");

        // Add the console sink node that outputs results and performance statistics
        var sink = builder.AddSink<ConsoleSinkWithMetrics, ProcessedWorkItem>("console-sink-with-metrics");

        // Connect the nodes in a linear flow: source -> parallelTransform -> monitoringTransform -> sink
        builder.Connect(source, parallelTransform);
        builder.Connect(parallelTransform, monitoringTransform);
        builder.Connect(monitoringTransform, sink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and parallel processing capabilities.</returns>
    public static string GetDescription()
    {
        return @"Parallel Processing Pipeline Sample:

This sample demonstrates advanced NPipeline parallel processing capabilities:
- CPU-bound parallel transforms with configurable degree of parallelism
- Resource management and contention handling
- Thread safety considerations in concurrent execution
- Performance metrics collection and analysis
- Different parallel execution strategies (Blocking, DropOldest, DropNewest)

The pipeline flow:
1. CpuIntensiveDataSource generates CPU-intensive work items with varying complexity
2. ParallelCpuTransform processes items in parallel using multiple threads
3. PerformanceMonitoringTransform tracks execution metrics and timing
4. ConsoleSinkWithMetrics outputs results and performance statistics

Key concepts demonstrated:
- Configurable parallel execution strategies using NPipeline.Extensions.Parallelism
- Degree of parallelism configuration for optimal resource utilization
- Resource contention handling with bounded queues and different policies
- Performance metrics collection for monitoring and optimization
- Thread-safe processing and result aggregation

Parallel execution strategies:
- Blocking: Preserves ordering and applies backpressure (default)
- DropOldest: Discards oldest items when queue is full (high throughput)
- DropNewest: Discards newest items when queue is full (preserve historical data)

This implementation follows best practices for:
- Efficient CPU utilization in parallel scenarios
- Proper resource management and cleanup
- Thread-safe operations and data sharing
- Performance monitoring and optimization";
    }
}
