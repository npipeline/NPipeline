namespace NPipeline.Observability.Metrics;

/// <summary>
///     Represents performance and throughput metrics for an entire pipeline execution.
/// </summary>
/// <remarks>
///     <para>
///         Pipeline metrics provide high-level insights into overall pipeline performance:
///         - Total execution time
///         - Success/failure status
///         - Total items processed
///         - Per-node metrics for granular analysis
///     </para>
///     <para>
///         Use this interface to monitor pipeline health, identify bottlenecks, and track
///         historical performance trends.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Access metrics after pipeline execution
/// var context = new PipelineContextBuilder().Build();
/// await runner.RunAsync&lt;MyPipeline&gt;(context);
/// 
/// if (context.Metrics is IPipelineMetrics metrics)
/// {
///     Console.WriteLine($"Pipeline: {metrics.PipelineName}");
///     Console.WriteLine($"Items processed: {metrics.TotalItemsProcessed}");
///     Console.WriteLine($"Duration: {metrics.DurationMs}ms");
///     Console.WriteLine($"Success: {metrics.Success}");
/// 
///     foreach (var nodeMetric in metrics.NodeMetrics)
///     {
///         Console.WriteLine($"  {nodeMetric.NodeName}: {nodeMetric.ItemsProcessed} items");
///     }
/// }
/// </code>
/// </example>
public interface IPipelineMetrics
{
    /// <summary>
    ///     The name of the pipeline.
    /// </summary>
    string PipelineName { get; }

    /// <summary>
    ///     The unique identifier for this pipeline run.
    /// </summary>
    Guid RunId { get; }

    /// <summary>
    ///     The timestamp when the pipeline execution started.
    /// </summary>
    DateTimeOffset StartTime { get; }

    /// <summary>
    ///     The timestamp when the pipeline execution completed.
    /// </summary>
    DateTimeOffset? EndTime { get; }

    /// <summary>
    ///     The total duration of the pipeline execution in milliseconds.
    /// </summary>
    long? DurationMs { get; }

    /// <summary>
    ///     Whether the pipeline execution was successful.
    /// </summary>
    bool Success { get; }

    /// <summary>
    ///     The total number of items processed by all nodes in the pipeline.
    /// </summary>
    long TotalItemsProcessed { get; }

    /// <summary>
    ///     Metrics for individual nodes in the pipeline.
    /// </summary>
    /// <remarks>
    ///     Each entry in this list corresponds to a node in the pipeline definition.
    ///     Use per-node metrics to identify which nodes are bottlenecks or experiencing errors.
    /// </remarks>
    IReadOnlyList<INodeMetrics> NodeMetrics { get; }

    /// <summary>
    ///     Any exception that occurred during execution.
    /// </summary>
    /// <remarks>
    ///     If <c>Success</c> is <c>false</c>, this will contain the exception that caused the failure.
    /// </remarks>
    Exception? Exception { get; }
}
