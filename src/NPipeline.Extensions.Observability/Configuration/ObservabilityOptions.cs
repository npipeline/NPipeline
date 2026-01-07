namespace NPipeline.Observability.Configuration;

/// <summary>
///     Configuration options for automatic observability metrics collection on a node.
/// </summary>
/// <remarks>
///     <para>
///         Use these options with the <c>WithObservability</c> extension method to configure
///         automatic metrics collection for specific nodes in your pipeline.
///     </para>
///     <para>
///         When observability is enabled, the framework automatically records metrics
///         without requiring manual instrumentation in your node implementations.
///     </para>
/// </remarks>
/// <example>
///     <code>
///     // Enable observability with default options
///     builder.AddTransform&lt;MyNode, int, string&gt;()
///         .WithObservability(builder);
///     
///     // Enable full observability including memory tracking
///     builder.AddTransform&lt;MyNode, int, string&gt;()
///         .WithObservability(builder, ObservabilityOptions.Full);
///     
///     // Custom configuration
///     builder.AddTransform&lt;MyNode, int, string&gt;()
///         .WithObservability(builder, new ObservabilityOptions
///         {
///             RecordTiming = true,
///             RecordItemCounts = true,
///             RecordMemoryUsage = false
///         });
///     </code>
/// </example>
public sealed record ObservabilityOptions
{
    /// <summary>
    ///     Gets or sets whether to record node execution timing (start time, end time, duration).
    /// </summary>
    /// <remarks>
    ///     When enabled, automatically records when the node starts and completes execution,
    ///     calculating the total duration in milliseconds.
    /// </remarks>
    public bool RecordTiming { get; init; } = true;

    /// <summary>
    ///     Gets or sets whether to record item counts (items processed and items emitted).
    /// </summary>
    /// <remarks>
    ///     When enabled, tracks the number of items that flow through the node.
    ///     For transform nodes, this includes both input items processed and output items emitted.
    /// </remarks>
    public bool RecordItemCounts { get; init; } = true;

    /// <summary>
    ///     Gets or sets whether to record memory usage statistics.
    /// </summary>
    /// <remarks>
    ///     When enabled, captures initial and peak memory usage during node execution.
    ///     This adds some overhead and should be disabled for high-throughput scenarios
    ///     unless memory profiling is specifically needed.
    /// </remarks>
    public bool RecordMemoryUsage { get; init; }

    /// <summary>
    ///     Gets or sets whether to record thread information.
    /// </summary>
    /// <remarks>
    ///     When enabled, records which thread executed the node. Useful for debugging
    ///     parallelism and understanding thread affinity in your pipeline.
    /// </remarks>
    public bool RecordThreadInfo { get; init; } = true;

    /// <summary>
    ///     Gets or sets whether to calculate and record performance metrics.
    /// </summary>
    /// <remarks>
    ///     When enabled, calculates throughput (items per second) and average processing
    ///     time per item based on the recorded timing and item counts.
    /// </remarks>
    public bool RecordPerformanceMetrics { get; init; } = true;

    /// <summary>
    ///     Gets the default observability options with timing, item counts, thread info, and performance metrics enabled.
    /// </summary>
    public static ObservabilityOptions Default => new();

    /// <summary>
    ///     Gets full observability options with all metrics enabled, including memory usage.
    /// </summary>
    public static ObservabilityOptions Full => new()
    {
        RecordTiming = true,
        RecordItemCounts = true,
        RecordMemoryUsage = true,
        RecordThreadInfo = true,
        RecordPerformanceMetrics = true
    };

    /// <summary>
    ///     Gets minimal observability options with only timing enabled.
    /// </summary>
    public static ObservabilityOptions Minimal => new()
    {
        RecordTiming = true,
        RecordItemCounts = false,
        RecordMemoryUsage = false,
        RecordThreadInfo = false,
        RecordPerformanceMetrics = false
    };

    /// <summary>
    ///     Gets disabled observability options (no metrics recorded).
    /// </summary>
    public static ObservabilityOptions Disabled => new()
    {
        RecordTiming = false,
        RecordItemCounts = false,
        RecordMemoryUsage = false,
        RecordThreadInfo = false,
        RecordPerformanceMetrics = false
    };
}
