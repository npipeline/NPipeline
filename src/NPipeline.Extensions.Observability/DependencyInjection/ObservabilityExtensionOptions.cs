namespace NPipeline.Observability.DependencyInjection;

/// <summary>
///     Configuration options for the NPipeline observability extension.
/// </summary>
/// <remarks>
///     Use these options to control automatic metrics collection behavior,
///     such as whether to enable memory sampling which adds overhead.
/// </remarks>
public sealed record ObservabilityExtensionOptions
{
    /// <summary>
    ///     Gets or sets whether to automatically collect memory metrics (peak memory usage) for each node.
    /// </summary>
    /// <remarks>
    ///     When enabled, the framework samples memory usage via GC.GetTotalMemory(false) at node start and end.
    ///     This adds a small but measurable overhead and is disabled by default.
    ///     Memory metrics are only recorded if nodes also have the RecordMemoryUsage option enabled.
    /// </remarks>
    public bool EnableMemoryMetrics { get; init; }

    /// <summary>
    ///     Gets the default observability extension options with memory metrics disabled.
    /// </summary>
    public static ObservabilityExtensionOptions Default => new() { EnableMemoryMetrics = false };

    /// <summary>
    ///     Gets observability extension options with memory metrics enabled.
    /// </summary>
    public static ObservabilityExtensionOptions WithMemoryMetrics => new() { EnableMemoryMetrics = true };
}
