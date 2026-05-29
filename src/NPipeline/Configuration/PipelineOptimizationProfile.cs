namespace NPipeline.Configuration;

/// <summary>
///     Controls the optimization profile for pipeline configuration and analyzer behavior.
/// </summary>
/// <remarks>
///     <para>
///         <strong>Default:</strong> Safe, batteries-included defaults. Thread-safe context dictionaries,
///         automatic retry defaults, and relaxed build-time analyzer rules. Best for prototyping,
///         low-to-medium-throughput scenarios, and developer velocity.
///     </para>
///     <para>
///         <strong>HighThroughput:</strong> Zero-allocation, explicit-everything model. Users must
///         configure retry, materialization caps, and delay strategies explicitly. All build-time
///         performance analyzers are active. Best for production pipelines processing
///         millions of items per second.
///     </para>
/// </remarks>
public enum PipelineOptimizationProfile
{
    /// <summary>
    ///     Safe, batteries-included defaults. Thread-safe context dictionaries,
    ///     automatic retry defaults, and relaxed build-time analyzer rules.
    /// </summary>
    Default = 0,

    /// <summary>
    ///     Zero-allocation, explicit-everything model. Users must configure retry,
    ///     materialization caps, and delay strategies explicitly.
    /// </summary>
    HighThroughput = 1
}
