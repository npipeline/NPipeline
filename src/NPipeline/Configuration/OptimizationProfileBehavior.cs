using NPipeline.Reliability;

namespace NPipeline.Configuration;

/// <summary>
///     Encapsulates runtime behavior knobs associated with a <see cref="PipelineOptimizationProfile" />.
/// </summary>
internal interface IOptimizationProfileBehavior
{
    /// <summary>
    ///     The profile represented by this behavior implementation.
    /// </summary>
    PipelineOptimizationProfile Profile { get; }

    /// <summary>
    ///     The resilience options a pipeline built with this profile starts from.
    /// </summary>
    PipelineResilienceOptions ResilienceDefaults { get; }

    /// <summary>
    ///     True when context dictionaries should use <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey, TValue}" />.
    /// </summary>
    bool UsesThreadSafeContextDictionaries { get; }
}

/// <summary>
///     Registry for resolving behavior implementations by optimization profile.
/// </summary>
internal static class OptimizationProfileBehaviorRegistry
{
    private static readonly IReadOnlyDictionary<PipelineOptimizationProfile, IOptimizationProfileBehavior> Behaviors =
        new Dictionary<PipelineOptimizationProfile, IOptimizationProfileBehavior>
        {
            [PipelineOptimizationProfile.Default] = new DefaultOptimizationProfileBehavior(),
            [PipelineOptimizationProfile.HighThroughput] = new HighThroughputOptimizationProfileBehavior(),
        };

    public static IOptimizationProfileBehavior For(PipelineOptimizationProfile profile) =>
        Behaviors.TryGetValue(profile, out var behavior)
            ? behavior
            : throw new ArgumentOutOfRangeException(nameof(profile), profile, "Unsupported pipeline optimization profile.");

    private sealed class DefaultOptimizationProfileBehavior : IOptimizationProfileBehavior
    {
        public PipelineOptimizationProfile Profile => PipelineOptimizationProfile.Default;

        public PipelineResilienceOptions ResilienceDefaults => PipelineResilienceOptions.ForProfile(PipelineOptimizationProfile.Default);

        public bool UsesThreadSafeContextDictionaries => true;
    }

    private sealed class HighThroughputOptimizationProfileBehavior : IOptimizationProfileBehavior
    {
        public PipelineOptimizationProfile Profile => PipelineOptimizationProfile.HighThroughput;

        public PipelineResilienceOptions ResilienceDefaults => PipelineResilienceOptions.ForProfile(PipelineOptimizationProfile.HighThroughput);

        public bool UsesThreadSafeContextDictionaries => false;
    }
}
