namespace NPipeline.Configuration
{
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
        ///     Retry options applied by explicit retry shorthand APIs.
        /// </summary>
        PipelineRetryOptions RetryDefaults { get; }

        /// <summary>
        ///     Retry options that should be applied automatically when retry has not been explicitly configured.
        ///     Null means no automatic retry defaults should be applied.
        /// </summary>
        PipelineRetryOptions? AutomaticRetryDefaults { get; }

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

        public static IOptimizationProfileBehavior For(PipelineOptimizationProfile profile)
        {
            return Behaviors.TryGetValue(profile, out var behavior)
                ? behavior
                : throw new ArgumentOutOfRangeException(nameof(profile), profile, "Unsupported pipeline optimization profile.");
        }

        private sealed class DefaultOptimizationProfileBehavior : IOptimizationProfileBehavior
        {
            public PipelineOptimizationProfile Profile => PipelineOptimizationProfile.Default;

            public PipelineRetryOptions RetryDefaults => PipelineRetryOptions.ForProfile(PipelineOptimizationProfile.Default);

            public PipelineRetryOptions? AutomaticRetryDefaults => RetryDefaults;

            public bool UsesThreadSafeContextDictionaries => true;
        }

        private sealed class HighThroughputOptimizationProfileBehavior : IOptimizationProfileBehavior
        {
            public PipelineOptimizationProfile Profile => PipelineOptimizationProfile.HighThroughput;

            public PipelineRetryOptions RetryDefaults => PipelineRetryOptions.ForProfile(PipelineOptimizationProfile.HighThroughput);

            public PipelineRetryOptions? AutomaticRetryDefaults => null;

            public bool UsesThreadSafeContextDictionaries => false;
        }
    }
}