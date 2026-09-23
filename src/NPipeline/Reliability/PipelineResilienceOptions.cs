using NPipeline.Configuration;

namespace NPipeline.Reliability;

/// <summary>
///     What happens to an item whose failure is not retried.
/// </summary>
public enum ItemFailureAction
{
    /// <summary>
    ///     The node fails, and with it the pipeline.
    /// </summary>
    Fail,

    /// <summary>
    ///     The item is dropped and the node continues.
    /// </summary>
    Skip,

    /// <summary>
    ///     The item is sent to the pipeline's dead-letter sink and the node continues. Running a pipeline that
    ///     dead-letters without a dead-letter sink fails before any node runs.
    /// </summary>
    DeadLetter,
}

/// <summary>
///     Resilience configuration for a pipeline, or for one node when derived from the pipeline's options with
///     <c>with</c>. It covers the three layers that can repeat work, plus what happens to an item that is not retried.
/// </summary>
/// <remarks>
///     <list type="table">
///         <listheader>
///             <term>Layer</term>
///             <description>What it repeats</description>
///         </listheader>
///         <item>
///             <term><see cref="ItemRetry" /> (L1)</term>
///             <description>One item's transform, in transform nodes.</description>
///         </item>
///         <item>
///             <term><see cref="NodeRestart" /> (L2)</term>
///             <description>A transform node's output stream, for nodes wrapped for restart.</description>
///         </item>
///         <item>
///             <term><see cref="NodeRetry" /> (L3)</term>
///             <description>A whole node's execution, in any node.</description>
///         </item>
///     </list>
///     <para>
///         The options describe the defaults. A registered <see cref="IResiliencePolicy" /> makes each decision and
///         can depart from them; <see cref="DefaultResiliencePolicy" /> follows them exactly.
///     </para>
///     <code>
///     builder.WithResilience(o => o with
///     {
///         ItemRetry = ItemRetryOptions.Default with { MaxRetries = 5 },
///         OnItemFailure = ItemFailureAction.DeadLetter,
///     });
///     </code>
/// </remarks>
public sealed record PipelineResilienceOptions
{
    /// <summary>
    ///     Nothing is retried, restarted, or skipped. This is the <see cref="PipelineOptimizationProfile.HighThroughput" />
    ///     profile's configuration.
    /// </summary>
    public static PipelineResilienceOptions None { get; } = new();

    /// <summary>
    ///     Item retry (L1): how many times one item's transform is retried, and how long to wait between attempts.
    /// </summary>
    public ItemRetryOptions ItemRetry { get; init; } = ItemRetryOptions.None;

    /// <summary>
    ///     Node restart (L2): how many times a failed transform stream is restarted.
    /// </summary>
    public NodeRestartOptions NodeRestart { get; init; } = NodeRestartOptions.None;

    /// <summary>
    ///     Node retry (L3): how many times a failed node is executed again.
    /// </summary>
    public NodeRetryOptions NodeRetry { get; init; } = NodeRetryOptions.None;

    /// <summary>
    ///     The circuit breaker guarding each item attempt in a transform node. Null, the default, means no breaker.
    /// </summary>
    /// <remarks>
    ///     An open breaker fails the attempt by default. Waiting for the breaker instead is opt-in, through
    ///     <see cref="CircuitBreakerOptions.WhenOpen" />.
    /// </remarks>
    public CircuitBreakerOptions? CircuitBreaker { get; init; }

    /// <summary>
    ///     What happens to an item whose failure is not retried. Default: <see cref="ItemFailureAction.Fail" />.
    /// </summary>
    public ItemFailureAction OnItemFailure { get; init; } = ItemFailureAction.Fail;

    /// <summary>
    ///     The clock every retry delay waits on. Tests substitute a fake to avoid real sleeps.
    /// </summary>
    public TimeProvider Time { get; init; } = TimeProvider.System;

    /// <summary>
    ///     The options an optimization profile starts from.
    /// </summary>
    /// <param name="profile">The profile.</param>
    /// <returns>
    ///     For <see cref="PipelineOptimizationProfile.Default" />, transient item failures are retried three times
    ///     (<see cref="ItemRetryOptions.Default" />). For <see cref="PipelineOptimizationProfile.HighThroughput" />,
    ///     <see cref="None" />.
    /// </returns>
    public static PipelineResilienceOptions ForProfile(PipelineOptimizationProfile profile)
    {
        return profile switch
        {
            PipelineOptimizationProfile.Default => DefaultProfile,
            PipelineOptimizationProfile.HighThroughput => None,
            _ => throw new ArgumentOutOfRangeException(nameof(profile), profile, "Unsupported pipeline optimization profile."),
        };
    }

    private static PipelineResilienceOptions DefaultProfile { get; } = new() { ItemRetry = ItemRetryOptions.Default };

    /// <summary>
    ///     Throws when a value is out of range.
    /// </summary>
    /// <returns>The same options, for chaining.</returns>
    public PipelineResilienceOptions Validate()
    {
        ArgumentNullException.ThrowIfNull(ItemRetry);
        ArgumentNullException.ThrowIfNull(NodeRestart);
        ArgumentNullException.ThrowIfNull(NodeRetry);
        ArgumentNullException.ThrowIfNull(Time);

        if (!Enum.IsDefined(OnItemFailure))
            throw new ArgumentOutOfRangeException(nameof(OnItemFailure), OnItemFailure, "Unknown item failure action.");

        ItemRetry.Validate();
        NodeRestart.Validate();
        NodeRetry.Validate();
        _ = CircuitBreaker?.Validate();
        return this;
    }
}

/// <summary>
///     Item retry (L1): retries one item's transform in a transform node.
/// </summary>
public sealed record ItemRetryOptions
{
    /// <summary>
    ///     No item is retried.
    /// </summary>
    public static ItemRetryOptions None { get; } = new() { MaxRetries = 0 };

    /// <summary>
    ///     Three retries of transient failures, with exponential backoff from 200 ms up to 30 s and full jitter.
    ///     The <see cref="PipelineOptimizationProfile.Default" /> profile's configuration.
    /// </summary>
    public static ItemRetryOptions Default { get; } = new()
    {
        MaxRetries = 3,
        Backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(200), maxDelay: TimeSpan.FromSeconds(30)),
    };

    /// <summary>
    ///     Retries after the first attempt. An item is attempted at most <c>MaxRetries + 1</c> times.
    /// </summary>
    public int MaxRetries { get; init; }

    /// <summary>
    ///     The wait before each retry. Default: none.
    /// </summary>
    public RetryBackoff Backoff { get; init; } = RetryBackoff.None;

    /// <summary>
    ///     Which failures are worth retrying. Default: <see cref="RetryClassifier.Default" />, transient failures only.
    /// </summary>
    public RetryClassifier Classifier { get; init; } = RetryClassifier.Default;

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegative(MaxRetries);
        ArgumentNullException.ThrowIfNull(Classifier);
        Backoff.Validate();
    }
}

/// <summary>
///     Node restart (L2): restarts a transform node's output stream after it fails.
/// </summary>
public sealed record NodeRestartOptions
{
    /// <summary>
    ///     A failed stream is never restarted.
    /// </summary>
    public static NodeRestartOptions None { get; } = new() { MaxRestarts = 0 };

    /// <summary>
    ///     Restarts after the first run. A stream runs at most <c>MaxRestarts + 1</c> times.
    /// </summary>
    public int MaxRestarts { get; init; }

    /// <summary>
    ///     The wait before each restart. Default: exponential from 1 s up to 30 s, with full jitter.
    /// </summary>
    public RetryBackoff Backoff { get; init; } =
        RetryBackoff.Exponential(TimeSpan.FromSeconds(1), maxDelay: TimeSpan.FromSeconds(30));

    /// <summary>
    ///     The most input items held so that a restart can process them again. Default: 10,000.
    /// </summary>
    /// <remarks>
    ///     A restart resumes at the node's checkpoint, the first input item whose outcome has not been delivered, so
    ///     only the items between the checkpoint and the last item read are held. When this many are held, the node
    ///     stops reading its input until the checkpoint advances. It is a backpressure bound, never an error, and it
    ///     does not limit the length of the input.
    /// </remarks>
    public int MaxReplayWindow { get; init; } = 10_000;

    /// <summary>
    ///     When set, the restart count starts again after this many outputs are delivered following a restart, so a
    ///     long-running stream with rare faults does not use up its restarts over hours. Default: never.
    /// </summary>
    public int? ResetAfterItems { get; init; }

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegative(MaxRestarts);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxReplayWindow);

        if (ResetAfterItems is { } resetAfter)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(resetAfter, nameof(ResetAfterItems));

        Backoff.Validate();
    }
}

/// <summary>
///     Node retry (L3): executes a failed node again.
/// </summary>
public sealed record NodeRetryOptions
{
    /// <summary>
    ///     A failed node is never executed again.
    /// </summary>
    public static NodeRetryOptions None { get; } = new() { MaxRetries = 0 };

    /// <summary>
    ///     Retries after the first execution. A node executes at most <c>MaxRetries + 1</c> times.
    /// </summary>
    public int MaxRetries { get; init; }

    /// <summary>
    ///     The wait before each retry. Default: exponential from 1 s up to 30 s, with full jitter.
    /// </summary>
    public RetryBackoff Backoff { get; init; } =
        RetryBackoff.Exponential(TimeSpan.FromSeconds(1), maxDelay: TimeSpan.FromSeconds(30));

    /// <summary>
    ///     Which failures are worth retrying. Default: <see cref="RetryClassifier.Default" />, transient failures only.
    /// </summary>
    public RetryClassifier Classifier { get; init; } = RetryClassifier.Default;

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegative(MaxRetries);
        ArgumentNullException.ThrowIfNull(Classifier);
        Backoff.Validate();
    }
}
