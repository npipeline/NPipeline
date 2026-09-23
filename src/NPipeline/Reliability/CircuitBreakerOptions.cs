namespace NPipeline.Reliability;

/// <summary>
///     What an item attempt does when its node's circuit breaker is open.
/// </summary>
/// <remarks>
///     <see cref="Fail" /> is the default. <see cref="Pause" /> is opt-in: set
///     <see cref="CircuitBreakerOptions.WhenOpen" /> to it.
/// </remarks>
public enum BreakerOpenBehavior
{
    /// <summary>
    ///     The default. The attempt is refused at once and the item fails with a <c>CircuitBreakerOpenException</c>.
    ///     The resilience policy sees the failure with <see cref="ItemFailure{TIn}.IsBreakerOpen" /> set, and the
    ///     default policy fails the node rather than skipping or dead-lettering every item while the dependency is down.
    /// </summary>
    Fail,

    /// <summary>
    ///     Opt-in. The attempt waits until the breaker lets a probe through, for at most
    ///     <see cref="CircuitBreakerOptions.MaxPause" />, and then fails as <see cref="Fail" /> does. While it waits, the
    ///     node reads no more input, so upstream nodes are held back by backpressure instead of items being lost.
    /// </summary>
    Pause,
}

/// <summary>
///     A node's circuit breaker. It guards every attempt of an item's transform (L1): once the node's dependency looks
///     unhealthy, the breaker opens and attempts stop being made until a probe succeeds.
/// </summary>
/// <remarks>
///     <para>
///         Only failures that the node's <see cref="ItemRetryOptions.Classifier" /> judges transient count against the
///         breaker. A permanent failure, such as a malformed record, says nothing about the dependency's health, and
///         neither does cancellation.
///     </para>
///     <para>
///         The breaker trips when any configured condition is met: <see cref="ConsecutiveFailures" /> transient
///         failures in a row, or a <see cref="FailureRate" /> over <see cref="Window" /> once at least
///         <see cref="MinimumCalls" /> attempts were seen. It then stays open for <see cref="OpenDuration" />, after
///         which up to <see cref="HalfOpenProbes" /> attempts are let through at a time. <see cref="ProbeSuccesses" />
///         successful probes close it; a transient probe failure opens it again.
///     </para>
///     <para>
///         A breaker belongs to one node of one pipeline definition and lives as long as the <c>PipelineFactory</c>
///         that built the pipeline, so its state carries over from one run to the next.
///     </para>
///     <code>
///     builder.WithResilience(handle, o => o with
///     {
///         CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 10, WhenOpen = BreakerOpenBehavior.Pause },
///     });
///     </code>
/// </remarks>
public sealed record CircuitBreakerOptions
{
    /// <summary>
    ///     Five consecutive transient failures trip the breaker, which stays open for 30 seconds and fails attempts
    ///     while open.
    /// </summary>
    public static CircuitBreakerOptions Default { get; } = new();

    /// <summary>
    ///     Trip after this many transient failures in a row. Null disables the condition. Default: 5.
    /// </summary>
    public int? ConsecutiveFailures { get; init; } = 5;

    /// <summary>
    ///     Trip when this fraction of the attempts in <see cref="Window" /> failed transiently, between 0 (exclusive) and
    ///     1 (inclusive). Null, the default, disables the condition.
    /// </summary>
    public double? FailureRate { get; init; }

    /// <summary>
    ///     The fewest attempts in <see cref="Window" /> before <see cref="FailureRate" /> is considered. Default: 20.
    /// </summary>
    public int MinimumCalls { get; init; } = 20;

    /// <summary>
    ///     The period <see cref="FailureRate" /> is measured over. Default: 30 seconds.
    /// </summary>
    public TimeSpan Window { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     How long the breaker stays open before it lets a probe through. Default: 30 seconds.
    /// </summary>
    public TimeSpan OpenDuration { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     The most probe attempts in flight at once while the breaker is half-open. Default: 1.
    /// </summary>
    public int HalfOpenProbes { get; init; } = 1;

    /// <summary>
    ///     The successful probes needed to close the breaker again. Default: 1.
    /// </summary>
    public int ProbeSuccesses { get; init; } = 1;

    /// <summary>
    ///     What an attempt does while the breaker is open. Default: <see cref="BreakerOpenBehavior.Fail" />.
    ///     <see cref="BreakerOpenBehavior.Pause" /> is opt-in; it waits for the breaker, up to <see cref="MaxPause" />.
    /// </summary>
    public BreakerOpenBehavior WhenOpen { get; init; } = BreakerOpenBehavior.Fail;

    /// <summary>
    ///     With <see cref="BreakerOpenBehavior.Pause" />, the longest one attempt waits for the breaker before it fails.
    ///     Ignored with <see cref="BreakerOpenBehavior.Fail" />. Default: 5 minutes.
    /// </summary>
    public TimeSpan MaxPause { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    ///     Throws when a value is out of range, or when no trip condition is configured.
    /// </summary>
    /// <returns>The same options, for chaining.</returns>
    public CircuitBreakerOptions Validate()
    {
        if (ConsecutiveFailures is null && FailureRate is null)
            throw new ArgumentException("A circuit breaker needs ConsecutiveFailures, FailureRate, or both; with neither it can never trip.");

        if (ConsecutiveFailures is { } consecutive)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(consecutive, nameof(ConsecutiveFailures));

        if (FailureRate is { } rate && rate is not (> 0.0 and <= 1.0))
            throw new ArgumentOutOfRangeException(nameof(FailureRate), rate, "FailureRate must be greater than 0 and at most 1.");

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MinimumCalls);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(Window, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(OpenDuration, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(HalfOpenProbes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ProbeSuccesses);

        if (!Enum.IsDefined(WhenOpen))
            throw new ArgumentOutOfRangeException(nameof(WhenOpen), WhenOpen, "Unknown breaker open behavior.");

        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(MaxPause, TimeSpan.Zero);
        return this;
    }
}
