using System.Collections.Concurrent;
using NPipeline.Reliability;

namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     The circuit breakers of one pipeline definition, one per node.
/// </summary>
/// <remarks>
///     <para>
///         <see cref="NPipeline.Pipeline.PipelineFactory" /> keeps one registry per definition type for as long as it lives, and
///         <c>PipelineFactory</c> is a DI singleton, so a breaker's state carries over from one run to the next. A
///         pipeline triggered every few minutes against a dead dependency therefore fails fast instead of starting each
///         run with a closed breaker.
///     </para>
///     <para>
///         The key set is the definition's node ids, which is bounded by code rather than data, so nothing is ever
///         evicted. A node whose breaker options or clock change gets a new breaker.
///     </para>
/// </remarks>
internal sealed class CircuitBreakerRegistry
{
    private readonly ConcurrentDictionary<string, CircuitBreaker> _breakers = new(StringComparer.Ordinal);

    /// <summary>
    ///     The node's breaker, or null when its options configure none.
    /// </summary>
    public CircuitBreaker? Resolve(string nodeId, PipelineResilienceOptions options)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(options);

        if (options.CircuitBreaker is not { } breakerOptions)
            return null;

        var time = options.Time;

        if (_breakers.TryGetValue(nodeId, out var existing) && Matches(existing, breakerOptions, time))
            return existing;

        return _breakers.AddOrUpdate(
            nodeId,
            static (id, state) => new CircuitBreaker(id, state.Options, state.Time),
            static (id, current, state) => Matches(current, state.Options, state.Time)
                ? current
                : new CircuitBreaker(id, state.Options, state.Time),
            (Options: breakerOptions, Time: time));
    }

    private static bool Matches(CircuitBreaker breaker, CircuitBreakerOptions options, TimeProvider time) =>
        breaker.Options == options && ReferenceEquals(breaker.Time, time);
}
