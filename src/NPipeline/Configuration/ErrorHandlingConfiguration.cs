using System.Collections.Immutable;
using NPipeline.ErrorHandling;
using NPipeline.Resilience;

namespace NPipeline.Configuration;

/// <summary>
///     Configuration for error handling settings.
/// </summary>
public sealed record ErrorHandlingConfiguration
{
    /// <summary>
    ///     The optional unified resilience policy instance.
    /// </summary>
    public IResiliencePolicy? ResiliencePolicy { get; init; }

    /// <summary>
    ///     The optional unified resilience policy type.
    /// </summary>
    public Type? ResiliencePolicyType { get; init; }

    /// <summary>
    ///     The optional sink for failed items.
    /// </summary>
    public IDeadLetterSink? DeadLetterSink { get; init; }

    /// <summary>
    ///     The type of the dead letter sink.
    /// </summary>
    public Type? DeadLetterSinkType { get; init; }

    /// <summary>
    ///     The retry options.
    /// </summary>
    public PipelineRetryOptions? RetryOptions { get; init; }

    /// <summary>
    ///     The node retry overrides.
    /// </summary>
    public ImmutableDictionary<string, PipelineRetryOptions>? NodeRetryOverrides { get; init; }

    /// <summary>
    ///     The circuit breaker options.
    /// </summary>
    public PipelineCircuitBreakerOptions? CircuitBreakerOptions { get; init; }

    /// <summary>
    ///     The circuit breaker memory management options.
    /// </summary>
    public CircuitBreakerMemoryManagementOptions? CircuitBreakerMemoryOptions { get; init; }

    /// <summary>
    ///     Creates a new ErrorHandlingConfiguration with default values.
    /// </summary>
    public static ErrorHandlingConfiguration Default => new();
}
