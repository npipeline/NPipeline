using System.Collections.Immutable;
using NPipeline.Configuration;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Internal helper for constructing ErrorHandlingConfiguration from builder state.
/// </summary>
internal static class ErrorHandlingConfigurationBuilder
{
    /// <summary>
    ///     Builds an ErrorHandlingConfiguration from the current builder state.
    /// </summary>
    /// <param name="configState">The builder configuration state containing error handling properties.</param>
    /// <param name="retryOptions">The retry options.</param>
    /// <param name="nodeRetryOverrides">The node-level retry overrides.</param>
    /// <param name="circuitBreakerOptions">The circuit breaker options.</param>
    /// <param name="circuitBreakerMemoryOptions">The circuit breaker memory management options.</param>
    /// <returns>A new ErrorHandlingConfiguration with all properties set.</returns>
    public static ErrorHandlingConfiguration Build(
        BuilderConfigurationState configState,
        PipelineRetryOptions? retryOptions,
        IReadOnlyDictionary<string, PipelineRetryOptions> nodeRetryOverrides,
        PipelineCircuitBreakerOptions? circuitBreakerOptions,
        CircuitBreakerMemoryManagementOptions? circuitBreakerMemoryOptions)
    {
        ArgumentNullException.ThrowIfNull(configState);
        ArgumentNullException.ThrowIfNull(nodeRetryOverrides);

        var overrideDict = nodeRetryOverrides.Count > 0
            ? nodeRetryOverrides.ToImmutableDictionary()
            : null;

        return new ErrorHandlingConfiguration
        {
            PipelineErrorHandler = configState.PipelineErrorHandler,
            DeadLetterSink = configState.DeadLetterSink,
            PipelineErrorHandlerType = configState.PipelineErrorHandlerType,
            DeadLetterSinkType = configState.DeadLetterSinkType,
            RetryOptions = retryOptions,
            NodeRetryOverrides = overrideDict,
            CircuitBreakerOptions = circuitBreakerOptions,
            CircuitBreakerMemoryOptions = circuitBreakerMemoryOptions,
        };
    }
}
