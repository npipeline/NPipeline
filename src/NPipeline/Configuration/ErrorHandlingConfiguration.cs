using System.Collections.Immutable;
using NPipeline.ErrorHandling;
using NPipeline.Reliability;

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
    ///     The pipeline's resilience options, with the optimization profile's defaults already applied.
    /// </summary>
    public PipelineResilienceOptions? Resilience { get; init; }

    /// <summary>
    ///     Per-node resilience options, each derived from <see cref="Resilience" />, keyed by node id.
    /// </summary>
    public ImmutableDictionary<string, PipelineResilienceOptions>? NodeResilience { get; init; }

    /// <summary>
    ///     Creates a new ErrorHandlingConfiguration with default values.
    /// </summary>
    public static ErrorHandlingConfiguration Default => new();
}
