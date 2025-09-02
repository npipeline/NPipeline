using NPipeline.Pipeline;

namespace NPipeline.Graph;

/// <summary>
///     Controls how pipeline graph validation is applied during <see cref="PipelineBuilder.Build" />.
/// </summary>
public enum GraphValidationMode
{
    /// <summary>
    ///     Validation errors cause <see cref="PipelineBuilder.Build" /> to throw and block pipeline construction.
    /// </summary>
    Error = 0,

    /// <summary>
    ///     Validation still runs but any detected issues are surfaced as warnings; build always succeeds.
    /// </summary>
    Warn = 1,

    /// <summary>
    ///     Validation is skipped entirely (use cautiously, mainly for advanced scenarios / performance measurements).
    /// </summary>
    Off = 2,
}
