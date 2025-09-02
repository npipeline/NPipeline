using NPipeline.Pipeline;

namespace NPipeline.Lineage;

/// <summary>
///     Provides a default pipeline-level lineage sink when none has been explicitly configured.
///     Implementations can be supplied by optional packages (e.g., NPipeline.Lineage) via DI.
/// </summary>
public interface IPipelineLineageSinkProvider
{
    /// <summary>
    ///     Creates a pipeline lineage sink instance using the provided pipeline run context,
    ///     or returns null if the provider cannot supply one in the current environment.
    /// </summary>
    /// <param name="context">The current pipeline context for this run.</param>
    /// <returns>An instance of <see cref="IPipelineLineageSink" /> or null.</returns>
    IPipelineLineageSink? Create(PipelineContext context);
}
