using NPipeline.Pipeline;

namespace NPipeline.Lineage;

/// <summary>
///     Default provider that creates a <see cref="LoggingPipelineLineageSink" /> for pipeline-level lineage reporting.
/// </summary>
public sealed class DefaultPipelineLineageSinkProvider : IPipelineLineageSinkProvider
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="DefaultPipelineLineageSinkProvider" /> class.
    /// </summary>
    public DefaultPipelineLineageSinkProvider()
    {
    }

    /// <summary>
    ///     Creates a pipeline lineage sink instance using the provided pipeline run context.
    /// </summary>
    /// <param name="context">The current pipeline context for this run.</param>
    /// <returns>An instance of <see cref="IPipelineLineageSink" /> or null.</returns>
    public IPipelineLineageSink? Create(PipelineContext context)
    {
        if (context == null)
            return null;

        // Create a logging sink without a logger
        // The sink will use NullLogger internally if no logger is provided
        // Users can configure a custom sink with proper logging via DI or builder configuration
        return new LoggingPipelineLineageSink();
    }
}
