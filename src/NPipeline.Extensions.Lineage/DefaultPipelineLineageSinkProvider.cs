using Microsoft.Extensions.DependencyInjection;
using NPipeline.Pipeline;

namespace NPipeline.Lineage;

/// <summary>
///     Default provider for pipeline-level lineage reporting. Supplies the sink registered in the container
///     when there is one, and otherwise falls back to a <see cref="LoggingPipelineLineageSink" />.
/// </summary>
public sealed class DefaultPipelineLineageSinkProvider : IPipelineLineageSinkProvider
{
    private readonly IServiceProvider? _serviceProvider;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DefaultPipelineLineageSinkProvider" /> class that always
    ///     falls back to a <see cref="LoggingPipelineLineageSink" />.
    /// </summary>
    public DefaultPipelineLineageSinkProvider()
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DefaultPipelineLineageSinkProvider" /> class that prefers
    ///     the <see cref="IPipelineLineageSink" /> registered in the container.
    /// </summary>
    /// <param name="serviceProvider">The service provider to resolve the registered sink from.</param>
    public DefaultPipelineLineageSinkProvider(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
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

        // AddNPipelineLineage registers the caller's sink (or the logging default) as IPipelineLineageSink.
        // Prefer it: a sink supplied through DI is an explicit choice, and resolving it here is what makes
        // AddNPipelineLineage(sp => new MySink()) take effect without also configuring the builder.
        var registered = _serviceProvider?.GetService<IPipelineLineageSink>();

        if (registered is not null)
            return registered;

        // No container, or nothing registered: fall back to a logging sink. It uses NullLogger internally when
        // constructed without one, so this stays usable outside DI.
        return new LoggingPipelineLineageSink();
    }
}
