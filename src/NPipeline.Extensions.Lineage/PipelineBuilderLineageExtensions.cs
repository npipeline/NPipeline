using System.Text.Json;
using NPipeline.Pipeline;

namespace NPipeline.Lineage;

/// <summary>
///     Provides extension methods for configuring lineage on <see cref="PipelineBuilder" />.
/// </summary>
public static class PipelineBuilderLineageExtensions
{
    /// <summary>
    ///     Configures the pipeline to use a <see cref="LoggingPipelineLineageSink" /> for pipeline-level lineage reporting.
    /// </summary>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="jsonOptions">Optional JSON serialization options for the sink.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    /// <remarks>
    ///     This is a convenience method that registers a logging sink for pipeline-level lineage information.
    ///     The sink will serialize lineage reports to JSON and log them using the configured logger.
    /// </remarks>
    public static PipelineBuilder UseLoggingPipelineLineageSink(
        this PipelineBuilder builder,
        JsonSerializerOptions? jsonOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var sink = new LoggingPipelineLineageSink(jsonOptions: jsonOptions);
        return builder.AddPipelineLineageSink(sink);
    }
}
