namespace NPipeline.Observability.Logging;

/// <summary>
///     Represents a type that can create instances of <see cref="IPipelineLogger" />.
/// </summary>
public interface IPipelineLoggerFactory
{
    /// <summary>
    ///     Creates a new <see cref="IPipelineLogger" /> instance.
    /// </summary>
    /// <param name="categoryName">The category name for messages produced by the logger.</param>
    /// <returns>The <see cref="IPipelineLogger" />.</returns>
    IPipelineLogger CreateLogger(string categoryName);
}
