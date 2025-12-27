namespace NPipeline.Observability.Logging;

/// <summary>
///     A logger factory that creates instances of <see cref="NullPipelineLogger" />.
/// </summary>
public class NullPipelineLoggerFactory : IPipelineLoggerFactory
{
    private NullPipelineLoggerFactory()
    {
    }

    /// <summary>
    ///     Returns the shared instance of <see cref="NullPipelineLoggerFactory" />.
    /// </summary>
    public static NullPipelineLoggerFactory Instance { get; } = new();

    /// <inheritdoc />
    public IPipelineLogger CreateLogger(string categoryName)
    {
        return NullPipelineLogger.Instance;
    }
}
