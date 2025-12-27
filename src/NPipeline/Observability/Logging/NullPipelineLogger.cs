namespace NPipeline.Observability.Logging;

/// <summary>
///     A logger that does nothing.
/// </summary>
public class NullPipelineLogger : IPipelineLogger
{
    private NullPipelineLogger()
    {
    }

    /// <summary>
    ///     Returns the shared instance of <see cref="NullPipelineLogger" />.
    /// </summary>
    public static NullPipelineLogger Instance { get; } = new();

    /// <inheritdoc />
    public void Log(LogLevel logLevel, string message, params object[] args)
    {
    }

    /// <inheritdoc />
    public void Log(LogLevel logLevel, Exception exception, string message, params object[] args)
    {
    }

    /// <inheritdoc />
    public bool IsEnabled(LogLevel logLevel)
    {
        return false;
    }
}
