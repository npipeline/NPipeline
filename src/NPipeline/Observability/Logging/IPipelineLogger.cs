namespace NPipeline.Observability.Logging;

/// <summary>
///     A generic interface for logging events within a pipeline.
/// </summary>
/// <remarks>
///     <para>
///         The pipeline logger provides a simple, unified interface for logging across the pipeline framework.
///         It supports multiple log levels and formatted messages, and can be integrated with popular logging
///         frameworks via adapters.
///     </para>
///     <para>
///         For testing and scenarios where logging is not needed, use <see cref="NullPipelineLogger.Instance" />,
///         which provides a no-op implementation that discards all log entries.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Custom logger implementation
/// public class ConsoleLogger : IPipelineLogger
/// {
///     public void Log(LogLevel level, string message, params object[] args)
///     {
///         Console.WriteLine($"[{level}] {string.Format(message, args)}");
///     }
/// 
///     public void Log(LogLevel level, Exception ex, string message, params object[] args)
///     {
///         var formatted = string.Format(message, args);
///         Console.WriteLine($"[{level}] {formatted}");
///         Console.WriteLine($"Exception: {ex}");
///     }
/// 
///     public bool IsEnabled(LogLevel level) => true;
/// }
/// 
/// // Use in pipeline context
/// var context = new PipelineContext(
///     PipelineContextConfiguration.WithLogging(new ConsoleLogger()));
/// </code>
/// </example>
public interface IPipelineLogger
{
    /// <summary>
    ///     Writes a log entry.
    /// </summary>
    /// <param name="logLevel">Entry will be written on this level.</param>
    /// <param name="message">The message to write.</param>
    /// <param name="args">Optional arguments to format the message.</param>
    /// <remarks>
    ///     The message supports composite formatting with the provided arguments,
    ///     similar to <see cref="string.Format(string, object[])" />.
    /// </remarks>
    void Log(LogLevel logLevel, string message, params object[] args);

    /// <summary>
    ///     Writes a log entry with an exception.
    /// </summary>
    /// <param name="logLevel">Entry will be written on this level.</param>
    /// <param name="exception">The exception to log.</param>
    /// <param name="message">The message to write.</param>
    /// <param name="args">Optional arguments to format the message.</param>
    /// <remarks>
    ///     Include the exception in log output for error diagnostics. The logger should record
    ///     the exception details including the message, stack trace, and any inner exceptions.
    /// </remarks>
    void Log(LogLevel logLevel, Exception exception, string message, params object[] args);

    /// <summary>
    ///     Checks if the given <paramref name="logLevel" /> is enabled.
    /// </summary>
    /// <param name="logLevel">The level to be checked.</param>
    /// <returns><c>true</c> if enabled.</returns>
    /// <remarks>
    ///     Use this to avoid expensive message formatting when the log level is disabled.
    ///     For example, check before constructing large debug information.
    /// </remarks>
    bool IsEnabled(LogLevel logLevel);
}
