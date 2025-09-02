using NPipeline.Observability.Logging;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A logger that captures all log messages for inspection.
/// </summary>
public sealed class CapturingLogger : IPipelineLogger
{
    private readonly List<LogEntry> _logEntries = [];

    /// <summary>
    ///     Gets the list of log entries that have been captured.
    /// </summary>
    public IReadOnlyList<LogEntry> LogEntries => _logEntries;

    /// <inheritdoc />
    public void Log(LogLevel logLevel, string message, params object[] args)
    {
        _logEntries.Add(new LogEntry(logLevel, message, args));
    }

    /// <inheritdoc />
    public void Log(LogLevel logLevel, Exception exception, string message, params object[] args)
    {
        _logEntries.Add(new LogEntry(logLevel, message, args, exception));
    }

    /// <inheritdoc />
    public bool IsEnabled(LogLevel logLevel)
    {
        return true;
    }
}

/// <summary>
///     Represents a single log entry captured by the <see cref="CapturingLogger" />.
/// </summary>
/// <param name="LogLevel">The level of the log entry.</param>
/// <param name="Message">The log message.</param>
/// <param name="Args">The arguments for the log message.</param>
/// <param name="Exception">The optional exception associated with the log entry.</param>
public sealed record LogEntry(LogLevel LogLevel, string Message, object[] Args, Exception? Exception = null);
