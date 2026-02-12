using Microsoft.Extensions.Logging;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A logger that captures all log messages for inspection.
/// </summary>
public sealed class CapturingLogger : ILogger
{
    private readonly List<LogEntry> _logEntries = [];

    /// <summary>
    ///     Gets the list of log entries that have been captured.
    /// </summary>
    public IReadOnlyList<LogEntry> LogEntries => _logEntries;

    /// <inheritdoc />
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        return null;
    }

    /// <inheritdoc />
    public bool IsEnabled(LogLevel logLevel)
    {
        return true;
    }

    /// <inheritdoc />
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        ArgumentNullException.ThrowIfNull(formatter);

        var (message, args) = ExtractMessageAndArgs(state);
        _logEntries.Add(new LogEntry(logLevel, message, args, exception));
    }

    private static (string? Message, object[]? Args) ExtractMessageAndArgs<TState>(TState state)
    {
        if (state is IReadOnlyList<KeyValuePair<string, object?>> keyValuePairs)
        {
            // Try to get the original format (message template)
            string? originalFormat = null;
            var args = new List<object?>();

            foreach (var kvp in keyValuePairs)
            {
                if (kvp.Key == "{OriginalFormat}")
                    originalFormat = kvp.Value?.ToString();
                else
                    args.Add(kvp.Value);
            }

            // If we found an original format, return the template and args
            if (originalFormat is not null)
            {
                // Cast to handle nullability - args contain the original argument values
                return ((string?)originalFormat, (object[]?)(args.Count > 0
                    ? args.ToArray()
                    : null));
            }
        }

        // Fallback: state is the message itself (e.g., when logging without format arguments)
        var stateString = state?.ToString();
        return (stateString, null);
    }
}

/// <summary>
///     Represents a single log entry captured by the <see cref="CapturingLogger" />.
/// </summary>
/// <param name="LogLevel">The level of the log entry.</param>
/// <param name="Message">The log message template.</param>
/// <param name="Args">The arguments for the log message, or null if no arguments were provided.</param>
/// <param name="Exception">The optional exception associated with the log entry.</param>
public sealed record LogEntry(LogLevel LogLevel, string? Message, object[]? Args, Exception? Exception = null);
