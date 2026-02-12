using Microsoft.Extensions.Logging;

namespace NPipeline.Observability.Logging;

/// <summary>
///     A null logger factory that creates <see cref="NullLogger" /> instances.
///     Used as a default when no logging is required.
/// </summary>
public sealed class NullLoggerFactory : ILoggerFactory
{
    /// <summary>
    ///     Gets the singleton instance of the null logger factory.
    /// </summary>
    public static readonly NullLoggerFactory Instance = new();

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName)
    {
        return NullLogger.Instance;
    }

    /// <inheritdoc />
    public void AddProvider(ILoggerProvider provider)
    {
    }

    /// <inheritdoc />
    public void Dispose()
    {
    }
}

/// <summary>
///     A null logger that discards all log messages.
///     Used as a default when no logging is required.
/// </summary>
public sealed class NullLogger : ILogger
{
    /// <summary>
    ///     Gets the singleton instance of the null logger.
    /// </summary>
    public static readonly NullLogger Instance = new();

    /// <inheritdoc />
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        return null;
    }

    /// <inheritdoc />
    public bool IsEnabled(LogLevel logLevel)
    {
        return false;
    }

    /// <inheritdoc />
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
    }
}
