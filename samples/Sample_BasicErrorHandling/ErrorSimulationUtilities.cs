using System.Collections.Concurrent;

namespace Sample_BasicErrorHandling;

/// <summary>
///     Provides utilities for simulating various types of errors in pipeline components.
///     This class helps create realistic failure scenarios for testing error handling mechanisms.
/// </summary>
public static class ErrorSimulation
{
    private static readonly Random Random = new();

    /// <summary>
    ///     Creates an InvalidOperationException with a detailed message.
    /// </summary>
    /// <param name="operationName">The name of the operation that failed.</param>
    /// <param name="additionalDetails">Optional additional details about the error.</param>
    /// <returns>An InvalidOperationException with formatted message.</returns>
    public static InvalidOperationException CreateInvalidOperationException(string operationName, string? additionalDetails = null)
    {
        var message = additionalDetails != null
            ? $"Operation '{operationName}' failed. {additionalDetails}"
            : $"Operation '{operationName}' failed.";

        return new InvalidOperationException(message);
    }

    /// <summary>
    ///     Creates a TimeoutException with a detailed message.
    /// </summary>
    /// <param name="operationName">The name of the operation that timed out.</param>
    /// <param name="timeoutDuration">The duration that was exceeded.</param>
    /// <returns>A TimeoutException with formatted message.</returns>
    public static TimeoutException CreateTimeoutException(string operationName, TimeSpan timeoutDuration)
    {
        return new TimeoutException($"Operation '{operationName}' timed out after {timeoutDuration.TotalSeconds} seconds.");
    }

    /// <summary>
    ///     Creates an ArgumentException with a detailed message.
    /// </summary>
    /// <param name="parameterName">The name of the invalid parameter.</param>
    /// <param name="reason">The reason why the parameter is invalid.</param>
    /// <returns>An ArgumentException with formatted message.</returns>
    public static ArgumentException CreateArgumentException(string parameterName, string reason)
    {
        return new ArgumentException($"Parameter '{parameterName}' is invalid. {reason}", parameterName);
    }

    /// <summary>
    ///     Creates a custom exception with detailed error information.
    /// </summary>
    /// <param name="exceptionType">The type of exception to create.</param>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">Optional inner exception.</param>
    /// <returns>An exception of the specified type with the provided details.</returns>
    public static Exception CreateDetailedException(Type exceptionType, string message, Exception? innerException = null)
    {
        if (!typeof(Exception).IsAssignableFrom(exceptionType))
            throw new ArgumentException($"Type {exceptionType.Name} is not an exception type", nameof(exceptionType));

        var constructors = exceptionType.GetConstructors();

        // Try to find a constructor that accepts a string and optionally an Exception
        var constructor = constructors.FirstOrDefault(c =>
        {
            var parameters = c.GetParameters();

            return (parameters.Length == 1 && parameters[0].ParameterType == typeof(string)) ||
                   (parameters.Length == 2 && parameters[0].ParameterType == typeof(string) &&
                    parameters[1].ParameterType == typeof(Exception));
        });

        if (constructor != null)
        {
            var parameters = constructor.GetParameters().Length == 2
                ? new object[] { message, innerException! }
                : new object[] { message };

            return (Exception)Activator.CreateInstance(exceptionType, parameters)!;
        }

        // Fallback to default constructor if available
        if (exceptionType.GetConstructor(Type.EmptyTypes) != null)
            return (Exception)Activator.CreateInstance(exceptionType)!;

        throw new InvalidOperationException($"Cannot create instance of {exceptionType.Name} - no suitable constructor found");
    }

    /// <summary>
    ///     Simulates a random failure based on the specified failure rate.
    /// </summary>
    /// <param name="failureRate">The probability of failure (0.0 to 1.0).</param>
    /// <param name="exceptionFactory">Optional factory method to create the exception on failure.</param>
    /// <exception cref="Exception">Thrown when a failure is simulated.</exception>
    public static void SimulateRandomFailure(double failureRate, Func<Exception>? exceptionFactory = null)
    {
        if (Random.NextDouble() < failureRate)
        {
            var exception = exceptionFactory?.Invoke() ?? new InvalidOperationException("Simulated random failure");
            throw exception;
        }
    }

    /// <summary>
    ///     Simulates a network failure with appropriate exception type.
    /// </summary>
    /// <param name="resourceName">The name of the network resource that failed.</param>
    /// <param name="failureRate">The probability of failure (0.0 to 1.0).</param>
    /// <exception cref="InvalidOperationException">Thrown when a network failure is simulated.</exception>
    public static void SimulateNetworkFailure(string resourceName, double failureRate)
    {
        SimulateRandomFailure(failureRate, () =>
            new InvalidOperationException($"Network failure: Unable to connect to '{resourceName}'"));
    }

    /// <summary>
    ///     Simulates a disk I/O failure with appropriate exception type.
    /// </summary>
    /// <param name="filePath">The file path that failed.</param>
    /// <param name="failureRate">The probability of failure (0.0 to 1.0).</param>
    /// <exception cref="InvalidOperationException">Thrown when a disk failure is simulated.</exception>
    public static void SimulateDiskFailure(string filePath, double failureRate)
    {
        SimulateRandomFailure(failureRate, () =>
            new InvalidOperationException($"Disk I/O failure: Unable to access file '{filePath}'"));
    }

    /// <summary>
    ///     Simulates a database failure with appropriate exception type.
    /// </summary>
    /// <param name="operation">The database operation that failed.</param>
    /// <param name="failureRate">The probability of failure (0.0 to 1.0).</param>
    /// <exception cref="InvalidOperationException">Thrown when a database failure is simulated.</exception>
    public static void SimulateDatabaseFailure(string operation, double failureRate)
    {
        SimulateRandomFailure(failureRate, () =>
            new InvalidOperationException($"Database failure: {operation} operation failed"));
    }
}

/// <summary>
///     Provides centralized error logging functionality with consistent formatting and severity tracking.
///     This class helps maintain consistent error logging across all pipeline components.
/// </summary>
public static class ErrorLogger
{
    /// <summary>
    ///     Represents the severity level of an error log entry.
    /// </summary>
    public enum ErrorSeverity
    {
        Info,
        Warning,
        Error,
        Critical,
    }

    private static readonly ConcurrentDictionary<Type, int> ErrorCounts = new();
    private static readonly List<ErrorLogEntry> ErrorHistory = new();

    /// <summary>
    ///     Logs an error with the specified severity and context.
    /// </summary>
    /// <param name="severity">The severity level of the error.</param>
    /// <param name="component">The name of the component where the error occurred.</param>
    /// <param name="message">The error message.</param>
    /// <param name="exception">Optional exception details.</param>
    /// <param name="context">Optional context information as key-value pairs.</param>
    public static void LogError(ErrorSeverity severity, string component, string message, Exception? exception = null,
        Dictionary<string, object>? context = null)
    {
        var entry = new ErrorLogEntry
        {
            Timestamp = DateTime.UtcNow,
            Severity = severity,
            Component = component,
            Message = message,
            Exception = exception,
            Context = context ?? new Dictionary<string, object>(),
        };

        // Track error counts by type
        if (exception != null)
            ErrorCounts.AddOrUpdate(exception.GetType(), 1, (_, count) => count + 1);

        // Add to history (limit to last 1000 entries to prevent memory issues)
        lock (ErrorHistory)
        {
            ErrorHistory.Add(entry);

            if (ErrorHistory.Count > 1000)
                ErrorHistory.RemoveAt(0);
        }

        // Format and output the error message
        var formattedMessage = FormatErrorMessage(entry);
        Console.WriteLine(formattedMessage);
    }

    /// <summary>
    ///     Logs an informational message.
    /// </summary>
    /// <param name="component">The name of the component.</param>
    /// <param name="message">The informational message.</param>
    /// <param name="context">Optional context information.</param>
    public static void LogInfo(string component, string message, Dictionary<string, object>? context = null)
    {
        LogError(ErrorSeverity.Info, component, message, null, context);
    }

    /// <summary>
    ///     Logs a warning message.
    /// </summary>
    /// <param name="component">The name of the component.</param>
    /// <param name="message">The warning message.</param>
    /// <param name="context">Optional context information.</param>
    public static void LogWarning(string component, string message, Dictionary<string, object>? context = null)
    {
        LogError(ErrorSeverity.Warning, component, message, null, context);
    }

    /// <summary>
    ///     Logs an error message.
    /// </summary>
    /// <param name="component">The name of the component.</param>
    /// <param name="message">The error message.</param>
    /// <param name="exception">Optional exception details.</param>
    /// <param name="context">Optional context information.</param>
    public static void LogError(string component, string message, Exception? exception = null, Dictionary<string, object>? context = null)
    {
        LogError(ErrorSeverity.Error, component, message, exception, context);
    }

    /// <summary>
    ///     Logs a critical error message.
    /// </summary>
    /// <param name="component">The name of the component.</param>
    /// <param name="message">The critical error message.</param>
    /// <param name="exception">Optional exception details.</param>
    /// <param name="context">Optional context information.</param>
    public static void LogCritical(string component, string message, Exception? exception = null, Dictionary<string, object>? context = null)
    {
        LogError(ErrorSeverity.Critical, component, message, exception, context);
    }

    /// <summary>
    ///     Gets the count of errors by exception type.
    /// </summary>
    /// <returns>A dictionary mapping exception types to their occurrence counts.</returns>
    public static Dictionary<Type, int> GetErrorCounts()
    {
        return new Dictionary<Type, int>(ErrorCounts);
    }

    /// <summary>
    ///     Gets the recent error history.
    /// </summary>
    /// <param name="maxEntries">Maximum number of entries to return.</param>
    /// <returns>A list of recent error log entries.</returns>
    public static List<ErrorLogEntry> GetErrorHistory(int maxEntries = 100)
    {
        lock (ErrorHistory)
        {
            return ErrorHistory.TakeLast(maxEntries).ToList();
        }
    }

    /// <summary>
    ///     Gets error statistics grouped by severity level.
    /// </summary>
    /// <returns>A dictionary mapping severity levels to their counts.</returns>
    public static Dictionary<ErrorSeverity, int> GetErrorStatistics()
    {
        lock (ErrorHistory)
        {
            return ErrorHistory.GroupBy(e => e.Severity)
                .ToDictionary(g => g.Key, g => g.Count());
        }
    }

    /// <summary>
    ///     Clears all error history and statistics.
    /// </summary>
    public static void ClearErrorHistory()
    {
        ErrorCounts.Clear();

        lock (ErrorHistory)
        {
            ErrorHistory.Clear();
        }

        Console.WriteLine("ErrorLogger: Error history cleared");
    }

    /// <summary>
    ///     Formats an error log entry into a consistent string representation.
    /// </summary>
    /// <param name="entry">The error log entry to format.</param>
    /// <returns>A formatted error message string.</returns>
    private static string FormatErrorMessage(ErrorLogEntry entry)
    {
        var severityStr = entry.Severity.ToString().ToUpper();
        var timestamp = entry.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff");

        var message = $"[{timestamp}] [{severityStr}] [{entry.Component}] {entry.Message}";

        if (entry.Exception != null)
            message += $" | Exception: {entry.Exception.GetType().Name}: {entry.Exception.Message}";

        if (entry.Context.Any())
        {
            var contextStr = string.Join(", ", entry.Context.Select(kvp => $"{kvp.Key}={kvp.Value}"));
            message += $" | Context: {contextStr}";
        }

        return message;
    }

    /// <summary>
    ///     Represents a single error log entry with metadata.
    /// </summary>
    public class ErrorLogEntry
    {
        public DateTime Timestamp { get; set; }
        public ErrorSeverity Severity { get; set; }
        public string Component { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
        public Exception? Exception { get; set; }
        public Dictionary<string, object> Context { get; set; } = new();
    }
}

/// <summary>
///     Provides metrics tracking for error handling performance and effectiveness.
///     This class helps measure the success rate of error handling strategies.
/// </summary>
public static class ErrorMetrics
{
    private static int _totalOperations;
    private static int _successfulOperations;
    private static int _failedOperations;
    private static int _retryCount;
    private static int _fallbackActivations;
    private static readonly ConcurrentDictionary<string, int> ComponentFailures = new();
    private static readonly ConcurrentDictionary<Type, int> ExceptionTypes = new();

    /// <summary>
    ///     Records the start of an operation.
    /// </summary>
    /// <param name="componentName">The name of the component performing the operation.</param>
    public static void RecordOperationStart(string componentName)
    {
        _totalOperations++;
    }

    /// <summary>
    ///     Records a successful operation.
    /// </summary>
    /// <param name="componentName">The name of the component that succeeded.</param>
    public static void RecordOperationSuccess(string componentName)
    {
        _successfulOperations++;
    }

    /// <summary>
    ///     Records a failed operation.
    /// </summary>
    /// <param name="componentName">The name of the component that failed.</param>
    /// <param name="exception">The exception that caused the failure.</param>
    public static void RecordOperationFailure(string componentName, Exception exception)
    {
        _failedOperations++;
        ComponentFailures.AddOrUpdate(componentName, 1, (_, count) => count + 1);
        ExceptionTypes.AddOrUpdate(exception.GetType(), 1, (_, count) => count + 1);
    }

    /// <summary>
    ///     Records a retry attempt.
    /// </summary>
    /// <param name="componentName">The name of the component performing the retry.</param>
    /// <param name="attemptNumber">The attempt number (1-based).</param>
    public static void RecordRetryAttempt(string componentName, int attemptNumber)
    {
        _retryCount++;
    }

    /// <summary>
    ///     Records a fallback activation.
    /// </summary>
    /// <param name="componentName">The name of the component that activated fallback.</param>
    public static void RecordFallbackActivation(string componentName)
    {
        _fallbackActivations++;
    }

    /// <summary>
    ///     Calculates the success rate as a percentage.
    /// </summary>
    /// <returns>The success rate (0.0 to 100.0).</returns>
    public static double GetSuccessRate()
    {
        if (_totalOperations == 0)
            return 0.0;

        return (double)_successfulOperations / _totalOperations * 100.0;
    }

    /// <summary>
    ///     Calculates the failure rate as a percentage.
    /// </summary>
    /// <returns>The failure rate (0.0 to 100.0).</returns>
    public static double GetFailureRate()
    {
        if (_totalOperations == 0)
            return 0.0;

        return (double)_failedOperations / _totalOperations * 100.0;
    }

    /// <summary>
    ///     Gets the total number of operations recorded.
    /// </summary>
    /// <returns>The total operation count.</returns>
    public static int GetTotalOperations()
    {
        return _totalOperations;
    }

    /// <summary>
    ///     Gets the total number of successful operations.
    /// </summary>
    /// <returns>The successful operation count.</returns>
    public static int GetSuccessfulOperations()
    {
        return _successfulOperations;
    }

    /// <summary>
    ///     Gets the total number of failed operations.
    /// </summary>
    /// <returns>The failed operation count.</returns>
    public static int GetFailedOperations()
    {
        return _failedOperations;
    }

    /// <summary>
    ///     Gets the total number of retry attempts.
    /// </summary>
    /// <returns>The retry count.</returns>
    public static int GetRetryCount()
    {
        return _retryCount;
    }

    /// <summary>
    ///     Gets the total number of fallback activations.
    /// </summary>
    /// <returns>The fallback activation count.</returns>
    public static int GetFallbackActivations()
    {
        return _fallbackActivations;
    }

    /// <summary>
    ///     Gets failure counts by component name.
    /// </summary>
    /// <returns>A dictionary mapping component names to their failure counts.</returns>
    public static Dictionary<string, int> GetComponentFailures()
    {
        return new Dictionary<string, int>(ComponentFailures);
    }

    /// <summary>
    ///     Gets exception counts by exception type.
    /// </summary>
    /// <returns>A dictionary mapping exception types to their occurrence counts.</returns>
    public static Dictionary<Type, int> GetExceptionTypes()
    {
        return new Dictionary<Type, int>(ExceptionTypes);
    }

    /// <summary>
    ///     Generates a comprehensive summary of all error metrics.
    /// </summary>
    /// <returns>A formatted string containing all metrics.</returns>
    public static string GetMetricsSummary()
    {
        var summary = new List<string>
        {
            "=== ERROR METRICS SUMMARY ===",
            $"Total Operations: {_totalOperations}",
            $"Successful Operations: {_successfulOperations} ({GetSuccessRate():F1}%)",
            $"Failed Operations: {_failedOperations} ({GetFailureRate():F1}%)",
            $"Total Retry Attempts: {_retryCount}",
            $"Total Fallback Activations: {_fallbackActivations}",
            "",
        };

        if (ComponentFailures.Any())
        {
            summary.Add("Component Failures:");

            foreach (var kvp in ComponentFailures.OrderByDescending(x => x.Value))
            {
                summary.Add($"  {kvp.Key}: {kvp.Value}");
            }

            summary.Add("");
        }

        if (ExceptionTypes.Any())
        {
            summary.Add("Exception Types:");

            foreach (var kvp in ExceptionTypes.OrderByDescending(x => x.Value))
            {
                summary.Add($"  {kvp.Key.Name}: {kvp.Value}");
            }
        }

        return string.Join(Environment.NewLine, summary);
    }

    /// <summary>
    ///     Resets all metrics to zero.
    /// </summary>
    public static void ResetMetrics()
    {
        _totalOperations = 0;
        _successfulOperations = 0;
        _failedOperations = 0;
        _retryCount = 0;
        _fallbackActivations = 0;
        ComponentFailures.Clear();
        ExceptionTypes.Clear();

        Console.WriteLine("ErrorMetrics: All metrics reset to zero");
    }

    /// <summary>
    ///     Prints the current metrics summary to the console.
    /// </summary>
    public static void PrintMetricsSummary()
    {
        Console.WriteLine();
        Console.WriteLine(GetMetricsSummary());
        Console.WriteLine();
    }
}
