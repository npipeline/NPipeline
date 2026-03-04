using NPipeline.Connectors.MySql.Configuration;

namespace NPipeline.Connectors.MySql.Exceptions;

/// <summary>
///     Utility methods for handling MySQL exceptions and implementing retry logic.
/// </summary>
public static class MySqlExceptionHandler
{
    private static readonly Dictionary<int, string> ErrorDescriptions = new()
    {
        [1040] = "Too many connections",
        [1044] = "Access denied for user to database",
        [1045] = "Access denied for user (wrong password)",
        [1049] = "Unknown database",
        [1062] = "Duplicate entry for key (unique constraint violation)",
        [1064] = "SQL syntax error",
        [1146] = "Table doesn't exist",
        [1205] = "Lock wait timeout exceeded",
        [1213] = "Deadlock found when trying to get lock",
        [1216] = "Foreign key constraint fails (child row violation)",
        [1217] = "Foreign key constraint fails (parent row violation)",
        [1292] = "Incorrect datetime value",
        [1366] = "Incorrect integer value",
        [1406] = "Data too long for column",
        [2006] = "MySQL server has gone away",
        [2013] = "Lost connection to MySQL server during query",
    };

    /// <summary>
    ///     Determines whether an exception should be retried based on the configuration.
    /// </summary>
    public static bool ShouldRetry(Exception exception, MySqlConfiguration configuration)
    {
        if (configuration.MaxRetryAttempts <= 0)
            return false;

        return MySqlTransientErrorDetector.IsTransient(exception);
    }

    /// <summary>
    ///     Gets the retry delay for a given attempt using exponential back-off with jitter.
    /// </summary>
    public static TimeSpan GetRetryDelay(Exception exception, int attemptCount,
        MySqlConfiguration configuration)
    {
        var exponentialDelay = TimeSpan.FromSeconds(
            configuration.RetryDelay.TotalSeconds * Math.Pow(2, attemptCount - 1));

        // ±25 % jitter to avoid thundering herd
        var jitterFactor = 0.75 + Random.Shared.NextDouble() * 0.5;
        var delayWithJitter = TimeSpan.FromMilliseconds(
            exponentialDelay.TotalMilliseconds * jitterFactor);

        const int maxDelaySeconds = 30;
        if (delayWithJitter.TotalSeconds > maxDelaySeconds)
            delayWithJitter = TimeSpan.FromSeconds(maxDelaySeconds);

        return delayWithJitter;
    }

    /// <summary>
    ///     Handles an exception by wrapping it in a MySQL exception or rethrowing it.
    /// </summary>
    public static void Handle(Exception exception, MySqlConfiguration configuration)
    {
        if (exception is MySqlException or MySqlConnectionException or MySqlMappingException)
            throw exception;

        var isTransient = MySqlTransientErrorDetector.IsTransient(exception);
        var errorCode = GetErrorCode(exception);

        if (IsConnectionError(exception))
        {
            throw MySqlExceptionFactory.CreateConnection(
                $"Connection error: {exception.Message}",
                exception);
        }

        throw new MySqlException(exception.Message, errorCode, isTransient, exception);
    }

    /// <summary>
    ///     Gets the MySQL error code string from an exception.
    /// </summary>
    public static string? GetErrorCode(Exception exception) =>
        exception switch
        {
            MySqlConnector.MySqlException mysqlEx => mysqlEx.Number.ToString(),
            MySqlException mysqlEx => mysqlEx.ErrorCode,
            _ => null,
        };

    /// <summary>
    ///     Returns a human-readable description for a MySQL error code.
    /// </summary>
    public static string? GetErrorDescription(int errorCode) =>
        ErrorDescriptions.TryGetValue(errorCode, out var description) ? description : null;

    private static bool IsConnectionError(Exception exception) =>
        exception switch
        {
            MySqlConnectionException => true,
            MySqlConnector.MySqlException mysqlEx =>
                mysqlEx.Number is 1040 or 2006 or 2013,
            InvalidOperationException invalidOpEx when
                invalidOpEx.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) => true,
            _ => false,
        };
}
