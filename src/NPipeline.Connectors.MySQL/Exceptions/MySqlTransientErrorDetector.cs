namespace NPipeline.Connectors.MySql.Exceptions;

/// <summary>
///     Detects transient (retryable) MySQL errors.
/// </summary>
public static class MySqlTransientErrorDetector
{
    /// <summary>
    ///     MySQL error codes that are considered transient (retryable).
    /// </summary>
    /// <remarks>
    ///     1040: Too many connections
    ///     1205: Lock wait timeout exceeded
    ///     1213: Deadlock found when trying to get lock
    ///     2006: MySQL server has gone away
    ///     2013: Lost connection to MySQL server during query
    /// </remarks>
    private static readonly HashSet<int> TransientErrorCodes = new()
    {
        1040, // Too many connections
        1205, // Lock wait timeout exceeded
        1213, // Deadlock found when trying to get lock
        2006, // MySQL server has gone away
        2013, // Lost connection to MySQL server during query
    };

    /// <summary>
    ///     Determines if an exception represents a transient (retryable) error.
    /// </summary>
    public static bool IsTransient(Exception exception)
    {
        return exception switch
        {
            TimeoutException => true,
            OperationCanceledException => true,
            MySqlConnector.MySqlException mysqlEx => IsTransientError(mysqlEx.Number),
            InvalidOperationException invalidOpEx when
                invalidOpEx.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                invalidOpEx.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) => true,
            _ => false,
        };
    }

    /// <summary>
    ///     Gets the MySQL error number from a <see cref="MySqlConnector.MySqlException"/>.
    /// </summary>
    public static int? GetErrorCode(MySqlConnector.MySqlException exception) =>
        exception.Number;

    /// <summary>
    ///     Determines if a specific MySQL error number is transient.
    /// </summary>
    public static bool IsTransientError(int errorCode) =>
        TransientErrorCodes.Contains(errorCode);
}
