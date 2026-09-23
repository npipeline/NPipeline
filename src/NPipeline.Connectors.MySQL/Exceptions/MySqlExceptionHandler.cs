using NPipeline.Connectors.MySql.Configuration;

namespace NPipeline.Connectors.MySql.Exceptions;

/// <summary>
///     Utility methods for translating and describing MySQL exceptions.
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
    public static string? GetErrorCode(Exception exception)
    {
        return exception switch
        {
            MySqlConnector.MySqlException mysqlEx => mysqlEx.Number.ToString(),
            MySqlException mysqlEx => mysqlEx.ErrorCode,
            _ => null,
        };
    }

    /// <summary>
    ///     Returns a human-readable description for a MySQL error code.
    /// </summary>
    public static string? GetErrorDescription(int errorCode)
    {
        return ErrorDescriptions.TryGetValue(errorCode, out var description)
            ? description
            : null;
    }

    private static bool IsConnectionError(Exception exception)
    {
        return exception switch
        {
            MySqlConnectionException => true,
            MySqlConnector.MySqlException mysqlEx =>
                mysqlEx.Number is 1040 or 2006 or 2013,
            InvalidOperationException invalidOpEx when
                invalidOpEx.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) => true,
            _ => false,
        };
    }
}
