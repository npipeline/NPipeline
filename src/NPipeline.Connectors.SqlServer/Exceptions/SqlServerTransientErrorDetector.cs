using Microsoft.Data.SqlClient;

namespace NPipeline.Connectors.SqlServer.Exceptions;

/// <summary>
///     Detects transient (retryable) SQL Server errors.
/// </summary>
public static class SqlServerTransientErrorDetector
{
    /// <summary>
    ///     SQL Server error codes that are transient (retryable).
    /// </summary>
    /// <remarks>
    ///     -2: Timeout expired
    ///     53: Named Pipes Provider error (network not found)
    ///     64: Named Pipes Provider error (network disconnected)
    ///     121: Named Pipes Provider error (network timeout)
    ///     1205: Deadlock victim
    ///     40501: Azure SQL Database service busy
    ///     40613: Azure SQL Database service unavailable
    ///     49918: Azure SQL Database insufficient resources
    ///     49919: Azure SQL Database insufficient resources
    ///     49920: Azure SQL Database insufficient resources
    /// </remarks>
    private static readonly HashSet<int> TransientErrorCodes = new()
    {
        -2, // Timeout expired
        53, // Named Pipes Provider error (network not found)
        64, // Named Pipes Provider error (network disconnected)
        121, // Named Pipes Provider error (network timeout)
        1205, // Deadlock victim
        40501, // Azure SQL Database service busy
        40613, // Azure SQL Database service unavailable
        49918, // Azure SQL Database insufficient resources
        49919, // Azure SQL Database insufficient resources
        49920, // Azure SQL Database insufficient resources
    };

    /// <summary>
    ///     Determines if an exception represents a transient (retryable) error.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if error is transient; otherwise, false.</returns>
    public static bool IsTransient(Exception exception)
    {
        return exception switch
        {
            TimeoutException => true,
            OperationCanceledException => true,
            SqlException sqlEx when sqlEx.Errors.Count > 0 =>
                sqlEx.Errors.Cast<SqlError>().Any(e => IsTransientError(e.Number)),
            SqlException sqlEx => IsTransientError(sqlEx.Number),
            InvalidOperationException invalidOpEx when
                invalidOpEx.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                invalidOpEx.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) => true,
            _ => false,
        };
    }

    /// <summary>
    ///     Gets the SQL Server error code from a SqlException.
    /// </summary>
    /// <param name="exception">The SqlException to extract the error code from.</param>
    /// <returns>The error code, or null if not found.</returns>
    public static int? GetErrorCode(SqlException exception)
    {
        if (exception.Errors.Count > 0)
            return exception.Errors[0].Number;

        return exception.Number;
    }

    /// <summary>
    ///     Determines if a specific SQL Server error code is transient.
    /// </summary>
    /// <param name="errorCode">The SQL Server error code.</param>
    /// <returns>True if the error code is transient; otherwise, false.</returns>
    public static bool IsTransientError(int errorCode)
    {
        return TransientErrorCodes.Contains(errorCode);
    }
}
