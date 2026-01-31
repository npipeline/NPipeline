using Npgsql;

namespace NPipeline.Connectors.PostgreSQL.Exceptions;

/// <summary>
///     Detects transient (retryable) PostgreSQL errors.
/// </summary>
public static class PostgresTransientErrorDetector
{
    private static readonly HashSet<string> TransientErrorCodes = new(StringComparer.OrdinalIgnoreCase)
    {
        "08006", // Connection failure
        "08001", // SQL client unable to establish SQL connection
        "08004", // SQL server rejected establishment of SQL connection
        "57P01", // Admin shutdown
        "57P02", // Crash shutdown
        "57P03", // Cannot connect now
        "40001", // Serialization failure
        "40P01", // Deadlock detected
        "53000", // Disk full
        "53100", // Disk full
        "53200", // Out of memory
        "54000", // Statement timeout
    };

    /// <summary>
    ///     Determines if an exception represents a transient (retryable) error.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if error is transient; otherwise, false.</returns>
    public static bool IsTransientError(Exception exception)
    {
        return exception switch
        {
            TimeoutException => true,
            OperationCanceledException => true,
            Npgsql.PostgresException npgsqlEx => IsTransientSqlState(npgsqlEx.SqlState),
            NpgsqlException npgsqlEx2 => npgsqlEx2.IsTransient,
            _ => false,
        };
    }

    /// <summary>
    ///     Determines if a SQL state code represents a transient error.
    /// </summary>
    /// <param name="sqlState">The PostgreSQL SQL state code.</param>
    /// <returns>True if SQL state is transient; otherwise, false.</returns>
    public static bool IsTransientSqlState(string sqlState)
    {
        return !string.IsNullOrWhiteSpace(sqlState) && TransientErrorCodes.Contains(sqlState);
    }
}
