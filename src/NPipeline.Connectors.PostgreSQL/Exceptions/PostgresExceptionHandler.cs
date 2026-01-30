using Npgsql;

namespace NPipeline.Connectors.PostgreSQL.Exceptions;

/// <summary>
///     Utility methods for translating and classifying PostgreSQL exceptions.
/// </summary>
public static class PostgresExceptionHandler
{
    private static readonly Dictionary<string, string> ErrorDescriptions = new()
    {
        ["08000"] = "Connection Exception",
        ["08001"] = "SQL Client Unable to Establish Connection",
        ["08003"] = "Connection Does Not Exist",
        ["08004"] = "SQL Server Rejected Establishment",
        ["08006"] = "Connection Failure",
        ["08007"] = "Transaction Resolution Unknown",
        ["40001"] = "Serialization Failure / Deadlock detected",
        ["40P01"] = "Deadlock Detected",
        ["53000"] = "Insufficient Resources",
        ["53100"] = "Disk Full",
        ["53200"] = "Out of Memory",
        ["53300"] = "Too Many Connections",
        ["23000"] = "Integrity Constraint Violation",
        ["23502"] = "Not Null Violation",
        ["23503"] = "Foreign Key Violation",
        ["23505"] = "Unique Violation",
        ["23514"] = "Check Violation",
        ["42601"] = "Syntax Error",
        ["42703"] = "Undefined Column",
        ["42P01"] = "Undefined Table",
        ["28000"] = "Invalid Authorization",
        ["28P01"] = "Invalid Password",
    };

    /// <summary>
    ///     Translates an exception into a <see cref="PostgresException" /> with context.
    /// </summary>
    public static PostgresException Translate(string message, Exception ex)
    {
        return ex switch
        {
            Npgsql.PostgresException pgEx =>
                new PostgresException(
                    string.IsNullOrEmpty(GetErrorDescription(pgEx.SqlState))
                        ? message
                        : $"{message} ({GetErrorDescription(pgEx.SqlState)})",
                    pgEx.SqlState,
                    PostgresTransientErrorDetector.IsTransientSqlState(pgEx.SqlState),
                    pgEx),
            PostgresException existing => existing,
            NpgsqlException npgsqlEx => new PostgresException(message, npgsqlEx),
            _ => new PostgresException(message, ex),
        };
    }

    /// <summary>
    ///     Returns a human-readable description for a SQLSTATE code when available.
    /// </summary>
    public static string? GetErrorDescription(string? sqlState)
    {
        return string.IsNullOrEmpty(sqlState)
            ? null
            : ErrorDescriptions.TryGetValue(sqlState, out var description)
                ? description
                : null;
    }

    /// <summary>
    ///     Determines whether an exception is transient and can be retried.
    /// </summary>
    public static bool IsTransient(Exception ex)
    {
        return ex switch
        {
            PostgresException pipelinePgEx when pipelinePgEx.ErrorCode is not null => IsTransientSqlState(pipelinePgEx.ErrorCode),
            Npgsql.PostgresException pgEx => IsTransientSqlState(pgEx.SqlState),
            NpgsqlException => true,
            _ => ex.InnerException != null && IsTransient(ex.InnerException),
        };
    }

    /// <summary>
    ///     Evaluates SQLSTATE codes for retryability.
    /// </summary>
    public static bool IsTransientSqlState(string? sqlState)
    {
        return !string.IsNullOrEmpty(sqlState)
               && (sqlState.StartsWith("08", StringComparison.Ordinal)
                   || sqlState == "40001"
                   || sqlState == "40P01"
                   || sqlState.StartsWith("53", StringComparison.Ordinal)
                   || sqlState == "57P01"
                   || sqlState == "57P02"
                   || sqlState == "57P03");
    }
}
