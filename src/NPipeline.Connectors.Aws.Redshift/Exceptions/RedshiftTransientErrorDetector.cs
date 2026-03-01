using System.Net.Sockets;
using Npgsql;

namespace NPipeline.Connectors.Aws.Redshift.Exceptions;

/// <summary>
///     Detects transient errors that can be safely retried.
/// </summary>
public static class RedshiftTransientErrorDetector
{
    // PostgreSQL/Redshift SQLSTATE codes for transient errors
    private static readonly HashSet<string> TransientSqlStates = new(StringComparer.Ordinal)
    {
        // Connection errors
        "08000", // connection_exception
        "08003", // connection_does_not_exist
        "08006", // connection_failure
        "08001", // sqlclient_unable_to_establish_sqlconnection
        "08004", // sqlserver_rejected_establishment_of_sqlconnection
        "08007", // transaction_resolution_unknown
        "08501", // insufficient_privilege

        // Cluster state errors
        "57P01", // admin_shutdown
        "57P02", // crash_shutdown
        "57P03", // cannot_connect_now
        "57P04", // database_dropped

        // Concurrency errors
        "40001", // serialization_failure
        "40P01", // deadlock_detected
        "53000", // insufficient_resources
        "53100", // disk_full
        "53200", // out_of_memory
        "53300", // too_many_connections
        "53400", // configuration_limit_exceeded

        // System errors
        "55000", // object_not_in_prerequisite_state
        "55006", // object_in_use
        "55P02", // cant_change_runtime_param
        "55P03", // lock_not_available

        // IO errors
        "58000", // system_error
        "58030", // io_error
    };

    /// <summary>
    ///     Determines whether an exception represents a transient error that can be retried.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the error is transient and can be retried.</returns>
    public static bool IsTransient(Exception? exception)
    {
        return exception switch
        {
            null => false,
            RedshiftException redshiftEx => IsTransientSqlState(redshiftEx.SqlState) ||
                                            IsTransient(redshiftEx.InnerException),
            NpgsqlException npgsqlEx => IsTransientNpgsqlError(npgsqlEx),
            SocketException => true, // Network errors are always transient
            TimeoutException => true, // Timeouts are transient
            AggregateException aggEx => aggEx.InnerExceptions.Any(IsTransient),
            _ => IsTransient(exception.InnerException),
        };
    }

    private static bool IsTransientNpgsqlError(NpgsqlException exception)
    {
        // Check SQLSTATE
        if (IsTransientSqlState(exception.SqlState))
            return true;

        // Check inner exception
        if (exception.InnerException != null && IsTransient(exception.InnerException))
            return true;

        // Check for specific Npgsql error conditions
        if (exception.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase))
            return true;

        if (exception.Message.Contains("connection", StringComparison.OrdinalIgnoreCase))
            return true;

        return false;
    }

    private static bool IsTransientSqlState(string? sqlState)
    {
        if (string.IsNullOrEmpty(sqlState))
            return false;

        // Check exact match
        if (TransientSqlStates.Contains(sqlState))
            return true;

        // Check class prefix (first 2 characters)
        var classPrefix = sqlState.Substring(0, Math.Min(2, sqlState.Length));
        return classPrefix is "08" or "57" or "58";
    }
}
