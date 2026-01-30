using NPipeline.Connectors.Exceptions;

namespace NPipeline.Connectors.Utilities;

/// <summary>
///     Provides static methods for classifying database errors.
/// </summary>
public static class DatabaseErrorClassifier
{
    /// <summary>
    ///     Determines whether an exception is a transient error that can be retried.
    /// </summary>
    /// <param name="exception">The exception to classify.</param>
    /// <returns>True if the exception is transient; otherwise, false.</returns>
    public static bool IsTransientError(Exception exception)
    {
        return exception is TimeoutException ||
               exception is OperationCanceledException ||
               (exception is DatabaseExceptionBase dbEx && IsTransientErrorCode(dbEx.ErrorCode));
    }

    /// <summary>
    ///     Determines whether an exception is a connection error.
    /// </summary>
    /// <param name="exception">The exception to classify.</param>
    /// <returns>True if the exception is a connection error; otherwise, false.</returns>
    public static bool IsConnectionError(Exception exception)
    {
        return exception is DatabaseConnectionException;
    }

    /// <summary>
    ///     Determines whether an exception is a mapping error.
    /// </summary>
    /// <param name="exception">The exception to classify.</param>
    /// <returns>True if the exception is a mapping error; otherwise, false.</returns>
    public static bool IsMappingError(Exception exception)
    {
        return exception is DatabaseMappingException;
    }

    /// <summary>
    ///     Determines whether an exception is a constraint violation.
    /// </summary>
    /// <param name="exception">The exception to classify.</param>
    /// <returns>True if the exception is a constraint violation; otherwise, false.</returns>
    public static bool IsConstraintViolation(Exception exception)
    {
        return exception is DatabaseOperationException dbEx &&
               IsConstraintErrorCode(dbEx.ErrorCode);
    }

    /// <summary>
    ///     Determines whether an exception is a syntax error.
    /// </summary>
    /// <param name="exception">The exception to classify.</param>
    /// <returns>True if the exception is a syntax error; otherwise, false.</returns>
    public static bool IsSyntaxError(Exception exception)
    {
        return exception is DatabaseOperationException dbEx &&
               IsSyntaxErrorCode(dbEx.ErrorCode);
    }

    /// <summary>
    ///     Determines whether an error code indicates a transient error.
    ///     Database-specific implementations will override this method.
    /// </summary>
    /// <param name="errorCode">The error code to check.</param>
    /// <returns>True if the error code indicates a transient error; otherwise, false.</returns>
    public static bool IsTransientErrorCode(string? errorCode)
    {
        // Database-specific implementations will override
        return false;
    }

    /// <summary>
    ///     Determines whether an error code indicates a constraint violation.
    ///     Database-specific implementations will override this method.
    /// </summary>
    /// <param name="errorCode">The error code to check.</param>
    /// <returns>True if the error code indicates a constraint violation; otherwise, false.</returns>
    public static bool IsConstraintErrorCode(string? errorCode)
    {
        // Database-specific implementations will override
        return false;
    }

    /// <summary>
    ///     Determines whether an error code indicates a syntax error.
    ///     Database-specific implementations will override this method.
    /// </summary>
    /// <param name="errorCode">The error code to check.</param>
    /// <returns>True if the error code indicates a syntax error; otherwise, false.</returns>
    public static bool IsSyntaxErrorCode(string? errorCode)
    {
        // Database-specific implementations will override
        return false;
    }
}
