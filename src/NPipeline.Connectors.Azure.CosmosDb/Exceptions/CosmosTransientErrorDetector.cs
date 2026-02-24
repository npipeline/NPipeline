using System.Net;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.Exceptions;

namespace NPipeline.Connectors.Azure.CosmosDb.Exceptions;

/// <summary>
///     Detects transient errors that should be retried for Cosmos DB operations.
///     Extends the base <see cref="Azure.Exceptions.AzureTransientErrorDetector" /> with Cosmos-specific error detection.
/// </summary>
public class CosmosTransientErrorDetector : AzureTransientErrorDetector
{
    /// <summary>
    ///     Singleton instance for convenience.
    /// </summary>
    public static readonly CosmosTransientErrorDetector Instance = new();

    /// <summary>
    ///     Determines whether an exception represents a transient error that should be retried.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the exception is transient; otherwise false.</returns>
    public override bool IsTransient(Exception? exception)
    {
        if (exception == null)
            return false;

        // Unwrap aggregate exceptions
        if (exception is AggregateException aggregateException)
            return aggregateException.InnerExceptions.Any(IsTransient);

        // Check inner exception recursively
        if (exception.InnerException != null && IsTransient(exception.InnerException))
            return true;

        return exception switch
        {
            CosmosException cosmosEx => IsTransientCosmosException(cosmosEx),
            _ => base.IsTransient(exception),
        };
    }

    /// <summary>
    ///     Determines whether an exception represents a rate-limiting error (429).
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the exception is a rate-limiting error; otherwise false.</returns>
    public override bool IsRateLimited(Exception? exception)
    {
        if (exception is CosmosException cosmosEx)
            return cosmosEx.StatusCode == HttpStatusCode.TooManyRequests;

        return base.IsRateLimited(exception);
    }

    /// <summary>
    ///     Gets the suggested retry delay from a Cosmos DB exception, if available.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>The suggested retry delay, or null if not available.</returns>
    public override TimeSpan? GetRetryDelay(Exception? exception)
    {
        if (exception is CosmosException cosmosEx)
            return cosmosEx.RetryAfter;

        return base.GetRetryDelay(exception);
    }

    /// <summary>
    ///     Gets the activity ID from a Cosmos DB exception for correlation.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>The activity ID, or null if not available.</returns>
    public override string? GetCorrelationId(Exception? exception)
    {
        if (exception is CosmosException cosmosEx)
            return cosmosEx.ActivityId;

        return base.GetCorrelationId(exception);
    }

    /// <summary>
    ///     Checks if a Cosmos exception represents a transient error.
    /// </summary>
    private static bool IsTransientCosmosException(CosmosException exception)
    {
        // Check status code
        if (IsTransientStatus((int)exception.StatusCode))
            return true;

        // Check specific sub-status codes
        // 3200: Request entity too large - not transient
        // 4100-4199: Various transient partition failures
        var subStatusCode = exception.SubStatusCode;

        if (subStatusCode is >= 4100 and <= 4199)
            return true;

        return false;
    }
}

/// <summary>
///     Static helper methods for Cosmos DB transient error detection.
///     Provides backward compatibility with the previous static API.
/// </summary>
public static class CosmosTransientErrorDetectorExtensions
{
    /// <summary>
    ///     Determines whether an exception represents a transient error that should be retried.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the exception is transient; otherwise false.</returns>
    public static bool IsTransient(Exception? exception)
    {
        return CosmosTransientErrorDetector.Instance.IsTransient(exception);
    }

    /// <summary>
    ///     Determines whether a Cosmos DB exception represents a rate-limiting error (429).
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the exception is a rate-limiting error; otherwise false.</returns>
    public static bool IsRateLimited(Exception? exception)
    {
        return CosmosTransientErrorDetector.Instance.IsRateLimited(exception);
    }

    /// <summary>
    ///     Gets the suggested retry delay from a Cosmos DB exception, if available.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>The suggested retry delay, or null if not available.</returns>
    public static TimeSpan? GetRetryDelay(Exception? exception)
    {
        return CosmosTransientErrorDetector.Instance.GetRetryDelay(exception);
    }

    /// <summary>
    ///     Gets the activity ID from a Cosmos DB exception for correlation.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>The activity ID, or null if not available.</returns>
    public static string? GetActivityId(Exception? exception)
    {
        return CosmosTransientErrorDetector.Instance.GetCorrelationId(exception);
    }
}
