namespace NPipeline.Connectors.Azure.Configuration;

/// <summary>
///     Retry configuration for Azure services whose SDK retries natively.
/// </summary>
/// <remarks>
///     These settings are passed to the Azure SDK, which owns the retry (for Cosmos DB, the SDK's rate-limited
///     (429) retry: <c>MaxRetryAttemptsOnRateLimitedRequests</c> and <c>MaxRetryWaitTimeOnRateLimitedRequests</c>).
///     The SDK chooses the delays, honoring the service's retry-after hints. NPipeline adds no retry layer of its own.
/// </remarks>
public class AzureRetryConfiguration
{
    /// <summary>
    ///     Gets or sets the maximum number of retry attempts for transient errors.
    ///     Default is 9 retries.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 9;

    /// <summary>
    ///     Gets or sets the maximum total time to wait for retries.
    ///     Default is 30 seconds.
    /// </summary>
    public TimeSpan MaxRetryWaitTime { get; set; } = TimeSpan.FromSeconds(30);
}
