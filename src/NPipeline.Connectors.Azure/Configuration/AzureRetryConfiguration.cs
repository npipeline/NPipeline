namespace NPipeline.Connectors.Azure.Configuration;

/// <summary>
///     Common retry configuration for Azure services.
/// </summary>
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

    /// <summary>
    ///     Gets or sets the initial delay for exponential backoff.
    ///     Default is 100 milliseconds.
    /// </summary>
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     Gets or sets the backoff factor for exponential retry delays.
    ///     Default is 2.0 (doubling delay).
    /// </summary>
    public double RetryBackoffFactor { get; set; } = 2.0;

    /// <summary>
    ///     Gets or sets whether to use jitter in retry delays to avoid thundering herd.
    ///     Default is true.
    /// </summary>
    public bool UseJitter { get; set; } = true;
}
