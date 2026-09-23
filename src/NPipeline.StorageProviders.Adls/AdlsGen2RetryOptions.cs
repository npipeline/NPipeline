using Azure.Core;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     Retry settings for the ADLS Gen2 storage provider, applied to the Azure SDK's <see cref="RetryOptions" /> on both
///     the Data Lake and Blob clients the provider creates.
/// </summary>
/// <remarks>
///     The Azure SDK owns the retry: it retries throttling (429), server errors (5xx), request timeouts, and network
///     failures, and honors the service's <c>Retry-After</c> hints. NPipeline adds no retry layer on top.
/// </remarks>
public sealed class AdlsGen2RetryOptions
{
    private TimeSpan _delay = TimeSpan.FromMilliseconds(800);
    private TimeSpan _maxDelay = TimeSpan.FromSeconds(8);
    private int _maxRetries = 5;
    private TimeSpan _networkTimeout = TimeSpan.FromSeconds(100);

    /// <summary>
    ///     Gets or sets how the delay between retries grows. Default is <see cref="RetryMode.Exponential" />.
    /// </summary>
    public RetryMode Mode { get; set; } = RetryMode.Exponential;

    /// <summary>
    ///     Gets or sets the maximum number of retries after the first attempt. Default is 5. Set to 0 to disable retries.
    /// </summary>
    public int MaxRetries
    {
        get => _maxRetries;
        set => _maxRetries = value >= 0
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Max retries cannot be negative.");
    }

    /// <summary>
    ///     Gets or sets the delay before the first retry, which is the base for exponential backoff. Default is
    ///     800 milliseconds.
    /// </summary>
    public TimeSpan Delay
    {
        get => _delay;
        set => _delay = value >= TimeSpan.Zero
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Retry delay cannot be negative.");
    }

    /// <summary>
    ///     Gets or sets the maximum delay between retries. Default is 8 seconds.
    /// </summary>
    public TimeSpan MaxDelay
    {
        get => _maxDelay;
        set => _maxDelay = value >= TimeSpan.Zero
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Maximum retry delay cannot be negative.");
    }

    /// <summary>
    ///     Gets or sets the timeout for each individual network operation. Default is 100 seconds.
    /// </summary>
    public TimeSpan NetworkTimeout
    {
        get => _networkTimeout;
        set => _networkTimeout = value > TimeSpan.Zero || value == Timeout.InfiniteTimeSpan
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Network timeout must be positive or infinite.");
    }

    /// <summary>
    ///     Copies these settings onto an Azure SDK <see cref="RetryOptions" />.
    /// </summary>
    /// <param name="retry">The SDK retry options to configure.</param>
    internal void ApplyTo(RetryOptions retry)
    {
        retry.Mode = Mode;
        retry.MaxRetries = MaxRetries;
        retry.Delay = Delay;
        retry.MaxDelay = MaxDelay;
        retry.NetworkTimeout = NetworkTimeout;
    }
}
