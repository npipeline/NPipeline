using Azure.Messaging.ServiceBus;

namespace NPipeline.Connectors.Azure.ServiceBus.Configuration;

/// <summary>
///     Retry settings for Azure Service Bus operations, mapped to <see cref="ServiceBusRetryOptions" />.
/// </summary>
public class ServiceBusRetryConfiguration
{
    /// <summary>
    ///     Gets or sets the retry mode. Defaults to <see cref="ServiceBusRetryMode.Exponential" />.
    /// </summary>
    public ServiceBusRetryMode Mode { get; set; } = ServiceBusRetryMode.Exponential;

    /// <summary>
    ///     Gets or sets the maximum number of retry attempts. Defaults to 3.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    ///     Gets or sets the initial delay between retries. Defaults to 1 second.
    /// </summary>
    public TimeSpan Delay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets the maximum delay between retries. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Gets or sets the timeout per individual attempt. Defaults to 1 minute.
    /// </summary>
    public TimeSpan TryTimeout { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    ///     Converts this configuration to <see cref="ServiceBusRetryOptions" />.
    /// </summary>
    public ServiceBusRetryOptions ToRetryOptions()
    {
        return new ServiceBusRetryOptions
        {
            Mode = Mode,
            MaxRetries = MaxRetries,
            Delay = Delay,
            MaxDelay = MaxDelay,
            TryTimeout = TryTimeout,
        };
    }
}
