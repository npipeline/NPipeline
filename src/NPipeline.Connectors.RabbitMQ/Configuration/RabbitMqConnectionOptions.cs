namespace NPipeline.Connectors.RabbitMQ.Configuration;

/// <summary>
///     Shared connection settings for RabbitMQ.
/// </summary>
public sealed record RabbitMqConnectionOptions
{
    /// <summary>
    ///     Gets or sets the RabbitMQ server hostname. Default is "localhost".
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    ///     Gets or sets the AMQP port. Default is 5672.
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    ///     Gets or sets the virtual host. Default is "/".
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    ///     Gets or sets the username. Default is "guest".
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    ///     Gets or sets the password. Default is "guest".
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    ///     Gets or sets an AMQP URI that overrides individual connection properties.
    ///     Example: amqp://user:pass@host:port/vhost
    /// </summary>
    public Uri? Uri { get; set; }

    /// <summary>
    ///     Gets or sets TLS configuration. Null disables TLS.
    /// </summary>
    public RabbitMqTlsOptions? Tls { get; set; }

    /// <summary>
    ///     Gets or sets the heartbeat interval. Default is 60 seconds.
    /// </summary>
    public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    ///     Gets or sets the network recovery interval. Default is 5 seconds.
    /// </summary>
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     Gets or sets whether automatic connection recovery is enabled. Default is true.
    /// </summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether topology recovery is enabled. Default is true.
    /// </summary>
    public bool TopologyRecoveryEnabled { get; set; } = true;

    /// <summary>
    ///     Gets or sets the client-provided connection name (visible in RabbitMQ management UI).
    /// </summary>
    public string? ClientProvidedName { get; set; }

    /// <summary>
    ///     Gets or sets the maximum size of the channel pool for publisher channels. Default is 4.
    /// </summary>
    public int MaxChannelPoolSize { get; set; } = 4;

    /// <summary>
    ///     Validates the connection options and throws if invalid.
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(HostName) && Uri is null)
            throw new InvalidOperationException("HostName must not be empty when Uri is not set.");

        if (Port is < 1 or > 65535)
            throw new InvalidOperationException("Port must be between 1 and 65535.");

        if (MaxChannelPoolSize < 1)
            throw new InvalidOperationException("MaxChannelPoolSize must be at least 1.");

        if (RequestedHeartbeat < TimeSpan.Zero)
            throw new InvalidOperationException("RequestedHeartbeat must be non-negative.");

        if (NetworkRecoveryInterval < TimeSpan.Zero)
            throw new InvalidOperationException("NetworkRecoveryInterval must be non-negative.");
    }
}
