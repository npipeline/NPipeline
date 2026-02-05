namespace NPipeline.Connectors.Configuration;

/// <summary>
///     Base configuration class for database connectors.
///     Designed to be extended by database-specific configurations.
/// </summary>
public abstract class DatabaseConfigurationBase
{
    /// <summary>
    ///     Gets or sets connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets command timeout in seconds.
    /// </summary>
    public int CommandTimeout { get; set; } = 30;

    /// <summary>
    ///     Gets or sets connection timeout in seconds.
    /// </summary>
    public int ConnectionTimeout { get; set; } = 15;

    /// <summary>
    ///     Gets or sets minimum pool size.
    /// </summary>
    public int MinPoolSize { get; set; } = 1;

    /// <summary>
    ///     Gets or sets maximum pool size.
    /// </summary>
    public int MaxPoolSize { get; set; } = 100;

    /// <summary>
    ///     Gets or sets whether to validate identifiers (SQL injection prevention).
    /// </summary>
    public bool ValidateIdentifiers { get; set; } = true;

    /// <summary>
    ///     Gets or sets delivery semantic.
    /// </summary>
    public DeliverySemantic DeliverySemantic { get; set; } = DeliverySemantic.AtLeastOnce;

    /// <summary>
    ///     Gets or sets checkpoint strategy.
    /// </summary>
    public CheckpointStrategy CheckpointStrategy { get; set; } = CheckpointStrategy.None;

    /// <summary>
    ///     Validates the configuration.
    ///     Virtual method for extensions.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when validation fails.</exception>
    public virtual void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException("ConnectionString is required");

        if (CommandTimeout < 0)
            throw new InvalidOperationException("CommandTimeout must be non-negative");

        if (ConnectionTimeout < 0)
            throw new InvalidOperationException("ConnectionTimeout must be non-negative");

        if (MinPoolSize < 0)
            throw new InvalidOperationException("MinPoolSize must be non-negative");

        if (MaxPoolSize < MinPoolSize)
            throw new InvalidOperationException("MaxPoolSize must be >= MinPoolSize");

        // Validate checkpoint strategy
        if (CheckpointStrategy != CheckpointStrategy.None && CheckpointStrategy != CheckpointStrategy.InMemory)
            throw new InvalidOperationException($"Only {CheckpointStrategy.None} and {CheckpointStrategy.InMemory} checkpoint strategies are supported");
    }
}
