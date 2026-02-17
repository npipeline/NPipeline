using Confluent.SchemaRegistry;

namespace NPipeline.Connectors.Kafka.Configuration;

/// <summary>
///     Configuration for Confluent Schema Registry integration.
/// </summary>
public sealed class SchemaRegistryConfiguration
{
    /// <summary>
    ///     Gets or sets the Schema Registry URL.
    /// </summary>
    public string Url { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the basic auth credentials username.
    /// </summary>
    public string? BasicAuthUsername { get; set; }

    /// <summary>
    ///     Gets or sets the basic auth credentials password.
    /// </summary>
    public string? BasicAuthPassword { get; set; }

    /// <summary>
    ///     Gets or sets whether to use TLS for connections.
    /// </summary>
    public bool EnableSsl { get; set; }

    /// <summary>
    ///     Gets or sets the timeout for schema operations in milliseconds.
    /// </summary>
    public int RequestTimeoutMs { get; set; } = 30000;

    /// <summary>
    ///     Gets or sets the maximum number of schemas to cache locally.
    /// </summary>
    public int SchemaCacheCapacity { get; set; } = 1000;

    /// <summary>
    ///     Gets or sets whether to automatically register schemas.
    /// </summary>
    public bool AutoRegisterSchemas { get; set; } = true;

    /// <summary>
    ///     Gets or sets the subject name strategy for schema registration.
    /// </summary>
    public SubjectNameStrategy? SubjectNameStrategy { get; set; }

    /// <summary>
    ///     Validates the Schema Registry configuration.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when configuration is invalid.</exception>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Url))
            throw new InvalidOperationException("Schema Registry URL is required.");

        if (RequestTimeoutMs <= 0)
            throw new InvalidOperationException("RequestTimeoutMs must be greater than zero.");

        if (SchemaCacheCapacity <= 0)
            throw new InvalidOperationException("SchemaCacheCapacity must be greater than zero.");
    }
}
