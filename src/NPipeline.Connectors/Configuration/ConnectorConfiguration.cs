namespace NPipeline.Connectors.Configuration;

/// <summary>
///     Root configuration for connector storage providers. This configuration is optional and only
///     used when consumers want to instantiate and register providers without DI.
/// </summary>
public sealed record ConnectorConfiguration
{
    /// <summary>
    ///     Map of provider names to their configs. The key is arbitrary and only used for identification.
    ///     Mutable for DX: callers can add via object initializer or <see cref="AddProvider" />.
    /// </summary>
    public Dictionary<string, StorageProviderConfig> Providers { get; } =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Default scheme to assume when parsing non-URI paths, typically "file".
    /// </summary>
    public string DefaultScheme { get; init; } = "file";

    /// <summary>
    ///     Convenience helper to add a provider entry fluently.
    /// </summary>
    public ConnectorConfiguration AddProvider(string name, StorageProviderConfig config)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(config);

        Providers[name] = config;
        return this;
    }
}
