namespace NPipeline.Connectors.Configuration;

/// <summary>
///     Root configuration for connector storage providers. This configuration is optional and only
///     used when consumers want to instantiate and register providers without DI.
/// </summary>
public sealed record ConnectorConfiguration
{
    /// <summary>
    ///     Map of provider names to their configs. The key is arbitrary and only used for identification.
    /// </summary>
    public IReadOnlyDictionary<string, StorageProviderConfig> Providers { get; init; }
        = new Dictionary<string, StorageProviderConfig>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Default scheme to assume when parsing non-URI paths, typically "file".
    /// </summary>
    public string DefaultScheme { get; init; } = "file";
}
