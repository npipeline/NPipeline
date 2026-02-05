using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.Configuration;

/// <summary>
///     Configuration model for a storage provider instance.
/// </summary>
public sealed record StorageProviderConfig
{
    /// <summary>
    ///     Type name of the provider to instantiate.
    ///     Accepts either an assembly-qualified name or a full type name discoverable from loaded assemblies.
    ///     Example: "NPipeline.StorageProviders.FileSystemStorageProvider"
    /// </summary>
    public required string ProviderType { get; init; }

    /// <summary>
    ///     Arbitrary settings passed to the provider if it supports configuration.
    ///     Providers can opt-in by implementing <see cref="IConfigurableStorageProvider" />.
    /// </summary>
    public IReadOnlyDictionary<string, object> Settings { get; init; } =
        new Dictionary<string, object>();

    /// <summary>
    ///     Whether this provider entry should be instantiated and registered.
    /// </summary>
    public bool Enabled { get; init; } = true;
}
