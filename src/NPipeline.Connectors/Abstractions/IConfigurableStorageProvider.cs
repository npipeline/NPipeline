namespace NPipeline.Connectors.Abstractions;

/// <summary>
///     Optional interface for storage providers that support runtime configuration via key/value settings.
///     Providers that implement this can be configured by <see cref="Configuration.StorageProviderConfig" /> through
///     <see cref="StorageProviderFactory" /> without introducing DI dependencies.
/// </summary>
public interface IConfigurableStorageProvider
{
    /// <summary>
    ///     Applies configuration settings to the provider instance.
    ///     Implementations should validate and normalize settings, throwing <see cref="System.ArgumentException" />
    ///     with a clear message when values are invalid.
    /// </summary>
    /// <param name="settings">Arbitrary key/value settings specific to the provider.</param>
    void Configure(IReadOnlyDictionary<string, object> settings);
}
