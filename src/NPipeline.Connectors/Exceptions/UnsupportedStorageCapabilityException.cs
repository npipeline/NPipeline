namespace NPipeline.Connectors.Exceptions;

public sealed class UnsupportedStorageCapabilityException(
    StorageUri uri,
    string capability,
    string providerName) : ConnectorException($"Provider '{providerName}' does not support '{capability}' for '{uri}'.")
{
    public StorageUri Uri { get; } = uri ?? throw new ArgumentNullException(nameof(uri));
    public string Capability { get; } = capability ?? throw new ArgumentNullException(nameof(capability));
    public string ProviderName { get; } = providerName ?? throw new ArgumentNullException(nameof(providerName));
}
