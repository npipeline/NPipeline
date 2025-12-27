namespace NPipeline.Connectors.Exceptions;

/// <summary>
///     Exception thrown when a storage provider does not support a specific capability for a given URI.
/// </summary>
/// <remarks>
///     This exception is used to indicate that a requested operation cannot be performed because
///     a storage provider lacks the necessary capability for the specified storage resource.
/// </remarks>
/// <param name="uri">The storage URI for which the capability is not supported.</param>
/// <param name="capability">The capability that is not supported.</param>
/// <param name="providerName">The name of the storage provider.</param>
public sealed class UnsupportedStorageCapabilityException(
    StorageUri uri,
    string capability,
    string providerName) : ConnectorException($"Provider '{providerName}' does not support '{capability}' for '{uri}'.")
{
    /// <summary>
    ///     Gets the storage URI associated with the unsupported capability.
    /// </summary>
    public StorageUri Uri { get; } = uri ?? throw new ArgumentNullException(nameof(uri));

    /// <summary>
    ///     Gets the capability that is not supported by the provider.
    /// </summary>
    public string Capability { get; } = capability ?? throw new ArgumentNullException(nameof(capability));

    /// <summary>
    ///     Gets the name of the storage provider that lacks the capability.
    /// </summary>
    public string ProviderName { get; } = providerName ?? throw new ArgumentNullException(nameof(providerName));
}
