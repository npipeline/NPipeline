namespace NPipeline.Connectors.Abstractions;

/// <summary>
///     Optional interface for storage providers to expose capability metadata for validation and diagnostics.
///     This enables connectors to preflight-check read/write support without introducing hard dependencies.
/// </summary>
public interface IStorageProviderMetadataProvider
{
    /// <summary>
    ///     Returns metadata describing the provider's capabilities and supported schemes.
    ///     Implementations should return a stable, inexpensive object (may be cached).
    /// </summary>
    StorageProviderMetadata GetMetadata();
}
