using Azure.Core;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Configuration options for the Azure Blob storage provider.
/// </summary>
public class AzureBlobStorageProviderOptions
{
    private long _blockBlobUploadThresholdBytes = 64 * 1024 * 1024;
    private int _clientCacheSizeLimit = 100;

    /// <summary>
    ///     Gets or sets the default Azure credential for authentication.
    ///     If not specified, uses DefaultAzureCredential chain when UseDefaultCredentialChain is true.
    /// </summary>
    public TokenCredential? DefaultCredential { get; set; }

    /// <summary>
    ///     Gets or sets the default connection string for Azure Storage.
    ///     Takes precedence over DefaultCredential if specified.
    /// </summary>
    public string? DefaultConnectionString { get; set; }

    /// <summary>
    ///     Gets or sets whether to use the default Azure credential chain.
    ///     Default is true.
    /// </summary>
    public bool UseDefaultCredentialChain { get; set; } = true;

    /// <summary>
    ///     Gets or sets the optional service URL for Azure Storage-compatible endpoints
    ///     (e.g., Azurite emulator, local development).
    ///     If not specified, uses the Azure Blob Storage endpoint.
    /// </summary>
    public Uri? ServiceUrl { get; set; }

    /// <summary>
    ///     Gets or sets the threshold in bytes for using block blob upload.
    ///     Default is 64 MB.
    /// </summary>
    public long BlockBlobUploadThresholdBytes
    {
        get => _blockBlobUploadThresholdBytes;
        set => _blockBlobUploadThresholdBytes = value > 0
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Block blob upload threshold must be positive.");
    }

    /// <summary>
    ///     Gets or sets the maximum concurrent upload requests for large blobs.
    /// </summary>
    public int? UploadMaximumConcurrency { get; set; }

    /// <summary>
    ///     Gets or sets the maximum transfer size in bytes for each upload chunk.
    /// </summary>
    public int? UploadMaximumTransferSizeBytes { get; set; }

    /// <summary>
    ///     Gets or sets the maximum number of cached clients before eviction occurs.
    ///     Default is 100. Set to a positive value to enable eviction.
    /// </summary>
    public int ClientCacheSizeLimit
    {
        get => _clientCacheSizeLimit;
        set => _clientCacheSizeLimit = value > 0
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Client cache size limit must be positive.");
    }
}
