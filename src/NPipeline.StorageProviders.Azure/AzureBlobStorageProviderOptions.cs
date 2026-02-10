using Azure.Core;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Configuration options for the Azure Blob storage provider.
/// </summary>
public class AzureBlobStorageProviderOptions
{
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
    public long BlockBlobUploadThresholdBytes { get; set; } = 64 * 1024 * 1024;

    /// <summary>
    ///     Gets or sets the maximum concurrent upload requests for large blobs.
    /// </summary>
    public int? UploadMaximumConcurrency { get; set; }

    /// <summary>
    ///     Gets or sets the maximum transfer size in bytes for each upload chunk.
    /// </summary>
    public int? UploadMaximumTransferSizeBytes { get; set; }
}
