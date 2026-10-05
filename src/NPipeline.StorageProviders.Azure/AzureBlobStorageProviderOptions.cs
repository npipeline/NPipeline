using Azure.Storage.Blobs;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Configuration options for the Azure Blob storage provider. Authentication, endpoint, retry and upload settings are
///     inherited from <see cref="AzureAccountOptions" />.
/// </summary>
public class AzureBlobStorageProviderOptions : AzureAccountOptions
{
    /// <summary>
    ///     Gets or sets the Blob service version to use when creating clients.
    ///     Set this when targeting storage emulators like Azurite.
    /// </summary>
    public BlobClientOptions.ServiceVersion? ServiceVersion { get; set; }
}
