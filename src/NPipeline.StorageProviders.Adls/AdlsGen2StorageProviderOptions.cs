using Azure.Storage.Files.DataLake;
using NPipeline.StorageProviders.Azure;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     Configuration options for the ADLS Gen2 storage provider. Authentication, endpoint, retry and upload settings are
///     inherited from <see cref="AzureAccountOptions" />.
/// </summary>
public class AdlsGen2StorageProviderOptions : AzureAccountOptions
{
    /// <summary>
    ///     Gets or sets the Data Lake service version to use when creating clients.
    ///     Set this when targeting storage emulators like Azurite.
    /// </summary>
    public DataLakeClientOptions.ServiceVersion? ServiceVersion { get; set; }
}
