using Azure.Core;
using Azure.Identity;
using Azure.Storage.Files.DataLake;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     Configuration options for the ADLS Gen2 storage provider.
/// </summary>
public class AdlsGen2StorageProviderOptions
{
    private readonly Lazy<TokenCredential> _defaultCredentialChain = new(() => new DefaultAzureCredential());
    private int _clientCacheSizeLimit = 100;
    private AdlsGen2RetryOptions _retry = new();
    private long _uploadThresholdBytes = 64 * 1024 * 1024;

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
    ///     Gets a cached instance of the default Azure credential chain. Uses DefaultAzureCredential.
    /// </summary>
    public TokenCredential DefaultCredentialChain => _defaultCredentialChain.Value;

    /// <summary>
    ///     Gets or sets the optional service URL for Azure Storage-compatible endpoints
    ///     (e.g., Azurite emulator, local development).
    ///     If not specified, uses the Azure Data Lake Storage endpoint.
    /// </summary>
    public Uri? ServiceUrl { get; set; }

    /// <summary>
    ///     Gets or sets the Data Lake service version to use when creating clients.
    ///     Set this when targeting storage emulators like Azurite.
    /// </summary>
    public DataLakeClientOptions.ServiceVersion? ServiceVersion { get; set; }

    /// <summary>
    ///     Gets or sets the threshold in bytes for using upload.
    ///     Default is 64 MB.
    /// </summary>
    public long UploadThresholdBytes
    {
        get => _uploadThresholdBytes;
        set => _uploadThresholdBytes = value > 0
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Upload threshold must be positive.");
    }

    /// <summary>
    ///     Gets or sets the maximum concurrent upload requests for large files.
    /// </summary>
    public int? UploadMaximumConcurrency { get; set; }

    /// <summary>
    ///     Gets or sets the maximum transfer size in bytes for each upload chunk.
    /// </summary>
    public int? UploadMaximumTransferSizeBytes { get; set; }

    /// <summary>
    ///     Gets or sets the Azure SDK retry settings for the Data Lake and Blob clients. The SDK retries natively;
    ///     defaults are exponential backoff, 5 retries, an 800 ms base delay, an 8 s maximum delay, and a 100 s network
    ///     timeout.
    /// </summary>
    public AdlsGen2RetryOptions Retry
    {
        get => _retry;
        set => _retry = value ?? throw new ArgumentNullException(nameof(value));
    }

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
