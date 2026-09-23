using Google.Apis.Auth.OAuth2;
using NPipeline.StorageProviders.Gcp.Reliability;

namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     Configuration options for the Google Cloud Storage provider.
/// </summary>
public sealed class GcsStorageProviderOptions
{
    /// <summary>
    ///     Gets or sets the default Google Cloud project ID.
    ///     Optional - used when bucket operations require project context.
    /// </summary>
    public string? DefaultProjectId { get; set; }

    /// <summary>
    ///     Gets or sets the default Google credentials for authentication.
    ///     If not specified and <see cref="UseDefaultCredentials" /> is true,
    ///     Application Default Credentials (ADC) will be used.
    /// </summary>
    public GoogleCredential? DefaultCredentials { get; set; }

    /// <summary>
    ///     Gets or sets whether to use Application Default Credentials (ADC) when
    ///     <see cref="DefaultCredentials" /> is not provided.
    ///     Default is true.
    /// </summary>
    public bool UseDefaultCredentials { get; set; } = true;

    /// <summary>
    ///     Gets or sets the optional service URL for GCS emulator or custom endpoints.
    ///     If not specified, uses the default Google Cloud Storage endpoint.
    /// </summary>
    public Uri? ServiceUrl { get; set; }

    /// <summary>
    ///     Gets or sets the chunk size in bytes for resumable uploads.
    ///     Must be positive and a multiple of 256 KiB (262,144 bytes).
    ///     Default is 16 MB.
    /// </summary>
    public int UploadChunkSizeBytes { get; set; } = 16 * 1024 * 1024;

    /// <summary>
    ///     Gets or sets the buffer threshold in bytes for switching upload strategies.
    ///     Reserved for future use.
    ///     Default is 64 MB.
    /// </summary>
    public long UploadBufferThresholdBytes { get; set; } = 64 * 1024 * 1024;

    /// <summary>
    ///     Gets or sets the maximum number of cached StorageClient instances.
    ///     Must be positive.
    ///     Default is 100.
    /// </summary>
    public int ClientCacheSizeLimit { get; set; } = 100;

    /// <summary>
    ///     Gets or sets how each GCS request is retried. Defaults to <see cref="GcsStorageResilience.Default" />: three
    ///     attempts with jittered exponential backoff from one second up to 32 seconds, retrying 408, 429 (honoring
    ///     <c>Retry-After</c>), 5xx, and network failures. Use <see cref="NResilience.Resilience.None" /> to turn retries
    ///     off.
    /// </summary>
    /// <remarks>
    ///     This is the only retry layer: clients built by <see cref="GcsClientFactory" /> have the Google SDK's own retry
    ///     turned off. A custom factory that builds its own clients should set
    ///     <c>client.Service.HttpClient.MessageHandler.NumTries = 1</c> to keep it that way.
    /// </remarks>
    public NResilience.Resilience Resilience { get; set; } = GcsStorageResilience.Default;

    /// <summary>
    ///     Validates the options and throws if invalid.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when validation fails.</exception>
    /// <exception cref="NResilience.ResilienceConfigurationException">Thrown when <see cref="Resilience" /> is impossible.</exception>
    public void Validate()
    {
        const int kiB256 = 256 * 1024;

        if (UploadChunkSizeBytes <= 0)
        {
            throw new InvalidOperationException(
                $"UploadChunkSizeBytes must be positive. Current value: {UploadChunkSizeBytes}");
        }

        if (UploadChunkSizeBytes % kiB256 != 0)
        {
            throw new InvalidOperationException(
                $"UploadChunkSizeBytes must be a multiple of 256 KiB ({kiB256} bytes). Current value: {UploadChunkSizeBytes}");
        }

        if (ClientCacheSizeLimit <= 0)
        {
            throw new InvalidOperationException(
                $"ClientCacheSizeLimit must be positive. Current value: {ClientCacheSizeLimit}");
        }

        ArgumentNullException.ThrowIfNull(Resilience);
        Resilience.Validate();
    }
}
