using Google.Apis.Auth.OAuth2;

namespace NPipeline.StorageProviders.Gcs;

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
    ///     Gets or sets optional retry settings for GCS operations.
    ///     When set, retry behavior is applied to provider operations.
    /// </summary>
    public GcsRetrySettings? RetrySettings { get; set; }

    /// <summary>
    ///     Validates the options and throws if invalid.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when validation fails.</exception>
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

        RetrySettings?.Validate();
    }
}

/// <summary>
///     Retry settings for Google Cloud Storage operations.
///     Applied to provider operations for transient HTTP failures.
/// </summary>
public sealed class GcsRetrySettings
{
    /// <summary>
    ///     Gets or sets the initial delay before the first retry.
    ///     Default is 1 second.
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets the maximum delay between retries.
    ///     Default is 32 seconds.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(32);

    /// <summary>
    ///     Gets or sets the delay multiplier for exponential backoff.
    ///     Default is 2.0.
    /// </summary>
    public double DelayMultiplier { get; set; } = 2.0;

    /// <summary>
    ///     Gets or sets the maximum number of retry attempts.
    ///     Default is 3. A value of 0 disables retries.
    /// </summary>
    public int MaxAttempts { get; set; } = 3;

    /// <summary>
    ///     Gets or sets whether to retry on rate limit errors (HTTP 429).
    ///     Default is true.
    /// </summary>
    public bool RetryOnRateLimit { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to retry on server errors (HTTP 5xx).
    ///     Default is true.
    /// </summary>
    public bool RetryOnServerErrors { get; set; } = true;

    /// <summary>
    ///     Validates retry settings.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when validation fails.</exception>
    public void Validate()
    {
        if (InitialDelay < TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                $"RetrySettings.InitialDelay must be non-negative. Current value: {InitialDelay}");
        }

        if (MaxDelay < TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                $"RetrySettings.MaxDelay must be non-negative. Current value: {MaxDelay}");
        }

        if (MaxDelay < InitialDelay)
        {
            throw new InvalidOperationException(
                $"RetrySettings.MaxDelay must be greater than or equal to InitialDelay. Current values: MaxDelay={MaxDelay}, InitialDelay={InitialDelay}");
        }

        if (DelayMultiplier < 1.0)
        {
            throw new InvalidOperationException(
                $"RetrySettings.DelayMultiplier must be greater than or equal to 1.0. Current value: {DelayMultiplier}");
        }

        if (MaxAttempts < 0)
        {
            throw new InvalidOperationException(
                $"RetrySettings.MaxAttempts must be non-negative. Current value: {MaxAttempts}");
        }
    }
}
