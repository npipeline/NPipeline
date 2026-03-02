namespace NPipeline.StorageProviders.S3;

/// <summary>
///     Base configuration options for S3 storage providers.
///     Contains provider-agnostic settings applicable to both AWS and S3-compatible services.
/// </summary>
public class S3CoreOptions
{
    /// <summary>
    ///     Gets or sets the threshold in bytes for using multipart upload.
    ///     Default is 64 MB.
    /// </summary>
    public long MultipartUploadThresholdBytes { get; set; } = 64 * 1024 * 1024;
}
