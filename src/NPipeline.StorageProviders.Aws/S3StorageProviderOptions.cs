using Amazon;
using Amazon.Runtime;

namespace NPipeline.StorageProviders.Aws;

/// <summary>
///     Configuration options for the S3 storage provider.
/// </summary>
public class S3StorageProviderOptions
{
    /// <summary>
    ///     Gets or sets the default AWS region endpoint.
    ///     If not specified, defaults to US East 1.
    /// </summary>
    public RegionEndpoint? DefaultRegion { get; set; }

    /// <summary>
    ///     Gets or sets the default AWS credentials.
    ///     If not specified, the default AWS credential chain is used.
    /// </summary>
    public AWSCredentials? DefaultCredentials { get; set; }

    /// <summary>
    ///     Gets or sets whether to use the default AWS credential chain.
    ///     Default is true.
    /// </summary>
    public bool UseDefaultCredentialChain { get; set; } = true;

    /// <summary>
    ///     Gets or sets the optional service URL for S3-compatible endpoints
    ///     (e.g., MinIO, LocalStack).
    ///     If not specified, uses the AWS S3 endpoint.
    /// </summary>
    public Uri? ServiceUrl { get; set; }

    /// <summary>
    ///     Gets or sets whether to force path-style addressing.
    ///     Default is false (virtual-hosted-style addressing).
    ///     Path-style addressing is required for some S3-compatible services.
    /// </summary>
    public bool ForcePathStyle { get; set; }

    /// <summary>
    ///     Gets or sets the threshold in bytes for using multipart upload.
    ///     Default is 64 MB.
    /// </summary>
    public long MultipartUploadThresholdBytes { get; set; } = 64 * 1024 * 1024;
}
