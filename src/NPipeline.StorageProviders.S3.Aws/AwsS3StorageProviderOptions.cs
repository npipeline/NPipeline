using Amazon;
using Amazon.Runtime;

namespace NPipeline.StorageProviders.S3.Aws;

/// <summary>
///     Configuration options for the AWS S3 storage provider.
///     Extends <see cref="S3CoreOptions" /> with AWS-specific settings.
/// </summary>
public sealed class AwsS3StorageProviderOptions : S3CoreOptions
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
}
