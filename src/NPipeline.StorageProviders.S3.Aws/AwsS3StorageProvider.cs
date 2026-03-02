using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.S3.Aws;

/// <summary>
///     S3 storage provider pre-configured for AWS (IAM credential chain, region-based endpoints).
/// </summary>
public class AwsS3StorageProvider : S3CoreStorageProvider
{
    private readonly AwsS3StorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AwsS3StorageProvider" /> class.
    /// </summary>
    /// <param name="factory">The AWS S3 client factory.</param>
    /// <param name="options">The AWS S3 storage provider options.</param>
    public AwsS3StorageProvider(AwsS3ClientFactory factory, AwsS3StorageProviderOptions options)
        : base(factory, options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Builds the provider metadata with AWS-specific information.
    /// </summary>
    /// <returns>A <see cref="StorageProviderMetadata" /> object.</returns>
    protected override StorageProviderMetadata BuildMetadata()
    {
        return new StorageProviderMetadata
        {
            Name = "AWS S3",
            SupportedSchemes = ["s3"],
            SupportsRead = true,
            SupportsWrite = true,
            SupportsListing = true,
            SupportsMetadata = true,
            SupportsHierarchy = false,
            Capabilities = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                ["multipartUploadThresholdBytes"] = _options.MultipartUploadThresholdBytes,
                ["supportsPathStyle"] = true,
                ["supportsServiceUrl"] = true,
            },
        };
    }
}
