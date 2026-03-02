using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.S3.Compatible;

/// <summary>
///     S3 storage provider for S3-compatible services (non-AWS).
///     Supports services like MinIO, DigitalOcean Spaces, Cloudflare R2, etc.
/// </summary>
public sealed class S3CompatibleStorageProvider : S3CoreStorageProvider
{
    private readonly S3CompatibleStorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="S3CompatibleStorageProvider" /> class.
    /// </summary>
    /// <param name="factory">The S3-compatible client factory.</param>
    /// <param name="options">The S3-compatible storage provider options.</param>
    public S3CompatibleStorageProvider(
        S3CompatibleClientFactory factory,
        S3CompatibleStorageProviderOptions options)
        : base(factory, options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Builds the provider metadata with S3-compatible-specific information.
    /// </summary>
    /// <returns>A <see cref="StorageProviderMetadata" /> object.</returns>
    protected override StorageProviderMetadata BuildMetadata()
    {
        return new StorageProviderMetadata
        {
            Name = "S3-Compatible",
            SupportedSchemes = ["s3"],
            SupportsRead = true,
            SupportsWrite = true,
            SupportsListing = true,
            SupportsMetadata = true,
            SupportsHierarchy = false,
            Capabilities = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                ["endpoint"] = _options.ServiceUrl.ToString(),
                ["forcePathStyle"] = _options.ForcePathStyle,
                ["multipartUploadThresholdBytes"] = _options.MultipartUploadThresholdBytes,
                ["signingRegion"] = _options.SigningRegion,
            },
        };
    }
}
