using Amazon.Runtime;
using Amazon.S3;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.S3.Compatible;

/// <summary>
///     Factory for creating Amazon S3 clients configured for S3-compatible services.
///     Uses static credentials and a fixed service URL from options.
/// </summary>
public class S3CompatibleClientFactory : S3ClientFactoryBase
{
    private readonly S3CompatibleStorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="S3CompatibleClientFactory" /> class.
    /// </summary>
    /// <param name="options">The S3-compatible storage provider options.</param>
    public S3CompatibleClientFactory(S3CompatibleStorageProviderOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Creates an Amazon S3 client configured for the S3-compatible endpoint.
    ///     Credentials and endpoint come from options only — per-URI overrides are not supported.
    /// </summary>
    /// <param name="uri">The storage URI (used only for bucket name extraction).</param>
    /// <returns>An <see cref="IAmazonS3" /> client.</returns>
    protected override IAmazonS3 CreateClient(StorageUri uri)
    {
        var credentials = new BasicAWSCredentials(_options.AccessKey, _options.SecretKey);

        var config = new AmazonS3Config
        {
            ServiceURL = _options.ServiceUrl.ToString(),
            ForcePathStyle = _options.ForcePathStyle,
            AuthenticationRegion = _options.SigningRegion,
        };

        return new AmazonS3Client(credentials, config);
    }

    /// <summary>
    ///     Builds a cache key for the client. Since all clients use the same configuration,
    ///     this returns a constant key.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>A cache key string.</returns>
    protected override string BuildCacheKey(StorageUri uri)
    {
        // All clients use the same configuration, so return a constant key
        return $"compatible|{_options.ServiceUrl}|{_options.ForcePathStyle}";
    }
}
