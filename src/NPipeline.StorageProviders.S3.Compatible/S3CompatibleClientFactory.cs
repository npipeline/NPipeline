using Amazon.Runtime;
using Amazon.S3;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.S3.Compatible;

/// <summary>
///     Factory for creating Amazon S3 clients configured for S3-compatible services.
///     Uses static credentials and a fixed service URL from options, so there is one client for the provider.
/// </summary>
public class S3CompatibleClientFactory : S3ClientFactoryBase
{
    private readonly S3CompatibleStorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="S3CompatibleClientFactory" /> class.
    /// </summary>
    /// <param name="options">The S3-compatible storage provider options.</param>
    public S3CompatibleClientFactory(S3CompatibleStorageProviderOptions options)
        : base(1)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Returns the one endpoint of the provider. Credentials and endpoint come from options only - per-URI overrides are
    ///     not supported, and the URI is used only for its bucket name.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The endpoint key.</returns>
    protected override S3EndpointKey GetEndpoint(StorageUri uri) =>
        new(_options.SigningRegion, _options.ServiceUrl.AbsoluteUri, _options.ForcePathStyle);

    /// <summary>
    ///     Creates an Amazon S3 client configured for the S3-compatible endpoint.
    /// </summary>
    /// <param name="endpoint">The endpoint.</param>
    /// <returns>An <see cref="IAmazonS3" /> client.</returns>
    protected override IAmazonS3 CreateClient(S3EndpointKey endpoint)
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
}
