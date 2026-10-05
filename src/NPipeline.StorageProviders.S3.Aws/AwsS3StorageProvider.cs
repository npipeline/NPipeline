using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.S3.Aws;

/// <summary>
///     S3 storage provider pre-configured for AWS (IAM credential chain, region-based endpoints).
/// </summary>
public class AwsS3StorageProvider : S3CoreStorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> SchemeList = [StorageScheme.S3];

    /// <summary>
    ///     Initializes a new instance of the <see cref="AwsS3StorageProvider" /> class.
    /// </summary>
    /// <param name="factory">The AWS S3 client factory.</param>
    /// <param name="options">The AWS S3 storage provider options.</param>
    public AwsS3StorageProvider(AwsS3ClientFactory factory, AwsS3StorageProviderOptions options)
        : base(factory, options)
    {
    }

    /// <inheritdoc />
    public override string Name => "AWS S3";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => SchemeList;

    /// <summary>AWS S3 honours <c>If-Match</c> and <c>If-None-Match</c> on writes; S3-compatible services are not assumed to.</summary>
    public override StorageCapabilities Capabilities => base.Capabilities | StorageCapabilities.ConditionalWrite;
}
