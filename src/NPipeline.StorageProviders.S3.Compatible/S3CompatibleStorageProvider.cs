using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.S3.Compatible;

/// <summary>
///     S3 storage provider for S3-compatible services (non-AWS).
///     Supports services like MinIO, DigitalOcean Spaces, Cloudflare R2, etc.
/// </summary>
public sealed class S3CompatibleStorageProvider : S3CoreStorageProvider
{
    private readonly IReadOnlyList<StorageScheme> _schemes;

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
        _schemes = options.Schemes.Select(static s => new StorageScheme(s)).ToArray();
    }

    /// <inheritdoc />
    public override string Name => "S3-Compatible";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => _schemes;
}
