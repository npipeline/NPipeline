namespace NPipeline.StorageProviders.S3.Compatible;

/// <summary>
///     Configuration options for the S3-compatible storage provider.
///     Designed for non-AWS S3-compatible services like MinIO, DigitalOcean Spaces, Cloudflare R2, etc.
/// </summary>
public sealed class S3CompatibleStorageProviderOptions : S3CoreOptions
{
    /// <summary>
    ///     Gets or initializes the base URL of the S3-compatible endpoint.
    ///     Example: "https://nyc3.digitaloceanspaces.com"
    /// </summary>
    public required Uri ServiceUrl { get; init; }

    /// <summary>
    ///     Gets or initializes the static access key (equivalent to AWS access key ID).
    /// </summary>
    public required string AccessKey { get; init; }

    /// <summary>
    ///     Gets or initializes the static secret key (equivalent to AWS secret access key).
    /// </summary>
    public required string SecretKey { get; init; }

    /// <summary>
    ///     Gets or initializes the optional region string used only for request signing.
    ///     Defaults to "us-east-1" when not specified, which most providers accept.
    /// </summary>
    /// <remarks>
    ///     Some providers like Cloudflare R2 require this to be set to "auto".
    /// </remarks>
    public string SigningRegion { get; init; } = "us-east-1";

    /// <summary>
    ///     Gets or initializes whether to use path-style addressing instead of virtual-hosted-style.
    ///     Default is true — required by most S3-compatible services.
    /// </summary>
    public bool ForcePathStyle { get; init; } = true;
}
