namespace NPipeline.StorageProviders.S3;

/// <summary>
///     Base configuration options for S3 storage providers.
///     Contains provider-agnostic settings applicable to both AWS and S3-compatible services.
/// </summary>
public class S3CoreOptions
{
    /// <summary>The smallest part S3 accepts (5 MiB), except for the last one.</summary>
    public const int MinPartSizeBytes = 5 * 1024 * 1024;

    private int _clientCacheSizeLimit = 100;
    private int _maxConcurrency = 4;
    private int _partSizeBytes = 8 * 1024 * 1024;

    /// <summary>
    ///     Gets or sets the size of each upload part in bytes. An object that fits in one part is sent with a single
    ///     <c>PutObject</c>; a larger one is sent as a multipart upload while it is being written. Memory use while writing is
    ///     about <c>PartSizeBytes × (MaxConcurrency + 1)</c>. Must be at least 5 MiB. Default is 8 MiB.
    /// </summary>
    public int PartSizeBytes
    {
        get => _partSizeBytes;
        set => _partSizeBytes = value >= MinPartSizeBytes
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), $"S3 parts must be at least {MinPartSizeBytes} bytes.");
    }

    /// <summary>Gets or sets the most parts of one object that upload at the same time. Default is 4.</summary>
    public int MaxConcurrency
    {
        get => _maxConcurrency;
        set => _maxConcurrency = value > 0
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Maximum concurrency must be positive.");
    }

    /// <summary>Gets or sets the most endpoint clients kept in the cache. Default is 100.</summary>
    public int ClientCacheSizeLimit
    {
        get => _clientCacheSizeLimit;
        set => _clientCacheSizeLimit = value > 0
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Client cache size limit must be positive.");
    }
}
