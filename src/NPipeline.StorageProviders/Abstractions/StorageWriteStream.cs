namespace NPipeline.StorageProviders.Abstractions;

/// <summary>A write-only stream whose bytes become visible at the target only when committed.</summary>
public abstract class StorageWriteStream : Stream
{
    /// <summary>The committed object's ETag or generation, when the store reports one. Set by <see cref="CommitAsync" />.</summary>
    public string? ETag { get; protected set; }

    /// <summary>
    ///     Makes the written bytes visible at the target. Call it once, after the last write.
    ///     Disposing a stream that was not committed discards what was written, for providers that implement explicit commit.
    /// </summary>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    public abstract Task CommitAsync(CancellationToken cancellationToken = default);
}
