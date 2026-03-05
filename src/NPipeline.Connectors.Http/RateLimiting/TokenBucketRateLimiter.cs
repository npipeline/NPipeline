using SystemRateLimiting = System.Threading.RateLimiting;

namespace NPipeline.Connectors.Http.RateLimiting;

/// <summary>Options for <see cref="TokenBucketRateLimiter" />.</summary>
public sealed record TokenBucketRateLimiterOptions
{
    /// <summary>Number of tokens replenished per <see cref="Period" />. Defaults to <c>10</c>.</summary>
    public int TokensPerPeriod { get; init; } = 10;

    /// <summary>Replenishment period. Defaults to one second.</summary>
    public TimeSpan Period { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Maximum token capacity. Defaults to <see cref="TokensPerPeriod" />.</summary>
    public int? BucketCapacity { get; init; }
}

/// <summary>
///     A <see cref="IRateLimiter" /> wrapping .NET's built-in <see cref="System.Threading.RateLimiting.TokenBucketRateLimiter" />
///     for battle-tested token-bucket throttling with full <see cref="CancellationToken" /> support.
/// </summary>
public sealed class TokenBucketRateLimiter : IRateLimiter, IAsyncDisposable
{
    private readonly SystemRateLimiting.TokenBucketRateLimiter _inner;

    /// <summary>Creates a new instance with the specified options.</summary>
    public TokenBucketRateLimiter(TokenBucketRateLimiterOptions? options = null)
    {
        var o = options ?? new TokenBucketRateLimiterOptions();

        _inner = new SystemRateLimiting.TokenBucketRateLimiter(
            new SystemRateLimiting.TokenBucketRateLimiterOptions
            {
                TokenLimit = o.BucketCapacity ?? o.TokensPerPeriod,
                TokensPerPeriod = o.TokensPerPeriod,
                ReplenishmentPeriod = o.Period,
                AutoReplenishment = true,
                QueueProcessingOrder = SystemRateLimiting.QueueProcessingOrder.OldestFirst,
                QueueLimit = int.MaxValue,
            });
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        return _inner.DisposeAsync();
    }

    /// <inheritdoc />
    public async ValueTask WaitAsync(CancellationToken cancellationToken = default)
    {
        using var lease = await _inner.AcquireAsync(1, cancellationToken).ConfigureAwait(false);

        if (!lease.IsAcquired)
            throw new OperationCanceledException("Rate limiter rejected the request.", cancellationToken);
    }
}
