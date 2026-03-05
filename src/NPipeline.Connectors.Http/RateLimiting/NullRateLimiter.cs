namespace NPipeline.Connectors.Http.RateLimiting;

/// <summary>A no-op <see cref="IRateLimiter" /> that completes immediately without any throttling.</summary>
public sealed class NullRateLimiter : IRateLimiter
{
    /// <summary>Gets the singleton instance.</summary>
    public static readonly NullRateLimiter Instance = new();

    private NullRateLimiter()
    {
    }

    /// <inheritdoc />
    public ValueTask WaitAsync(CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }
}
