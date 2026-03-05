using NPipeline.Connectors.Http.RateLimiting;

namespace NPipeline.Connectors.Http.Tests.RateLimiting;

public class TokenBucketRateLimiterTests
{
    [Fact]
    public async Task WaitAsync_WhenBucketHasTokens_CompletesImmediately()
    {
        await using var limiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
        {
            TokensPerPeriod = 10,
            BucketCapacity = 10,
            Period = TimeSpan.FromSeconds(60),
        });

        // Should complete quickly without blocking
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var act = async () => await limiter.WaitAsync(cts.Token);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task WaitAsync_WhenCancelled_ThrowsOperationCanceledException()
    {
        await using var limiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
        {
            TokensPerPeriod = 1,
            BucketCapacity = 1,
            Period = TimeSpan.FromSeconds(60),
        });

        // Drain the bucket
        await limiter.WaitAsync();

        // Next call should block — cancel immediately
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var act = async () => await limiter.WaitAsync(cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public void NullRateLimiter_ReturnsCompletedTaskWithoutAllocation()
    {
        var valueTask = NullRateLimiter.Instance.WaitAsync();

        valueTask.IsCompletedSuccessfully.Should().BeTrue();
    }

    [Fact]
    public async Task NullRateLimiter_DoesNotThrow_WithCancellation()
    {
        var act = async () => await NullRateLimiter.Instance.WaitAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();
    }
}
