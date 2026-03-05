using System.Net;
using System.Net.Http.Headers;
using NPipeline.Connectors.Http.Retry;

namespace NPipeline.Connectors.Http.Tests.Retry;

public class ExponentialBackoffHttpRetryStrategyTests
{
    [Theory]
    [InlineData(HttpStatusCode.TooManyRequests)]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.BadGateway)]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.GatewayTimeout)]
    public void ShouldRetry_WithRetryableStatusCode_ReturnsTrueForFirstAttempt(HttpStatusCode status)
    {
        var strategy = new ExponentialBackoffHttpRetryStrategy();
        using var response = new HttpResponseMessage(status);

        strategy.ShouldRetry(response, null, 1).Should().BeTrue();
    }

    [Theory]
    [InlineData(HttpStatusCode.OK)]
    [InlineData(HttpStatusCode.Created)]
    [InlineData(HttpStatusCode.BadRequest)]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.Unauthorized)]
    public void ShouldRetry_WithNonRetryableStatusCode_ReturnsFalse(HttpStatusCode status)
    {
        var strategy = new ExponentialBackoffHttpRetryStrategy();
        using var response = new HttpResponseMessage(status);

        strategy.ShouldRetry(response, null, 1).Should().BeFalse();
    }

    [Fact]
    public void ShouldRetry_WhenMaxRetriesReached_ReturnsFalse()
    {
        var strategy = new ExponentialBackoffHttpRetryStrategy { MaxRetries = 3 };
        using var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);

        strategy.ShouldRetry(response, null, 4).Should().BeFalse();
    }

    [Fact]
    public void ShouldRetry_WithHttpRequestException_ReturnsTrue()
    {
        var strategy = new ExponentialBackoffHttpRetryStrategy();

        strategy.ShouldRetry(null, new HttpRequestException("connection refused"), 1).Should().BeTrue();
    }

    [Fact]
    public void GetDelay_FirstAttempt_ReturnsBaseDelay()
    {
        var strategy = new ExponentialBackoffHttpRetryStrategy
        {
            BaseDelayMs = 200,
            JitterFactor = 0, // No jitter for deterministic test
        };

        var delay = strategy.GetDelay(null, 1);

        delay.TotalMilliseconds.Should().Be(200);
    }

    [Fact]
    public void GetDelay_SecondAttempt_DoublesDelay()
    {
        var strategy = new ExponentialBackoffHttpRetryStrategy
        {
            BaseDelayMs = 200,
            JitterFactor = 0,
        };

        var delay = strategy.GetDelay(null, 2);

        delay.TotalMilliseconds.Should().Be(400);
    }

    [Fact]
    public void GetDelay_NeverExceedsMaxDelayMs()
    {
        var strategy = new ExponentialBackoffHttpRetryStrategy
        {
            BaseDelayMs = 1000,
            MaxDelayMs = 5000,
            JitterFactor = 0,
        };

        // attempt 10 would be 1000 * 2^9 = 512000, capped at 5000
        var delay = strategy.GetDelay(null, 10);

        delay.TotalMilliseconds.Should().BeLessThanOrEqualTo(5000);
    }

    [Fact]
    public void GetDelay_With429AndRetryAfterHeader_HonoursRetryAfterValue()
    {
        var strategy = new ExponentialBackoffHttpRetryStrategy { JitterFactor = 0 };
        using var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
        response.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(10));

        var delay = strategy.GetDelay(response, 1);

        delay.TotalSeconds.Should().Be(10);
    }

    [Fact]
    public void GetDelay_With429AndRetryAfterDate_HonoursRetryAfterDate()
    {
        var strategy = new ExponentialBackoffHttpRetryStrategy { JitterFactor = 0 };
        using var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
        response.Headers.RetryAfter = new RetryConditionHeaderValue(DateTimeOffset.UtcNow.AddSeconds(2));

        var delay = strategy.GetDelay(response, 1);

        delay.Should().BeGreaterThan(TimeSpan.Zero);
        delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(2.5));
    }

    [Fact]
    public void GetDelay_WhenMaxTotalRetryDelayWouldBeExceeded_ClampsDelayToRemainingBudget()
    {
        var strategy = new ExponentialBackoffHttpRetryStrategy
        {
            BaseDelayMs = 100,
            MaxDelayMs = 10_000,
            JitterFactor = 0,
            MaxTotalRetryDelay = TimeSpan.FromMilliseconds(150),
        };

        var first = strategy.GetDelay(null, 1);
        var second = strategy.GetDelay(null, 2);

        first.TotalMilliseconds.Should().Be(100);
        second.TotalMilliseconds.Should().Be(50);
    }

    [Fact]
    public void Default_HasExpectedSettings()
    {
        var strategy = ExponentialBackoffHttpRetryStrategy.Default;

        strategy.MaxRetries.Should().Be(3);
        strategy.BaseDelayMs.Should().Be(200);
    }

    [Fact]
    public void Conservative_HasExpectedSettings()
    {
        var strategy = ExponentialBackoffHttpRetryStrategy.Conservative;

        strategy.MaxRetries.Should().Be(2);
        strategy.BaseDelayMs.Should().Be(1000);
    }
}
