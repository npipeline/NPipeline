using FluentAssertions;
using NPipeline.Execution.RetryDelay.Backoff;

namespace NPipeline.Tests.Execution.RetryDelay.Backoff;

public sealed class ExponentialBackoffStrategyTests
{
    [Fact]
    public void CalculateDelay_WithNegativeAttempt_ReturnsZero()
    {
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromMilliseconds(50),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(5),
        };

        var strategy = new ExponentialBackoffStrategy(configuration);

        _ = strategy.CalculateDelay(-1).Should().Be(TimeSpan.Zero);
    }

    [Theory]
    [InlineData(0, 50)]
    [InlineData(1, 100)]
    [InlineData(2, 200)]
    public void CalculateDelay_WithValidAttempt_ComputesExpectedDelay(int attempt, int expectedMilliseconds)
    {
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromMilliseconds(50),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(5),
        };

        var strategy = new ExponentialBackoffStrategy(configuration);

        _ = strategy.CalculateDelay(attempt)
            .Should().Be(TimeSpan.FromMilliseconds(expectedMilliseconds));
    }

    [Fact]
    public void CalculateDelay_WhenResultExceedsMaxDelay_IsCapped()
    {
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(1),
            Multiplier = 10.0,
            MaxDelay = TimeSpan.FromSeconds(5),
        };

        var strategy = new ExponentialBackoffStrategy(configuration);

        _ = strategy.CalculateDelay(3).Should().Be(configuration.MaxDelay);
    }
}
