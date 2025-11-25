using FluentAssertions;
using NPipeline.Execution.RetryDelay.Backoff;

namespace NPipeline.Tests.Execution.RetryDelay.Backoff;

public sealed class FixedDelayStrategyTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(-10)]
    public void CalculateDelay_WithNegativeAttempt_ReturnsZero(int attempt)
    {
        var configuration = new FixedDelayConfiguration
        {
            Delay = TimeSpan.FromMilliseconds(25),
        };

        var strategy = new FixedDelayStrategy(configuration);

        _ = strategy.CalculateDelay(attempt).Should().Be(TimeSpan.Zero);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    public void CalculateDelay_WithValidAttempt_ReturnsConfiguredDelay(int attempt)
    {
        var configuration = new FixedDelayConfiguration
        {
            Delay = TimeSpan.FromMilliseconds(25),
        };

        var strategy = new FixedDelayStrategy(configuration);

        _ = strategy.CalculateDelay(attempt).Should().Be(configuration.Delay);
    }
}
