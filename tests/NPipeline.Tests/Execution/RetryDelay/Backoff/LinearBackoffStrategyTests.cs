using FluentAssertions;
using NPipeline.Execution.RetryDelay.Backoff;

namespace NPipeline.Tests.Execution.RetryDelay.Backoff;

public sealed class LinearBackoffStrategyTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(-10)]
    public void CalculateDelay_WithNegativeAttempt_ReturnsZero(int attempt)
    {
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromMilliseconds(40),
            Increment = TimeSpan.FromMilliseconds(10),
            MaxDelay = TimeSpan.FromSeconds(2),
        };

        var strategy = new LinearBackoffStrategy(configuration);

        _ = strategy.CalculateDelay(attempt).Should().Be(TimeSpan.Zero);
    }

    [Theory]
    [InlineData(0, 40)]
    [InlineData(1, 50)]
    [InlineData(2, 60)]
    public void CalculateDelay_WithValidAttempt_ComputesExpectedDelay(int attempt, int expectedMilliseconds)
    {
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromMilliseconds(40),
            Increment = TimeSpan.FromMilliseconds(10),
            MaxDelay = TimeSpan.FromSeconds(2),
        };

        var strategy = new LinearBackoffStrategy(configuration);

        _ = strategy.CalculateDelay(attempt)
            .Should().Be(TimeSpan.FromMilliseconds(expectedMilliseconds));
    }

    [Fact]
    public void CalculateDelay_WhenIncrementWouldExceedMaxDelay_IsCapped()
    {
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromMilliseconds(40),
            Increment = TimeSpan.FromMilliseconds(50),
            MaxDelay = TimeSpan.FromMilliseconds(120),
        };

        var strategy = new LinearBackoffStrategy(configuration);

        _ = strategy.CalculateDelay(5).Should().Be(configuration.MaxDelay);
    }
}
