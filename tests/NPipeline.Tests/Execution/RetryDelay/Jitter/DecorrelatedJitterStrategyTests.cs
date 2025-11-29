using AwesomeAssertions;
using NPipeline.Execution.RetryDelay;

namespace NPipeline.Tests.Execution.RetryDelay.Jitter;

public sealed class DecorrelatedJitterStrategyTests
{
    [Fact]
    public void CreateDecorrelatedJitter_WithValidParameters_ShouldReturnValidDelegate()
    {
        // Act
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10));

        // Assert
        _ = strategy.Should().NotBeNull();
    }

    [Fact]
    public void CreateDecorrelatedJitter_WithInvalidMaxDelay_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => JitterStrategies.DecorrelatedJitter(TimeSpan.Zero));
    }

    [Fact]
    public void CreateDecorrelatedJitter_WithInvalidMultiplier_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10), 0.5));
    }

    [Fact]
    public void ApplyJitter_WithNullRandom_ShouldThrowArgumentNullException()
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10));
        var baseDelay = TimeSpan.FromSeconds(1);

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => strategy(baseDelay, null!));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(-10)]
    [InlineData(int.MinValue)]
    public void ApplyJitter_WithNegativeBaseDelay_ShouldReturnZero(int delayMs)
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10));
        var baseDelay = TimeSpan.FromMilliseconds(delayMs);
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void ApplyJitter_WithZeroBaseDelay_ShouldReturnZero()
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10));
        var baseDelay = TimeSpan.Zero;
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void ApplyJitter_WithFirstCall_ShouldUseBaseDelayAsUpperBound()
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10));
        var baseDelay = TimeSpan.FromSeconds(2);
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(2)); // Upper bound is baseDelay for first call
    }

    [Fact]
    public void ApplyJitter_WithSubsequentCalls_ShouldUsePreviousDelay()
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10));
        var baseDelay = TimeSpan.FromSeconds(2);
        var random = new Random(42);

        // Act
        var result1 = strategy(baseDelay, random);
        var result2 = strategy(baseDelay, random);

        // Assert
        _ = result1.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result1.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(2));

        _ = result2.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result2.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(6)); // Upper bound is result1 * 3.0
    }

    [Fact]
    public void ApplyJitter_WithMaxDelayCapping_ShouldCapAtMaxDelay()
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(5), 10.0); // Large multiplier
        var baseDelay = TimeSpan.FromSeconds(1);
        var random = new Random(42);

        // Act
        // First call
        var result1 = strategy(baseDelay, random);

        // Second call with large previous delay
        var result2 = strategy(baseDelay, random);

        // Assert
        _ = result1.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result1.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(1));

        _ = result2.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result2.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(5)); // Should be capped at MaxDelay
    }

    [Theory]
    [InlineData(1.0)]
    [InlineData(2.0)]
    [InlineData(3.0)]
    [InlineData(5.0)]
    [InlineData(10.0)]
    public void ApplyJitter_WithDifferentMultipliers_ShouldCalculateCorrectly(double multiplier)
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10), multiplier);
        var baseDelay = TimeSpan.FromSeconds(1);
        var random = new Random(42);

        // Act
        var result1 = strategy(baseDelay, random);
        var result2 = strategy(baseDelay, random);

        // Assert
        _ = result1.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result1.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(1));

        _ = result2.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        var expectedUpperBound = TimeSpan.FromSeconds(Math.Min(10, result1.TotalSeconds * multiplier));
        _ = result2.Should().BeLessThanOrEqualTo(expectedUpperBound);
    }

    [Fact]
    public void ApplyJitter_WithConcurrentCalls_ShouldBeThreadSafe()
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10));
        var baseDelay = TimeSpan.FromSeconds(1);
        var results = new List<TimeSpan>();
        var lockObject = new object();

        // Act
        Parallel.For(0, 10, i =>
        {
            var random = new Random();
            var localResults = new List<TimeSpan>();

            for (var j = 0; j < 10; j++)
            {
                var result = strategy(baseDelay, random);
                localResults.Add(result);
            }

            lock (lockObject)
            {
                results.AddRange(localResults);
            }
        });

        // Assert
        _ = results.Should().HaveCount(100);

        // All results should be in valid range
        foreach (var result in results)
        {
            _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
            _ = result.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(10));
        }
    }

    [Fact]
    public void ApplyJitter_WithZeroMaxDelay_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => JitterStrategies.DecorrelatedJitter(TimeSpan.Zero));
    }

    [Fact]
    public void ApplyJitter_WithNegativeMaxDelay_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(-1)));
    }

    [Fact]
    public void ApplyJitter_WithMultiplierLessThanOne_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1), 0.9));
    }

    [Fact]
    public void ApplyJitter_WithNegativeMultiplier_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1), -1.0));
    }

    [Fact]
    public void ApplyJitter_WithVerySmallBaseDelay_ShouldStillWork()
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10));
        var baseDelay = TimeSpan.FromTicks(1); // Smallest possible TimeSpan
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(baseDelay);
        _ = result.Should().BeLessThanOrEqualTo(baseDelay); // First call returns exactly baseDelay with decorrelated jitter
    }

    [Fact]
    public void ApplyJitter_WithLargeBaseDelay_ShouldCapAtMaxDelay()
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(5));
        var baseDelay = TimeSpan.FromSeconds(10); // Larger than MaxDelay
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        // When baseDelay > MaxDelay, result is capped at MaxDelay
        _ = result.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(5)); // Should be capped at MaxDelay
    }

    [Fact]
    public void ApplyJitter_WithMultipleCallsSequence_ShouldShowProgressiveUpperBound()
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(20), 2.0);
        var baseDelay = TimeSpan.FromSeconds(1);
        var random = new Random(42);

        // Act
        var results = new List<TimeSpan>();

        for (var i = 0; i < 5; i++)
        {
            results.Add(strategy(baseDelay, random));
        }

        // Assert
        _ = results.Should().HaveCount(5);

        // First result should be exactly baseDelay (on first call, no jitter range)
        _ = results[0].Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(1));

        // Subsequent results should have progressively higher upper bounds
        for (var i = 1; i < results.Count; i++)
        {
            _ = results[i].Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
            var expectedUpperBound = TimeSpan.FromSeconds(Math.Min(20, results[i - 1].TotalSeconds * 2.0));
            _ = results[i].Should().BeLessThanOrEqualTo(expectedUpperBound);
        }
    }

    [Fact]
    public void ApplyJitter_WithFractionalMultiplier_ShouldCalculateCorrectly()
    {
        // Arrange
        var strategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10), 2.5);
        var baseDelay = TimeSpan.FromSeconds(1);
        var random = new Random(42);

        // Act
        var result1 = strategy(baseDelay, random);
        var result2 = strategy(baseDelay, random);

        // Assert
        _ = result1.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result1.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(1));

        _ = result2.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        var expectedUpperBound = TimeSpan.FromSeconds(Math.Min(10, result1.TotalSeconds * 2.5));
        _ = result2.Should().BeLessThanOrEqualTo(expectedUpperBound);
    }

    [Fact]
    public void ApplyJitter_WithFixedSeed_ShouldProduceConsistentResults()
    {
        // Arrange
        var strategy1 = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10));
        var strategy2 = JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(10));
        var baseDelay = TimeSpan.FromSeconds(1);
        var random1 = new Random(42);
        var random2 = new Random(42);

        // Act
        var result1 = strategy1(baseDelay, random1);
        var result2 = strategy2(baseDelay, random2);

        // Assert
        _ = result1.Should().Be(result2);
    }
}
