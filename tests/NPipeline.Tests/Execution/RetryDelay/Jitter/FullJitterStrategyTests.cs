using AwesomeAssertions;
using NPipeline.Execution.RetryDelay.Jitter;

namespace NPipeline.Tests.Execution.RetryDelay.Jitter;

public sealed class FullJitterStrategyTests
{
    [Fact]
    public void Constructor_WithValidConfiguration_ShouldCreateInstance()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();

        // Act
        var strategy = new FullJitterStrategy(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new FullJitterStrategy(null!));
    }

    [Fact]
    public void ApplyJitter_WithNullRandom_ShouldThrowArgumentNullException()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromSeconds(1);

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => strategy.ApplyJitter(baseDelay, null!));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(-10)]
    [InlineData(int.MinValue)]
    public void ApplyJitter_WithNegativeBaseDelay_ShouldReturnZero(int delayMs)
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMilliseconds(delayMs);
        var random = new Random(42); // Fixed seed for predictable tests

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void ApplyJitter_WithZeroBaseDelay_ShouldReturnZero()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.Zero;
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.Zero);
    }

    [Theory]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(5000)]
    public void ApplyJitter_WithPositiveBaseDelay_ShouldReturnDelayInRange(int baseDelayMs)
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMilliseconds(baseDelayMs);
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result.Should().BeLessThan(TimeSpan.FromMilliseconds(baseDelayMs));
    }

    [Fact]
    public void ApplyJitter_WithSmallBaseDelay_ShouldReturnValidRange()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMilliseconds(1); // Very small delay
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result.Should().BeLessThan(TimeSpan.FromMilliseconds(1));
    }

    [Fact]
    public void ApplyJitter_WithLargeBaseDelay_ShouldReturnValidRange()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMinutes(5); // Large delay
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result.Should().BeLessThan(TimeSpan.FromMinutes(5));
    }

    [Fact]
    public void ApplyJitter_WithMultipleCalls_ShouldProduceDifferentResults()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromSeconds(1);
        var random = new Random(); // No seed for true randomness

        // Act
        var results = new List<TimeSpan>();

        for (var i = 0; i < 100; i++)
        {
            results.Add(strategy.ApplyJitter(baseDelay, random));
        }

        // Assert
        _ = results.Should().HaveCount(100);

        // Should have some variation (not all the same)
        var distinctResults = results.Distinct().ToList();
        _ = distinctResults.Should().HaveCountGreaterThan(1);

        // All results should be in valid range
        foreach (var result in results)
        {
            _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
            _ = result.Should().BeLessThan(TimeSpan.FromSeconds(1));
        }
    }

    [Fact]
    public void ApplyJitter_WithFixedSeed_ShouldProduceConsistentResults()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMilliseconds(1000);
        var random1 = new Random(42);
        var random2 = new Random(42);

        // Act
        var result1 = strategy.ApplyJitter(baseDelay, random1);
        var result2 = strategy.ApplyJitter(baseDelay, random2);

        // Assert
        _ = result1.Should().Be(result2);
    }

    [Fact]
    public void ApplyJitter_WithDifferentRandomInstances_ShouldProduceDifferentResults()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMilliseconds(1000);
        var random1 = new Random(42);
        var random2 = new Random(24); // Different seed

        // Act
        var result1 = strategy.ApplyJitter(baseDelay, random1);
        var result2 = strategy.ApplyJitter(baseDelay, random2);

        // Assert
        _ = result1.Should().NotBe(result2);
    }

    [Fact]
    public void ApplyJitter_WithVerySmallBaseDelay_ShouldHandleEdgeCase()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromTicks(1); // Smallest possible TimeSpan
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result.Should().BeLessThan(TimeSpan.FromTicks(1));
    }

    [Fact]
    public void ApplyJitter_WithMaxValueBaseDelay_ShouldHandleLargeValue()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.MaxValue;
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result.Should().BeLessThan(TimeSpan.MaxValue);
    }

    [Fact]
    public void ApplyJitter_WithFractionalBaseDelay_ShouldHandleCorrectly()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMilliseconds(123.456);
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result.Should().BeLessThan(TimeSpan.FromMilliseconds(123.456));
    }

    [Fact]
    public void ApplyJitter_StatisticalDistribution_ShouldBeUniform()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMilliseconds(1000);
        var random = new Random(42);

        // Act
        var results = new List<int>();

        for (var i = 0; i < 1000; i++)
        {
            var jitteredDelay = strategy.ApplyJitter(baseDelay, random);
            results.Add((int)jitteredDelay.TotalMilliseconds);
        }

        // Assert
        // Results should be distributed across the range
        results.Min().Should().BeGreaterThanOrEqualTo(0);
        results.Max().Should().BeLessThan(1000);

        // Average should be roughly in the middle of the range (with some tolerance)
        var average = results.Average();
        average.Should().BeGreaterThan(400); // Lower bound check
        average.Should().BeLessThan(600); // Upper bound check
    }

    [Fact]
    public void ApplyJitter_WithConcurrentCalls_ShouldBeThreadSafe()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMilliseconds(1000);
        var results = new List<TimeSpan>();
        var lockObject = new object();

        // Act
        Parallel.For(0, 10, i =>
        {
            var random = new Random();
            var localResults = new List<TimeSpan>();

            for (var j = 0; j < 100; j++)
            {
                var result = strategy.ApplyJitter(baseDelay, random);
                localResults.Add(result);
            }

            lock (lockObject)
            {
                results.AddRange(localResults);
            }
        });

        // Assert
        _ = results.Should().HaveCount(1000);

        // All results should be in valid range
        foreach (var result in results)
        {
            _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
            _ = result.Should().BeLessThan(TimeSpan.FromMilliseconds(1000));
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    public void ApplyJitter_WithVariousBaseDelays_ShouldMaintainCorrectRange(int baseDelayMs)
    {
        // Arrange
        var configuration = new FullJitterConfiguration();
        var strategy = new FullJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMilliseconds(baseDelayMs);
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result.Should().BeLessThan(TimeSpan.FromMilliseconds(baseDelayMs));
    }
}
