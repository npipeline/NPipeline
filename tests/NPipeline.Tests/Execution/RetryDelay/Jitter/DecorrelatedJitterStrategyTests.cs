using AwesomeAssertions;
using NPipeline.Execution.RetryDelay.Jitter;

namespace NPipeline.Tests.Execution.RetryDelay.Jitter;

public sealed class DecorrelatedJitterStrategyTests
{
    [Fact]
    public void Constructor_WithValidConfiguration_ShouldCreateInstance()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration();

        // Act
        var strategy = new DecorrelatedJitterStrategy(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new DecorrelatedJitterStrategy(null!));
    }

    [Fact]
    public void Constructor_WithInvalidConfiguration_ShouldThrowArgumentException()
    {
        // Arrange
        var invalidConfiguration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.Zero, // Invalid: must be positive
        };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new DecorrelatedJitterStrategy(invalidConfiguration));
    }

    [Fact]
    public void ApplyJitter_WithNullRandom_ShouldThrowArgumentNullException()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration();
        var strategy = new DecorrelatedJitterStrategy(configuration);
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
        var configuration = new DecorrelatedJitterConfiguration();
        var strategy = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromMilliseconds(delayMs);
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void ApplyJitter_WithZeroBaseDelay_ShouldReturnZero()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration();
        var strategy = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.Zero;
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void ApplyJitter_WithFirstCall_ShouldUseBaseDelayAsUpperBound()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromSeconds(10),
            Multiplier = 3.0,
        };

        var strategy = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromSeconds(2);
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = result.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(2)); // Upper bound is baseDelay for first call
    }

    [Fact]
    public void ApplyJitter_WithSubsequentCalls_ShouldUsePreviousDelay()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromSeconds(10),
            Multiplier = 3.0,
        };

        var strategy = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromSeconds(2);
        var random = new Random(42);

        // Act
        var result1 = strategy.ApplyJitter(baseDelay, random);
        var result2 = strategy.ApplyJitter(baseDelay, random);

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
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromSeconds(5),
            Multiplier = 10.0, // Large multiplier
        };

        var strategy = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromSeconds(1);
        var random = new Random(42);

        // Act
        // First call
        var result1 = strategy.ApplyJitter(baseDelay, random);

        // Second call with large previous delay
        var result2 = strategy.ApplyJitter(baseDelay, random);

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
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromSeconds(10),
            Multiplier = multiplier,
        };

        var strategy = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromSeconds(1);
        var random = new Random(42);

        // Act
        var result1 = strategy.ApplyJitter(baseDelay, random);
        var result2 = strategy.ApplyJitter(baseDelay, random);

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
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromSeconds(10),
            Multiplier = 3.0,
        };

        var strategy = new DecorrelatedJitterStrategy(configuration);
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
                var result = strategy.ApplyJitter(baseDelay, random);
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
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.Zero, // Invalid: must be positive
        };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new DecorrelatedJitterStrategy(configuration));
    }

    [Fact]
    public void ApplyJitter_WithNegativeMaxDelay_ShouldThrowArgumentException()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromSeconds(-1), // Invalid: must be positive
        };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new DecorrelatedJitterStrategy(configuration));
    }

    [Fact]
    public void ApplyJitter_WithMultiplierLessThanOne_ShouldThrowArgumentException()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromMinutes(1),
            Multiplier = 0.9, // Invalid: must be >= 1.0
        };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new DecorrelatedJitterStrategy(configuration));
    }

    [Fact]
    public void ApplyJitter_WithNegativeMultiplier_ShouldThrowArgumentException()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromMinutes(1),
            Multiplier = -1.0, // Invalid: must be >= 1.0
        };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new DecorrelatedJitterStrategy(configuration));
    }

    [Fact]
    public void ApplyJitter_WithVerySmallBaseDelay_ShouldStillWork()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration();
        var strategy = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromTicks(1); // Smallest possible TimeSpan
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        _ = result.Should().BeGreaterThanOrEqualTo(baseDelay);
        _ = result.Should().BeLessThanOrEqualTo(baseDelay); // First call returns exactly baseDelay with decorrelated jitter
    }

    [Fact]
    public void ApplyJitter_WithLargeBaseDelay_ShouldCapAtMaxDelay()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromSeconds(5),
        };

        var strategy = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromSeconds(10); // Larger than MaxDelay
        var random = new Random(42);

        // Act
        var result = strategy.ApplyJitter(baseDelay, random);

        // Assert
        // When baseDelay > MaxDelay, result is capped at MaxDelay
        _ = result.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(5)); // Should be capped at MaxDelay
    }

    [Fact]
    public void ApplyJitter_WithMultipleCallsSequence_ShouldShowProgressiveUpperBound()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromSeconds(20),
            Multiplier = 2.0,
        };

        var strategy = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromSeconds(1);
        var random = new Random(42);

        // Act
        var results = new List<TimeSpan>();

        for (var i = 0; i < 5; i++)
        {
            results.Add(strategy.ApplyJitter(baseDelay, random));
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
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromSeconds(10),
            Multiplier = 2.5,
        };

        var strategy = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromSeconds(1);
        var random = new Random(42);

        // Act
        var result1 = strategy.ApplyJitter(baseDelay, random);
        var result2 = strategy.ApplyJitter(baseDelay, random);

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
        var configuration = new DecorrelatedJitterConfiguration();
        var strategy1 = new DecorrelatedJitterStrategy(configuration);
        var strategy2 = new DecorrelatedJitterStrategy(configuration);
        var baseDelay = TimeSpan.FromSeconds(1);
        var random1 = new Random(42);
        var random2 = new Random(42);

        // Act
        var result1 = strategy1.ApplyJitter(baseDelay, random1);
        var result2 = strategy2.ApplyJitter(baseDelay, random2);

        // Assert
        _ = result1.Should().Be(result2);
    }
}
