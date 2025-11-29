using AwesomeAssertions;
using NPipeline.Execution.RetryDelay;

namespace NPipeline.Tests.Execution.RetryDelay.Jitter;

public sealed class NoJitterStrategyTests
{
    [Fact]
    public void CreateNoJitter_ShouldReturnValidDelegate()
    {
        // Act
        var strategy = JitterStrategies.NoJitter();

        // Assert
        _ = strategy.Should().NotBeNull();
    }

    [Fact]
    public void ApplyJitter_WithNullRandom_ShouldThrowArgumentNullException()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
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
        var strategy = JitterStrategies.NoJitter();
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
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.Zero;
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.Zero);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(5000)]
    public void ApplyJitter_WithPositiveBaseDelay_ShouldReturnBaseDelay(int baseDelayMs)
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.FromMilliseconds(baseDelayMs);
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.FromMilliseconds(baseDelayMs));
    }

    [Fact]
    public void ApplyJitter_WithVerySmallBaseDelay_ShouldReturnBaseDelay()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.FromTicks(1); // Smallest possible TimeSpan
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.FromTicks(1));
    }

    [Fact]
    public void ApplyJitter_WithLargeBaseDelay_ShouldReturnBaseDelay()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.FromMinutes(5); // Large delay
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.FromMinutes(5));
    }

    [Fact]
    public void ApplyJitter_WithMultipleCalls_ShouldReturnConsistentResults()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.FromMilliseconds(1000);
        var random = new Random(); // No seed for true randomness

        // Act
        var results = new List<TimeSpan>();

        for (var i = 0; i < 100; i++)
        {
            results.Add(strategy(baseDelay, random));
        }

        // Assert
        _ = results.Should().HaveCount(100);

        // All results should be identical (no jitter)
        var distinctResults = results.Distinct().ToList();
        _ = distinctResults.Should().HaveCount(1);
        _ = distinctResults[0].Should().Be(TimeSpan.FromMilliseconds(1000));
    }

    [Fact]
    public void ApplyJitter_WithDifferentRandomInstances_ShouldReturnSameResult()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.FromMilliseconds(1000);
        var random1 = new Random(42);
        var random2 = new Random(24); // Different seed

        // Act
        var result1 = strategy(baseDelay, random1);
        var result2 = strategy(baseDelay, random2);

        // Assert
        _ = result1.Should().Be(result2);
        _ = result1.Should().Be(TimeSpan.FromMilliseconds(1000));
    }

    [Fact]
    public void ApplyJitter_WithMaxValueBaseDelay_ShouldReturnMaxValue()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.MaxValue;
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.MaxValue);
    }

    [Fact]
    public void ApplyJitter_WithFractionalBaseDelay_ShouldReturnExactDelay()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.FromMilliseconds(123.456);
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.FromMilliseconds(123.456));
    }

    [Fact]
    public void ApplyJitter_WithConcurrentCalls_ShouldBeThreadSafe()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
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
                var result = strategy(baseDelay, random);
                localResults.Add(result);
            }

            lock (lockObject)
            {
                results.AddRange(localResults);
            }
        });

        // Assert
        _ = results.Should().HaveCount(1000);

        // All results should be identical to base delay
        foreach (var result in results)
        {
            _ = result.Should().Be(TimeSpan.FromMilliseconds(1000));
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    [InlineData(86400000)] // 1 day in milliseconds
    public void ApplyJitter_WithVariousBaseDelays_ShouldReturnExactBaseDelay(int baseDelayMs)
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.FromMilliseconds(baseDelayMs);
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.FromMilliseconds(baseDelayMs));
    }

    [Fact]
    public void ApplyJitter_WithMinValueBaseDelay_ShouldReturnMinValue()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.MinValue;
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.Zero); // Negative delays return zero
    }

    [Fact]
    public void ApplyJitter_WithZeroTicksBaseDelay_ShouldReturnZero()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.FromTicks(0);
        var random = new Random(42);

        // Act
        var result = strategy(baseDelay, random);

        // Assert
        _ = result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void ApplyJitter_RandomParameterNotUsed_ShouldStillWork()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.FromMilliseconds(1000);
        var random = new Random(42);

        // Act - Call multiple times to verify random doesn't affect result
        var result1 = strategy(baseDelay, random);
        var result2 = strategy(baseDelay, random);
        var result3 = strategy(baseDelay, random);

        // Assert
        _ = result1.Should().Be(TimeSpan.FromMilliseconds(1000));
        _ = result2.Should().Be(TimeSpan.FromMilliseconds(1000));
        _ = result3.Should().Be(TimeSpan.FromMilliseconds(1000));
    }

    [Fact]
    public void ApplyJitter_WithDifferentRandomStates_ShouldReturnConsistentResult()
    {
        // Arrange
        var strategy = JitterStrategies.NoJitter();
        var baseDelay = TimeSpan.FromMilliseconds(1000);
        var random = new Random(42);

        // Act
        var result1 = strategy(baseDelay, random);

        // Modify random state
        random.Next();
        random.NextDouble();

        var result2 = strategy(baseDelay, random);

        // Assert
        _ = result1.Should().Be(result2);
        _ = result1.Should().Be(TimeSpan.FromMilliseconds(1000));
    }
}
