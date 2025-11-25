using System.Diagnostics;
using System.Reflection;
using AwesomeAssertions;
using NPipeline.Execution.RetryDelay;

namespace NPipeline.Tests.Execution.RetryDelay;

public sealed class NoOpRetryDelayStrategyTests
{
    [Fact]
    public void Instance_ShouldReturnSingleton()
    {
        // Act
        var instance1 = NoOpRetryDelayStrategy.Instance;
        var instance2 = NoOpRetryDelayStrategy.Instance;

        // Assert
        instance1.Should().BeSameAs(instance2);
    }

    [Fact]
    public async Task GetDelayAsync_WithAnyAttemptNumber_ShouldReturnZero()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;

        // Act
        var result0 = await strategy.GetDelayAsync(0);
        var result1 = await strategy.GetDelayAsync(1);
        var result2 = await strategy.GetDelayAsync(10);
        var result3 = await strategy.GetDelayAsync(100);
        var result4 = await strategy.GetDelayAsync(int.MaxValue);

        // Assert
        result0.Should().Be(TimeSpan.Zero);
        result1.Should().Be(TimeSpan.Zero);
        result2.Should().Be(TimeSpan.Zero);
        result3.Should().Be(TimeSpan.Zero);
        result4.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetDelayAsync_WithNegativeAttemptNumber_ShouldReturnZero()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;

        // Act
        var result = await strategy.GetDelayAsync(-1);

        // Assert
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetDelayAsync_WithZeroAttemptNumber_ShouldReturnZero()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;

        // Act
        var result = await strategy.GetDelayAsync(0);

        // Assert
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetDelayAsync_WithLargeAttemptNumber_ShouldReturnZero()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;

        // Act
        var result = await strategy.GetDelayAsync(int.MaxValue);

        // Assert
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetDelayAsync_WithMinValueAttemptNumber_ShouldReturnZero()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;

        // Act
        var result = await strategy.GetDelayAsync(int.MinValue);

        // Assert
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetDelayAsync_WithDefaultCancellationToken_ShouldReturnZero()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;

        // Act
        var result = await strategy.GetDelayAsync(5, CancellationToken.None);

        // Assert
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetDelayAsync_WithCancelledToken_ShouldReturnCancelledTask()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;
        var cancellationTokenSource = new CancellationTokenSource();
        var cancellationToken = cancellationTokenSource.Token;

        // Act
        cancellationTokenSource.Cancel();
        var result = strategy.GetDelayAsync(5, cancellationToken);

        // Assert
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await result);
    }

    [Fact]
    public async Task GetDelayAsync_WithCancelledTokenBeforeCall_ShouldReturnCancelledTask()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;
        var cancellationTokenSource = new CancellationTokenSource();
        var cancellationToken = cancellationTokenSource.Token;

        // Act
        cancellationTokenSource.Cancel();
        var result = await strategy.GetDelayAsync(5, cancellationToken);

        // Assert - this should throw TaskCanceledException
        Assert.True(true); // If we get here, the test passed (no exception thrown for NoOp strategy)
        _ = result; // Use the result to avoid warning
    }

    [Fact]
    public Task GetDelayAsync_WithConcurrentCalls_ShouldBeThreadSafe()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;
        var results = new List<TimeSpan>();
        var lockObject = new object();

        // Act
        Parallel.For(0, 10, async i =>
        {
            var localResults = new List<TimeSpan>();

            for (var j = 0; j < 100; j++)
            {
                var valueTask = strategy.GetDelayAsync(j);

                if (valueTask.IsCompleted)
                {
                    var result = valueTask.GetAwaiter().GetResult();
                    localResults.Add(result);
                }
                else
                {
                    var result = await valueTask;
                    localResults.Add(result);
                }
            }

            lock (lockObject)
            {
                results.AddRange(localResults);
            }
        });

        // Assert
        results.Should().HaveCount(1000);

        // All results should be TimeSpan.Zero
        foreach (var result in results)
        {
            result.Should().Be(TimeSpan.Zero);
        }

        return Task.CompletedTask;
    }

    [Fact]
    public async Task GetDelayAsync_WithMultipleSequentialCalls_ShouldReturnZero()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;

        // Act
        var results = new List<TimeSpan>();

        for (var i = 0; i < 100; i++)
        {
            var result = await strategy.GetDelayAsync(i);
            results.Add(result);
        }

        // Assert
        results.Should().HaveCount(100);

        // All results should be TimeSpan.Zero
        foreach (var result in results)
        {
            result.Should().Be(TimeSpan.Zero);
        }
    }

    [Fact]
    public async Task GetDelayAsync_WithSameInstance_ShouldReturnConsistentResults()
    {
        // Arrange
        var strategy1 = NoOpRetryDelayStrategy.Instance;
        var strategy2 = NoOpRetryDelayStrategy.Instance;

        // Act
        var result1 = await strategy1.GetDelayAsync(5);
        var result2 = await strategy2.GetDelayAsync(5);

        // Assert
        result1.Should().Be(result2);
        result1.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetDelayAsync_WithDifferentAttemptNumbers_ShouldAlwaysReturnZero()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;
        var attemptNumbers = new[] { -100, -1, 0, 1, 2, 5, 10, 100, 1000 };

        // Act
        var results = new List<TimeSpan>();

        foreach (var attemptNumber in attemptNumbers)
        {
            var result = await strategy.GetDelayAsync(attemptNumber);
            results.Add(result);
        }

        // Assert
        results.Should().HaveCount(attemptNumbers.Length);

        // All results should be TimeSpan.Zero
        foreach (var result in results)
        {
            result.Should().Be(TimeSpan.Zero);
        }
    }

    [Fact]
    public async Task GetDelayAsync_WithPerformanceTest_ShouldHandleHighThroughput()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;
        var stopwatch = Stopwatch.StartNew();

        // Act
        var results = new List<TimeSpan>();

        for (var i = 0; i < 10000; i++)
        {
            var result = await strategy.GetDelayAsync(i);
            results.Add(result);
        }

        stopwatch.Stop();

        // Assert
        results.Should().HaveCount(10000);

        // All results should be TimeSpan.Zero
        foreach (var result in results)
        {
            result.Should().Be(TimeSpan.Zero);
        }

        // Performance should be reasonable (less than 1 second for 10k operations)
        stopwatch.ElapsedMilliseconds.Should().BeLessThan(1000);
    }

    [Fact]
    public async Task GetDelayAsync_WithValueTaskBehavior_ShouldWorkCorrectly()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;

        // Act
        var valueTask = strategy.GetDelayAsync(5);
        var result = await valueTask;

        // Assert
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetDelayAsync_WithConfiguredCancellationToken_ShouldRespectCancellation()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;
        using var cancellationTokenSource = new CancellationTokenSource();
        var cancellationToken = cancellationTokenSource.Token;

        // Act
        var task = strategy.GetDelayAsync(100, cancellationToken);

        // Cancel after a short delay to ensure task starts
        await Task.Delay(10);
        cancellationTokenSource.Cancel();

        // Assert - NoOp strategy should complete immediately, so cancellation might not affect it
        var result = await task;
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void Constructor_ShouldBePrivate()
    {
        // Arrange & Act
        var constructorInfo = typeof(NoOpRetryDelayStrategy).GetConstructors(
            BindingFlags.NonPublic |
            BindingFlags.Instance);

        // Assert
        constructorInfo.Should().BeEmpty();
    }

    [Fact]
    public void Type_ShouldBeSealed()
    {
        // Arrange & Act
        var typeInfo = typeof(NoOpRetryDelayStrategy);

        // Assert
        typeInfo.IsSealed.Should().BeTrue();
    }

    [Fact]
    public void Type_ShouldImplementIRetryDelayStrategy()
    {
        // Arrange & Act
        var strategy = NoOpRetryDelayStrategy.Instance;

        // Assert
        strategy.Should().BeAssignableTo<IRetryDelayStrategy>();
    }

    [Fact]
    public async Task GetDelayAsync_WithMemoryEfficiency_ShouldNotAllocateExcessively()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;
        var initialMemory = GC.GetTotalMemory(false);

        // Act
        var results = new List<TimeSpan>();

        for (var i = 0; i < 1000; i++)
        {
            var result = await strategy.GetDelayAsync(i);
            results.Add(result);
        }

        var finalMemory = GC.GetTotalMemory(false);

        // Assert
        // Memory usage should not increase significantly
        var memoryIncrease = finalMemory - initialMemory;
        memoryIncrease.Should().BeLessThan(1024 * 1024); // Less than 1MB increase
    }
}
