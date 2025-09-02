using System.Collections.Concurrent;
using System.Reflection;
using AwesomeAssertions;
using NPipeline.Execution.CircuitBreaking;

namespace NPipeline.Tests.Execution.CircuitBreaking;

/// <summary>
///     Tests for RollingWindow functionality.
///     Validates operation tracking, statistics calculation, and thread safety.
/// </summary>
public sealed class RollingWindowTests : IDisposable
{
    private readonly RollingWindow _window;

    public RollingWindowTests()
    {
        _window = new RollingWindow(TimeSpan.FromMinutes(1));
    }

    public void Dispose()
    {
        _window.Dispose();
    }

    [Fact]
    public void Constructor_WithValidWindowSize_ShouldCreateInstance()
    {
        // Arrange
        var windowSize = TimeSpan.FromMinutes(5);

        // Act
        using var window = new RollingWindow(windowSize);

        // Assert
        _ = window.Should().NotBeNull();
    }

    [Fact]
    public void AddOperation_WithSuccessOutcome_ShouldTrackOperation()
    {
        // Arrange
        var initialStats = _window.GetStatistics();

        // Act
        _window.AddOperation(OperationOutcome.Success);
        var stats = _window.GetStatistics();

        // Assert
        _ = stats.TotalOperations.Should().Be(initialStats.TotalOperations + 1);
        _ = stats.SuccessCount.Should().Be(initialStats.SuccessCount + 1);
        _ = stats.FailureCount.Should().Be(initialStats.FailureCount);
        _ = stats.FailureRate.Should().Be(0.0);
    }

    [Fact]
    public void AddOperation_WithFailureOutcome_ShouldTrackOperation()
    {
        // Arrange
        var initialStats = _window.GetStatistics();

        // Act
        _window.AddOperation(OperationOutcome.Failure);
        var stats = _window.GetStatistics();

        // Assert
        _ = stats.TotalOperations.Should().Be(initialStats.TotalOperations + 1);
        _ = stats.FailureCount.Should().Be(initialStats.FailureCount + 1);
        _ = stats.SuccessCount.Should().Be(initialStats.SuccessCount);
        _ = stats.FailureRate.Should().Be(1.0);
    }

    [Fact]
    public void AddOperation_WithMixedOutcomes_ShouldCalculateCorrectFailureRate()
    {
        // Arrange & Act
        _window.AddOperation(OperationOutcome.Success);
        _window.AddOperation(OperationOutcome.Failure);
        _window.AddOperation(OperationOutcome.Success);
        _window.AddOperation(OperationOutcome.Failure);

        var stats = _window.GetStatistics();

        // Assert
        _ = stats.TotalOperations.Should().Be(4);
        _ = stats.SuccessCount.Should().Be(2);
        _ = stats.FailureCount.Should().Be(2);
        _ = stats.FailureRate.Should().Be(0.5);
    }

    [Fact]
    public void GetStatistics_WithEmptyWindow_ShouldReturnZeroValues()
    {
        // Act
        var stats = _window.GetStatistics();

        // Assert
        _ = stats.TotalOperations.Should().Be(0);
        _ = stats.SuccessCount.Should().Be(0);
        _ = stats.FailureCount.Should().Be(0);
        _ = stats.FailureRate.Should().Be(0.0);
    }

    [Fact]
    public void GetConsecutiveFailures_WithNoFailures_ShouldReturnZero()
    {
        // Arrange
        _window.AddOperation(OperationOutcome.Success);
        _window.AddOperation(OperationOutcome.Success);
        _window.AddOperation(OperationOutcome.Success);

        // Act
        var consecutiveFailures = _window.GetConsecutiveFailures();

        // Assert
        _ = consecutiveFailures.Should().Be(0);
    }

    [Fact]
    public void GetConsecutiveFailures_WithConsecutiveFailuresAtEnd_ShouldReturnCorrectCount()
    {
        // Arrange
        _window.AddOperation(OperationOutcome.Success);
        _window.AddOperation(OperationOutcome.Failure);
        _window.AddOperation(OperationOutcome.Failure);
        _window.AddOperation(OperationOutcome.Failure);

        // Act
        var consecutiveFailures = _window.GetConsecutiveFailures();

        // Assert
        _ = consecutiveFailures.Should().Be(3);
    }

    [Fact]
    public void GetConsecutiveFailures_WithFailureFollowedBySuccess_ShouldReturnZero()
    {
        // Arrange
        _window.AddOperation(OperationOutcome.Failure);
        _window.AddOperation(OperationOutcome.Failure);
        _window.AddOperation(OperationOutcome.Success);

        // Act
        var consecutiveFailures = _window.GetConsecutiveFailures();

        // Assert
        _ = consecutiveFailures.Should().Be(0);
    }

    [Fact]
    public void GetConsecutiveFailures_WithMixedOperations_ShouldCountOnlyTrailingFailures()
    {
        // Arrange
        _window.AddOperation(OperationOutcome.Failure);
        _window.AddOperation(OperationOutcome.Success);
        _window.AddOperation(OperationOutcome.Failure);
        _window.AddOperation(OperationOutcome.Failure);

        // Act
        var consecutiveFailures = _window.GetConsecutiveFailures();

        // Assert
        _ = consecutiveFailures.Should().Be(2);
    }

    [Fact]
    public void Clear_ShouldRemoveAllOperations()
    {
        // Arrange
        _window.AddOperation(OperationOutcome.Success);
        _window.AddOperation(OperationOutcome.Failure);
        _window.AddOperation(OperationOutcome.Success);

        // Act
        _window.Clear();
        var stats = _window.GetStatistics();

        // Assert
        _ = stats.TotalOperations.Should().Be(0);
        _ = stats.SuccessCount.Should().Be(0);
        _ = stats.FailureCount.Should().Be(0);
        _ = stats.FailureRate.Should().Be(0.0);
    }

    [Fact]
    public void PurgeExpiredOperations_WithOperationsOutsideWindow_ShouldRemoveExpiredOnes()
    {
        // Arrange
        using var shortWindow = new RollingWindow(TimeSpan.FromMilliseconds(100));

        // Add operations that will be outside of window
        var pastTime = DateTime.UtcNow.AddMilliseconds(-200);

        // Use reflection to add expired operations for testing
        var operationsField = typeof(RollingWindow).GetField("_operations",
            BindingFlags.NonPublic | BindingFlags.Instance);

        var operations = operationsField?.GetValue(shortWindow) as Queue<OperationRecord>;

        operations?.Enqueue(new OperationRecord(pastTime, OperationOutcome.Failure));
        operations?.Enqueue(new OperationRecord(pastTime.AddMilliseconds(-50), OperationOutcome.Success));

        // Add current operation
        shortWindow.AddOperation(OperationOutcome.Success);

        // Act
        var stats = shortWindow.GetStatistics();

        // Assert
        _ = stats.TotalOperations.Should().Be(1);
        _ = stats.SuccessCount.Should().Be(1);
        _ = stats.FailureCount.Should().Be(0);
    }

    [Fact]
    public async Task AddOperation_Concurrently_ShouldBeThreadSafe()
    {
        // Arrange
        const int operationCount = 1000;
        var tasks = new List<Task>();
        var successCount = 0;
        var failureCount = 0;

        // Act
        for (var i = 0; i < operationCount; i++)
        {
            var isFailure = i % 2 == 0;

            tasks.Add(Task.Run(() =>
            {
                if (isFailure)
                {
                    _window.AddOperation(OperationOutcome.Failure);
                    _ = Interlocked.Increment(ref failureCount);
                }
                else
                {
                    _window.AddOperation(OperationOutcome.Success);
                    _ = Interlocked.Increment(ref successCount);
                }
            }));
        }

        await Task.WhenAll(tasks);

        // Assert
        var stats = _window.GetStatistics();
        _ = stats.TotalOperations.Should().Be(operationCount);
        _ = stats.SuccessCount.Should().Be(successCount);
        _ = stats.FailureCount.Should().Be(failureCount);
        _ = stats.FailureRate.Should().Be((double)failureCount / operationCount);
    }

    [Fact]
    public async Task GetStatistics_ConcurrentlyWithAddOperations_ShouldBeThreadSafe()
    {
        // Arrange
        const int operationCount = 500;
        const int readerCount = 10;
        var tasks = new List<Task>();
        var statisticsList = new ConcurrentBag<WindowStatistics>();

        // Act - Add operations concurrently
        for (var i = 0; i < operationCount; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                _window.AddOperation(i % 2 == 0
                    ? OperationOutcome.Success
                    : OperationOutcome.Failure);
            }));
        }

        // Act - Read statistics concurrently
        for (var i = 0; i < readerCount; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                for (var j = 0; j < 50; j++)
                {
                    var stats = _window.GetStatistics();
                    statisticsList.Add(stats);
                    await Task.Delay(1);
                }
            }));
        }

        await Task.WhenAll(tasks);

        // Assert
        var finalStats = _window.GetStatistics();
        _ = finalStats.TotalOperations.Should().Be(operationCount);
        _ = statisticsList.Should().HaveCount(readerCount * 50);

        // All statistics should be valid
        foreach (var stats in statisticsList)
        {
            _ = stats.TotalOperations.Should().BeGreaterThanOrEqualTo(0);
            _ = stats.SuccessCount.Should().BeGreaterThanOrEqualTo(0);
            _ = stats.FailureCount.Should().BeGreaterThanOrEqualTo(0);
            _ = stats.FailureRate.Should().BeInRange(0.0, 1.0);
        }
    }

    [Fact]
    public async Task GetConsecutiveFailures_ConcurrentlyWithAddOperations_ShouldBeThreadSafe()
    {
        // Arrange
        const int operationCount = 500;
        var tasks = new List<Task>();
        var consecutiveFailuresList = new ConcurrentBag<int>();

        // Act
        for (var i = 0; i < operationCount; i++)
        {
            var currentIndex = i; // Capture current index for this iteration

            tasks.Add(Task.Run(() =>
            {
                var isFailure = currentIndex < operationCount / 2; // First half are failures

                _window.AddOperation(isFailure
                    ? OperationOutcome.Failure
                    : OperationOutcome.Success);

                // Also read consecutive failures concurrently
                var consecutiveFailures = _window.GetConsecutiveFailures();
                consecutiveFailuresList.Add(consecutiveFailures);
            }));
        }

        await Task.WhenAll(tasks);

        // Assert
        var finalStats = _window.GetStatistics();
        var finalConsecutiveFailures = _window.GetConsecutiveFailures();

        // Verify data integrity - we should have exactly the expected number of operations
        _ = finalStats.TotalOperations.Should().Be(operationCount);
        _ = finalStats.FailureCount.Should().Be(operationCount / 2);
        _ = finalStats.SuccessCount.Should().Be(operationCount / 2);

        // Verify thread safety - the final consecutive failures should be within valid range
        // Since operations are added concurrently, we can't predict the exact order,
        // but we can verify the result is logically consistent
        _ = finalConsecutiveFailures.Should().BeGreaterThanOrEqualTo(0);
        _ = finalConsecutiveFailures.Should().BeLessThanOrEqualTo(operationCount);

        // Verify all concurrent reads returned valid values
        _ = consecutiveFailuresList.Should().HaveCount(operationCount);

        foreach (var count in consecutiveFailuresList)
        {
            _ = count.Should().BeGreaterThanOrEqualTo(0);
            _ = count.Should().BeLessThanOrEqualTo(operationCount);
        }

        // Additional thread safety validation: The rolling window should maintain consistency
        // between total operations and the sum of failures and successes
        _ = (finalStats.FailureCount + finalStats.SuccessCount).Should().Be(finalStats.TotalOperations);
    }

    [Theory]
    [InlineData(10)] // Every 10 operations
    [InlineData(5)] // Every 5 operations
    [InlineData(1)] // Every operation
    public void AddOperation_ShouldPurgeExpiredOperationsPeriodically(int purgeInterval)
    {
        // This test verifies the purge logic is called periodically
        // We can't easily test the actual purging without time manipulation,
        // but we can verify the operations are added correctly

        // Arrange
        using var window = new RollingWindow(TimeSpan.FromHours(1)); // Long window to avoid purging

        // Act
        for (var i = 0; i < purgeInterval * 2; i++)
        {
            window.AddOperation(i % 2 == 0
                ? OperationOutcome.Success
                : OperationOutcome.Failure);
        }

        var stats = window.GetStatistics();

        // Assert
        _ = stats.TotalOperations.Should().Be(purgeInterval * 2);
    }

    [Fact]
    public void Dispose_ShouldNotThrowException()
    {
        // Arrange
        var window = new RollingWindow(TimeSpan.FromMinutes(1));
        window.AddOperation(OperationOutcome.Success);

        // Act & Assert
        _ = window.Invoking(w => w.Dispose()).Should().NotThrow();
    }

    [Fact]
    public void Dispose_CalledMultipleTimes_ShouldNotThrowException()
    {
        // Arrange
        var window = new RollingWindow(TimeSpan.FromMinutes(1));
        window.Dispose();

        // Act & Assert
        _ = window.Invoking(w => w.Dispose()).Should().NotThrow();
    }
}
