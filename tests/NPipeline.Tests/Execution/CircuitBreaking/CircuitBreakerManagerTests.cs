using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Configuration;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Observability.Logging;

namespace NPipeline.Tests.Execution.CircuitBreaking;

/// <summary>
///     Tests for CircuitBreakerManager functionality.
///     Validates circuit breaker lifecycle management, caching, and cleanup.
/// </summary>
public sealed class CircuitBreakerManagerTests : IDisposable
{
    private readonly IPipelineLogger _logger;
    private readonly CircuitBreakerManager _manager;

    public CircuitBreakerManagerTests()
    {
        _logger = A.Fake<IPipelineLogger>();
        _manager = new CircuitBreakerManager(_logger);
    }

    public void Dispose()
    {
        _manager.Dispose();
    }

    [Fact]
    public void Constructor_WithValidLogger_ShouldCreateInstance()
    {
        // Arrange & Act
        using var manager = new CircuitBreakerManager(_logger);

        // Assert
        _ = manager.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullLogger_ShouldThrowArgumentNullException()
    {
        // Arrange & Act & Assert
        _ = ((Func<CircuitBreakerManager>)(() => new CircuitBreakerManager(null!)))
            .Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetCircuitBreaker_WithValidParameters_ShouldCreateAndReturnCircuitBreaker()
    {
        // Arrange
        var nodeId = "test-node";

        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        // Act
        var circuitBreaker = _manager.GetCircuitBreaker(nodeId, options);

        // Assert
        _ = circuitBreaker.Should().NotBeNull();
        _ = circuitBreaker.State.Should().Be(CircuitBreakerState.Closed);
        _ = circuitBreaker.Options.Should().Be(options);
    }

    [Fact]
    public void GetCircuitBreaker_WithNullNodeId_ShouldThrowArgumentNullException()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        // Act & Assert
        _ = _manager.Invoking(m => m.GetCircuitBreaker(null!, options))
            .Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetCircuitBreaker_WithNullOptions_ShouldThrowArgumentNullException()
    {
        // Arrange
        var nodeId = "test-node";

        // Act & Assert
        _ = _manager.Invoking(m => m.GetCircuitBreaker(nodeId, null!))
            .Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetCircuitBreaker_WithSameNodeId_ShouldReturnSameInstance()
    {
        // Arrange
        var nodeId = "test-node";

        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        // Act
        var circuitBreaker1 = _manager.GetCircuitBreaker(nodeId, options);
        var circuitBreaker2 = _manager.GetCircuitBreaker(nodeId, options);

        // Assert
        _ = circuitBreaker1.Should().BeSameAs(circuitBreaker2);
    }

    [Fact]
    public void GetCircuitBreaker_WithDifferentNodeIds_ShouldReturnDifferentInstances()
    {
        // Arrange
        var nodeId1 = "test-node-1";
        var nodeId2 = "test-node-2";

        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        // Act
        var circuitBreaker1 = _manager.GetCircuitBreaker(nodeId1, options);
        var circuitBreaker2 = _manager.GetCircuitBreaker(nodeId2, options);

        // Assert
        _ = circuitBreaker1.Should().NotBeSameAs(circuitBreaker2);
    }

    [Fact]
    public void GetCircuitBreaker_WithSameNodeIdDifferentOptions_ShouldReturnFirstInstance()
    {
        // Arrange
        var nodeId = "test-node";

        var options1 = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        var options2 = new PipelineCircuitBreakerOptions(
            5,
            TimeSpan.FromMinutes(2),
            TimeSpan.FromMinutes(10),
            true);

        // Act
        var circuitBreaker1 = _manager.GetCircuitBreaker(nodeId, options1);
        var circuitBreaker2 = _manager.GetCircuitBreaker(nodeId, options2);

        // Assert - Should return the first instance created
        _ = circuitBreaker1.Should().BeSameAs(circuitBreaker2);
    }

    [Fact]
    public async Task GetCircuitBreaker_Concurrently_ShouldBeThreadSafe()
    {
        // Arrange
        var nodeId = "test-node";

        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        var tasks = new List<Task<ICircuitBreaker>>();
        const int taskCount = 10;

        // Act - Get circuit breakers concurrently
        for (var i = 0; i < taskCount; i++)
        {
            tasks.Add(Task.Run(() => _manager.GetCircuitBreaker(nodeId, options)));
        }

        var results = await Task.WhenAll(tasks);

        // Assert
        _ = results.Should().HaveCount(taskCount);

        // All should be the same instance (due to caching)
        var firstInstance = results[0];

        foreach (var result in results)
        {
            _ = result.Should().BeSameAs(firstInstance);
        }
    }

    [Fact]
    public void RemoveCircuitBreaker_WithExistingNodeId_ShouldRemoveAndDisposeCircuitBreaker()
    {
        // Arrange
        var nodeId = "test-node";

        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        var circuitBreaker = _manager.GetCircuitBreaker(nodeId, options);
        _ = circuitBreaker.Should().NotBeNull();

        // Act
        _manager.RemoveCircuitBreaker(nodeId);

        // Assert - Try to get the circuit breaker again, should create a new instance
        var newCircuitBreaker = _manager.GetCircuitBreaker(nodeId, options);
        _ = newCircuitBreaker.Should().NotBeSameAs(circuitBreaker);
    }

    [Fact]
    public void RemoveCircuitBreaker_WithNullNodeId_ShouldThrowArgumentNullException()
    {
        // Arrange & Act & Assert
        _ = _manager.Invoking(m => m.RemoveCircuitBreaker(null!))
            .Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void RemoveCircuitBreaker_WithNonExistentNodeId_ShouldNotThrow()
    {
        // Arrange
        var nodeId = "non-existent-node";

        // Act & Assert - Should not throw even if node doesn't exist
        _ = _manager.Invoking(m => m.RemoveCircuitBreaker(nodeId))
            .Should().NotThrow();
    }

    [Fact]
    public async Task RemoveCircuitBreaker_Concurrently_ShouldBeThreadSafe()
    {
        // Arrange
        var nodeIdPrefix = "test-node-";

        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        // Create multiple circuit breakers
        var circuitBreakers = new List<ICircuitBreaker>();

        for (var i = 0; i < 5; i++)
        {
            var nodeId = $"{nodeIdPrefix}{i}";
            circuitBreakers.Add(_manager.GetCircuitBreaker(nodeId, options));
        }

        // Act - Remove circuit breakers concurrently
        var tasks = new List<Task>();

        for (var i = 0; i < 5; i++)
        {
            var nodeId = $"{nodeIdPrefix}{i}";
            tasks.Add(Task.Run(() => _manager.RemoveCircuitBreaker(nodeId)));
        }

        await Task.WhenAll(tasks);

        // Assert - No exceptions should be thrown
        foreach (var task in tasks)
        {
            _ = task.IsCompletedSuccessfully.Should().BeTrue();
        }
    }

    [Fact]
    public void Dispose_ShouldDisposeAllCircuitBreakers()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        // Create multiple circuit breakers
        _ = _manager.GetCircuitBreaker("node1", options);
        _ = _manager.GetCircuitBreaker("node2", options);
        _ = _manager.GetCircuitBreaker("node3", options);

        // Act
        _manager.Dispose();

        // Assert - Trying to use the manager after disposal should work but circuit breakers should be disposed
        _ = _manager.Invoking(m => m.GetCircuitBreaker("node1", options))
            .Should().NotThrow();
    }

    [Fact]
    public void Dispose_CalledMultipleTimes_ShouldNotThrowException()
    {
        // Arrange
        _manager.Dispose();

        // Act & Assert
        _ = _manager.Invoking(m => m.Dispose()).Should().NotThrow();
    }

    [Fact]
    public void GetCircuitBreaker_ShouldLogCreation()
    {
        // Arrange
        var nodeId = "test-node";

        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        // Act
        _manager.GetCircuitBreaker(nodeId, options);

        // Assert
        A.CallTo(_logger)
            .Where(call => call.Method.Name == "Log" &&
                           call.Arguments.Get<object>(0) != null &&
                           call.Arguments.Get<object>(0)!.ToString() == "Debug")
            .MustHaveHappened();
    }

    [Fact]
    public void RemoveCircuitBreaker_ShouldLogRemoval()
    {
        // Arrange
        var nodeId = "test-node";

        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        _manager.GetCircuitBreaker(nodeId, options);

        // Act
        _manager.RemoveCircuitBreaker(nodeId);

        // Assert
        A.CallTo(_logger)
            .Where(call => call.Method.Name == "Log" &&
                           call.Arguments.Get<object>(0) != null &&
                           call.Arguments.Get<object>(0)!.ToString() == "Debug")
            .MustHaveHappened();
    }

    [Fact]
    public void GetCircuitBreaker_WithDisabledOptions_ShouldCreateCircuitBreaker()
    {
        // Arrange
        var nodeId = "test-node";

        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            false);

        // Act
        var circuitBreaker = _manager.GetCircuitBreaker(nodeId, options);

        // Assert
        _ = circuitBreaker.Should().NotBeNull();
        _ = circuitBreaker.Options.Enabled.Should().BeFalse();
    }

    [Fact]
    public void GetCircuitBreaker_WithDefaultOptions_ShouldCreateCircuitBreaker()
    {
        // Arrange
        var nodeId = "test-node";
        var options = PipelineCircuitBreakerOptions.Default;

        // Act
        var circuitBreaker = _manager.GetCircuitBreaker(nodeId, options);

        // Assert
        _ = circuitBreaker.Should().NotBeNull();
        _ = circuitBreaker.Options.Should().Be(options);
    }

    [Fact]
    public void GetCircuitBreaker_WithDisabledOptions_ShouldReturnSameInstanceForMultipleCalls()
    {
        // Arrange
        var nodeId = "test-node";

        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            false);

        // Act
        var circuitBreaker1 = _manager.GetCircuitBreaker(nodeId, options);
        var circuitBreaker2 = _manager.GetCircuitBreaker(nodeId, options);

        // Assert - Should cache even disabled circuit breakers
        _ = circuitBreaker1.Should().BeSameAs(circuitBreaker2);
    }

    [Fact]
    public async Task MixedOperations_ShouldMaintainCorrectInstances()
    {
        // Arrange
        var nodeId1 = "test-node-1";
        var nodeId2 = "test-node-2";

        var options1 = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            true);

        var options2 = new PipelineCircuitBreakerOptions(
            5,
            TimeSpan.FromMinutes(2),
            TimeSpan.FromMinutes(10),
            true);

        // Act - Mix operations concurrently
        var tasks = new List<Task<ICircuitBreaker>>();

        for (var i = 0; i < 20; i++)
        {
            var nodeId = i % 2 == 0
                ? nodeId1
                : nodeId2;

            var options = i % 2 == 0
                ? options1
                : options2;

            tasks.Add(Task.Run(() => _manager.GetCircuitBreaker(nodeId, options)));
        }

        var results = await Task.WhenAll(tasks);

        // Assert
        _ = results.Should().HaveCount(20);

        // All even indices (0, 2, 4, etc.) should be the same instance
        var evenIndexInstance = results[0];

        for (var i = 0; i < 20; i += 2)
        {
            _ = results[i].Should().BeSameAs(evenIndexInstance);
        }

        // All odd indices (1, 3, 5, etc.) should be the same instance
        var oddIndexInstance = results[1];

        for (var i = 1; i < 20; i += 2)
        {
            _ = results[i].Should().BeSameAs(oddIndexInstance);
        }

        _ = evenIndexInstance.Should().NotBeSameAs(oddIndexInstance);
    }

    [Fact]
    public void TriggerCleanup_ShouldRemoveInactiveCircuitBreakers()
    {
        // Arrange
        var memoryOptions = new CircuitBreakerMemoryManagementOptions(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(20),
            false,
            10);

        using var manager = new CircuitBreakerManager(_logger, memoryOptions);
        var circuitBreaker = manager.GetCircuitBreaker("inactive-node", PipelineCircuitBreakerOptions.Default);
        _ = circuitBreaker.Should().NotBeNull();

        // Allow the inactivity threshold to elapse
        Thread.Sleep(TimeSpan.FromMilliseconds(60));

        // Act
        var removed = manager.TriggerCleanup();

        // Assert
        _ = removed.Should().Be(1);
        _ = manager.GetTrackedCircuitBreakerCount().Should().Be(0);
    }

    [Fact]
    public void GetCircuitBreaker_WhenMaxLimitReached_ShouldEvictLeastRecentlyUsed()
    {
        // Arrange
        var memoryOptions = new CircuitBreakerMemoryManagementOptions(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMinutes(5),
            false,
            2);

        using var manager = new CircuitBreakerManager(_logger, memoryOptions);
        var options = PipelineCircuitBreakerOptions.Default;

        var breakerA = manager.GetCircuitBreaker("node-a", options);
        Thread.Sleep(TimeSpan.FromMilliseconds(25));
        var breakerB = manager.GetCircuitBreaker("node-b", options);

        _ = manager.GetTrackedCircuitBreakerCount().Should().Be(2);

        // Act - request a third circuit breaker to trigger aggressive eviction
        Thread.Sleep(TimeSpan.FromMilliseconds(25));
        var breakerC = manager.GetCircuitBreaker("node-c", options);
        _ = breakerC.Should().NotBeNull();

        // Assert
        _ = manager.GetTrackedCircuitBreakerCount().Should().Be(2);

        // node-b should still use the cached instance (least recently used eviction should remove node-a)
        var breakerBAgain = manager.GetCircuitBreaker("node-b", options);
        _ = breakerBAgain.Should().BeSameAs(breakerB);

        // node-a should be recreated because it was evicted
        var breakerAReplacement = manager.GetCircuitBreaker("node-a", options);
        _ = breakerAReplacement.Should().NotBeSameAs(breakerA);
    }
}
