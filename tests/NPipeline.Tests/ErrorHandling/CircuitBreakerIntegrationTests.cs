using FakeItEasy;
using FluentAssertions;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

/// <summary>
///     Integration tests for CircuitBreaker functionality within the broader system.
///     Tests circuit breaker behavior with real components and interactions.
/// </summary>
public class CircuitBreakerIntegrationTests
{
    private readonly IPipelineLogger _logger;

    public CircuitBreakerIntegrationTests()
    {
        _logger = A.Fake<IPipelineLogger>();
    }

    [Fact]
    public void CircuitBreaker_WithConsecutiveFailures_ShouldTripAfterThreshold()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            ThresholdType: CircuitBreakerThresholdType.ConsecutiveFailures);

        using var circuitBreaker = new CircuitBreaker(options, _logger);

        // Act & Assert
        circuitBreaker.State.Should().Be(CircuitBreakerState.Closed);
        circuitBreaker.CanExecute().Should().BeTrue();

        // First failure - should remain closed
        var result1 = circuitBreaker.RecordFailure();
        result1.Allowed.Should().BeTrue();
        result1.StateChanged.Should().BeFalse();
        result1.NewState.Should().Be(CircuitBreakerState.Closed);
        circuitBreaker.State.Should().Be(CircuitBreakerState.Closed);

        // Second failure - should remain closed
        var result2 = circuitBreaker.RecordFailure();
        result2.Allowed.Should().BeTrue();
        result2.StateChanged.Should().BeFalse();
        result2.NewState.Should().Be(CircuitBreakerState.Closed);
        circuitBreaker.State.Should().Be(CircuitBreakerState.Closed);

        // Third failure - should trip to open
        var result3 = circuitBreaker.RecordFailure();
        result3.Allowed.Should().BeFalse(); // Not allowed when tripping to Open
        result3.StateChanged.Should().BeTrue();
        result3.NewState.Should().Be(CircuitBreakerState.Open);
        circuitBreaker.State.Should().Be(CircuitBreakerState.Open);
        circuitBreaker.CanExecute().Should().BeFalse();
    }

    [Fact]
    public async Task CircuitBreaker_WithRecovery_ShouldTransitionToHalfOpenAndClosed()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            2,
            TimeSpan.FromMilliseconds(100), // Short duration for testing
            TimeSpan.FromMinutes(5),
            ThresholdType: CircuitBreakerThresholdType.ConsecutiveFailures);

        using var circuitBreaker = new CircuitBreaker(options, _logger);

        // Trip the circuit breaker
        circuitBreaker.RecordFailure();
        circuitBreaker.RecordFailure();

        circuitBreaker.State.Should().Be(CircuitBreakerState.Open);
        circuitBreaker.CanExecute().Should().BeFalse();

        // Act - Wait for recovery timeout
        await Task.Delay(150);

        // Should now be in half-open state
        circuitBreaker.State.Should().Be(CircuitBreakerState.HalfOpen);
        circuitBreaker.CanExecute().Should().BeTrue();

        // Act - Record success to close the circuit
        var result = circuitBreaker.RecordSuccess();
        result.Allowed.Should().BeTrue();
        result.StateChanged.Should().BeTrue();
        result.NewState.Should().Be(CircuitBreakerState.Closed);
        circuitBreaker.State.Should().Be(CircuitBreakerState.Closed);
        circuitBreaker.CanExecute().Should().BeTrue();
    }

    [Fact]
    public void CircuitBreaker_WithStatistics_ShouldTrackCorrectly()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            5,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            TrackOperationsInWindow: true);

        using var circuitBreaker = new CircuitBreaker(options, _logger);

        // Act - Record various operations
        circuitBreaker.RecordSuccess();
        circuitBreaker.RecordSuccess();
        circuitBreaker.RecordFailure();
        circuitBreaker.RecordSuccess();
        circuitBreaker.RecordFailure();
        circuitBreaker.RecordFailure();

        // Assert - Check statistics
        var stats = circuitBreaker.GetStatistics();
        stats.TotalOperations.Should().Be(6);
        stats.SuccessCount.Should().Be(3);
        stats.FailureCount.Should().Be(3);
        stats.FailureRate.Should().Be(0.5);
    }

    [Fact]
    public void CircuitBreaker_WithDisabledTracking_ShouldOnlyUseConsecutiveFailures()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            TrackOperationsInWindow: false,
            ThresholdType: CircuitBreakerThresholdType.ConsecutiveFailures);

        using var circuitBreaker = new CircuitBreaker(options, _logger);

        // Act - Record mixed operations
        circuitBreaker.RecordSuccess();
        circuitBreaker.RecordFailure();
        circuitBreaker.RecordSuccess();
        circuitBreaker.RecordFailure();
        circuitBreaker.RecordSuccess();

        // Should still be closed as we don't have 3 consecutive failures
        circuitBreaker.State.Should().Be(CircuitBreakerState.Closed);
        circuitBreaker.CanExecute().Should().BeTrue();

        // Now record 3 consecutive failures
        circuitBreaker.RecordFailure();
        circuitBreaker.RecordFailure();
        var result = circuitBreaker.RecordFailure();

        // Should trip now
        result.Allowed.Should().BeFalse(); // Not allowed when tripping to Open
        result.StateChanged.Should().BeTrue();
        result.NewState.Should().Be(CircuitBreakerState.Open);
        circuitBreaker.State.Should().Be(CircuitBreakerState.Open);
    }

    [Fact]
    public void CircuitBreaker_WithRollingWindow_ShouldTrackWindowCorrectly()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            TrackOperationsInWindow: true,
            ThresholdType: CircuitBreakerThresholdType.RollingWindowCount);

        using var circuitBreaker = new CircuitBreaker(options, _logger);

        // Act - Record failures to reach threshold
        circuitBreaker.RecordFailure();
        circuitBreaker.RecordFailure();
        var result = circuitBreaker.RecordFailure();

        // Should trip on third failure
        result.Allowed.Should().BeFalse(); // Not allowed when tripping to Open
        result.StateChanged.Should().BeTrue();
        result.NewState.Should().Be(CircuitBreakerState.Open);
        circuitBreaker.State.Should().Be(CircuitBreakerState.Open);
    }

    [Fact]
    public void CircuitBreaker_WithRateThreshold_ShouldCalculateFailureRateCorrectly()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            5, // Minimum operations
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            TrackOperationsInWindow: true,
            ThresholdType: CircuitBreakerThresholdType.RollingWindowRate,
            FailureRateThreshold: 0.6); // 60% failure rate threshold

        using var circuitBreaker = new CircuitBreaker(options, _logger);

        // Act - Record operations with 40% failure rate (not enough to trip)
        circuitBreaker.RecordSuccess();
        circuitBreaker.RecordFailure();
        circuitBreaker.RecordSuccess();
        circuitBreaker.RecordFailure();
        circuitBreaker.RecordSuccess();

        // Should not trip yet as we have 40% failure rate (2 failures out of 5 operations)
        circuitBreaker.State.Should().Be(CircuitBreakerState.Closed);

        // Add two more failures to exceed threshold (now 57% failure rate)
        var result1 = circuitBreaker.RecordFailure();
        var result2 = circuitBreaker.RecordFailure();

        // Should still not trip as we're at 57% (4 failures out of 7 operations) and need 60%
        result1.Allowed.Should().BeTrue();
        result2.Allowed.Should().BeTrue();
        circuitBreaker.State.Should().Be(CircuitBreakerState.Closed);

        // Add one more failure to exceed threshold (now 62.5% failure rate)
        var result3 = circuitBreaker.RecordFailure();

        // Should trip now (5 failures out of 8 operations = 62.5% failure rate)
        result3.Allowed.Should().BeFalse(); // Not allowed when tripping to Open
        result3.StateChanged.Should().BeTrue();
        result3.NewState.Should().Be(CircuitBreakerState.Open);
        circuitBreaker.State.Should().Be(CircuitBreakerState.Open);
    }

    [Fact]
    public void CircuitBreaker_WithMultipleNodes_ShouldTrackFailuresCorrectly()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            ThresholdType: CircuitBreakerThresholdType.ConsecutiveFailures);

        using var circuitBreaker = new CircuitBreaker(options, _logger);

        // Act - Simulate failures from different sources
        var result1 = circuitBreaker.RecordFailure(); // Node 1
        var result2 = circuitBreaker.RecordFailure(); // Node 2
        var result3 = circuitBreaker.RecordFailure(); // Node 3

        // Assert - Should trip on third consecutive failure
        result1.Allowed.Should().BeTrue();
        result1.StateChanged.Should().BeFalse();
        result1.NewState.Should().Be(CircuitBreakerState.Closed);

        result2.Allowed.Should().BeTrue();
        result2.StateChanged.Should().BeFalse();
        result2.NewState.Should().Be(CircuitBreakerState.Closed);

        result3.Allowed.Should().BeFalse(); // Not allowed when tripping to Open
        result3.StateChanged.Should().BeTrue();
        result3.NewState.Should().Be(CircuitBreakerState.Open);
        circuitBreaker.State.Should().Be(CircuitBreakerState.Open);
    }

    #region Resilient Execution Strategy Integration Tests

    [Fact]
    public async Task ResilientExecution_WithCircuitBreakerEnabled_ShouldCreateCircuitBreakerManager()
    {
        // Arrange
        var context = CreatePipelineContextWithCircuitBreaker();
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);
        await using var input = new InMemoryDataPipe<int>([1, 2, 3], "test-input");
        var node = new TestTransformNode();

        // Act
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Assert
            result.Should().NotBeNull();
            context.Items.Should().ContainKey(PipelineContextKeys.CircuitBreakerManager);
            context.Items[PipelineContextKeys.CircuitBreakerManager].Should().BeAssignableTo<ICircuitBreakerManager>();
        }
    }

    [Fact]
    public async Task ResilientExecution_WithCircuitBreakerDisabled_ShouldNotCreateCircuitBreakerManager()
    {
        // Arrange
        var context = CreatePipelineContextWithoutCircuitBreaker();
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);
        await using var input = new InMemoryDataPipe<int>([1, 2, 3], "test-input");
        var node = new TestTransformNode();

        // Act
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Assert
            result.Should().NotBeNull();
            context.Items.Should().NotContainKey(PipelineContextKeys.CircuitBreakerManager);
        }
    }

    [Fact]
    public async Task ResilientExecution_WithCircuitBreakerTripped_ShouldThrowCircuitBreakerOpenException()
    {
        // Arrange
        var context = CreatePipelineContextWithCircuitBreaker();
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);

        // Increase input items to ensure we have enough failures to trip circuit breaker
        await using var input = new InMemoryDataPipe<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "test-input");
        var node = new FailingTransformNode(10); // Fail more times to ensure circuit breaker trips

        // Act & Assert
        // ExecuteAsync returns a lazy IDataPipe, so we need to consume it to trigger circuit breaker
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Try to consume the result - this should trigger the circuit breaker
            var exception = await Assert.ThrowsAsync<NodeExecutionException>(async () =>
            {
                var output = new List<string>();

                await foreach (var item in result.WithCancellation(CancellationToken.None))
                {
                    output.Add(item);
                }
            });

            // Verify the inner exception is CircuitBreakerOpenException
            exception.InnerException.Should().BeOfType<CircuitBreakerOpenException>();
        }
    }

    [Fact]
    public async Task ResilientExecution_WhenCircuitBreakerRecovers_ShouldAllowExecutionAfterOpenDuration()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            1,
            TimeSpan.FromMilliseconds(200),
            TimeSpan.FromSeconds(1),
            true,
            CircuitBreakerThresholdType.ConsecutiveFailures,
            0.5,
            1,
            2);

        var context = CreatePipelineContextWithCircuitBreaker(options);
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);
        var node = new RecoveringTransformNode(1);

        using (context.ScopedNode("recovery-node"))
        {
            // Act 1: trigger breaker
            await using (var initialInput = new InMemoryDataPipe<int>([1], "first"))
            {
                await using var result = await resilientStrategy.ExecuteAsync(initialInput, node, context, CancellationToken.None);

                await Assert.ThrowsAsync<NodeExecutionException>(async () =>
                {
                    await foreach (var _ in result.WithCancellation(CancellationToken.None))
                    {
                        // exhaust
                    }
                });
            }

            // Allow the breaker to transition to half-open by polling its state
            // Wait for initial delay plus additional buffer for timer callback execution
            await Task.Delay(options.OpenDuration + TimeSpan.FromMilliseconds(500));

            // Poll to ensure the circuit breaker has transitioned to HalfOpen
            var manager = context.Items[PipelineContextKeys.CircuitBreakerManager] as ICircuitBreakerManager;
            var circuitBreaker = manager?.GetCircuitBreaker("recovery-node", options);

            // Add a small retry loop to account for timing variations
            var maxRetries = 5;
            var retryCount = 0;

            while (retryCount < maxRetries && circuitBreaker?.State != CircuitBreakerState.HalfOpen)
            {
                await Task.Delay(50);
                retryCount++;
            }

            // Act 2: half-open should permit execution and recover to closed
            await using var recoveryInput = new InMemoryDataPipe<int>([2], "second");
            await using var recoveryResult = await resilientStrategy.ExecuteAsync(recoveryInput, node, context, CancellationToken.None);

            var outputs = new List<string>();

            await foreach (var item in recoveryResult.WithCancellation(CancellationToken.None))
            {
                outputs.Add(item);
            }

            // Assert
            outputs.Should().HaveCount(1);
            outputs[0].Should().Be("processed-2");
        }
    }

    #endregion

    #region Helper Methods

    private static PipelineContext CreatePipelineContextWithCircuitBreaker(
        PipelineCircuitBreakerOptions? options = null,
        CircuitBreakerMemoryManagementOptions? memoryOptions = null)
    {
        var context = new PipelineContext(
            PipelineContextConfiguration.Default with { PipelineErrorHandler = new TestErrorHandler(PipelineErrorDecision.RestartNode) });

        context.Items[PipelineContextKeys.CircuitBreakerOptions] = (options ?? new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5))).Validate();

        if (memoryOptions is not null)
            context.Items[PipelineContextKeys.CircuitBreakerMemoryOptions] = memoryOptions.Validate();

        return context;
    }

    private static PipelineContext CreatePipelineContextWithoutCircuitBreaker()
    {
        return new PipelineContext(
            PipelineContextConfiguration.Default with { PipelineErrorHandler = new TestErrorHandler(PipelineErrorDecision.RestartNode) });
    }

    #endregion

    #region Test Helpers

    private sealed class TestTransformNode : TransformNode<int, string>
    {
        public override async Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            return $"processed-{item}";
        }
    }

    private sealed class FailingTransformNode(int failCount = 1) : TransformNode<int, string>
    {
        private int _currentAttempt;

        public override async Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            _currentAttempt++;

            return _currentAttempt > failCount
                ? $"processed-{item}"
                : throw new InvalidOperationException($"Simulated failure {_currentAttempt}");
        }
    }

    private sealed class RecoveringTransformNode(int failuresBeforeSuccess) : TransformNode<int, string>
    {
        private int _attempts;

        public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempts++;

            return _attempts > failuresBeforeSuccess
                ? Task.FromResult($"processed-{item}")
                : throw new InvalidOperationException($"Simulated failure {_attempts}");
        }
    }

    private sealed class TestErrorHandler(PipelineErrorDecision decision) : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception error, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(decision);
        }
    }

    #endregion
}
