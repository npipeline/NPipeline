using FakeItEasy;
using FluentAssertions;
using NPipeline.Configuration;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Observability.Logging;

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
}
