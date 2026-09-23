using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Execution.CircuitBreaking;

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     Circuit breaker lifetime. Until Phase 4 moves the breaker onto the pipeline's clock this drives the manager directly, with the
///     30-minute inactivity threshold shrunk to milliseconds.
/// </summary>
public sealed class CircuitBreakerBehaviorTests
{
    [Fact]
    public async Task BreakerInUse_IsNotDisposedByInactivityCleanup()
    {
        var inactivityThreshold = TimeSpan.FromMilliseconds(100);

        using var manager = new CircuitBreakerManager(
            NullLogger.Instance,
            new CircuitBreakerMemoryManagementOptions(
                CleanupInterval: TimeSpan.FromMilliseconds(50),
                InactivityThreshold: inactivityThreshold));

        // A resilient stream looks its breaker up once, then keeps recording outcomes on it for as long as it runs.
        var breaker = manager.GetCircuitBreaker("node", new PipelineCircuitBreakerOptions(5, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)));

        var act = async () =>
        {
            var until = DateTime.UtcNow + inactivityThreshold * 4;

            while (DateTime.UtcNow < until)
            {
                _ = breaker.RecordSuccess();
                _ = manager.TriggerCleanup();
                await Task.Delay(10);
            }
        };

        await act.Should().NotThrowAsync("a breaker that is recording outcomes is in use, not idle");
    }

    [Fact]
    public void QuietBreaker_EvictedByCleanup_KeepsWorkingForTheStreamThatHoldsIt()
    {
        using var manager = new CircuitBreakerManager(
            NullLogger.Instance,
            new CircuitBreakerMemoryManagementOptions(
                CleanupInterval: TimeSpan.FromMilliseconds(1),
                InactivityThreshold: TimeSpan.FromMilliseconds(1),
                EnableAutomaticCleanup: false));

        var breaker = manager.GetCircuitBreaker("node", new PipelineCircuitBreakerOptions(5, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)));

        // A source with no traffic, such as a quiet Kafka topic, leaves the breaker idle past the threshold.
        Thread.Sleep(20);
        manager.TriggerCleanup().Should().Be(1);

        var act = () => breaker.RecordSuccess();

        act.Should().NotThrow("eviction forgets the breaker; it must not dispose one a stream still holds");
    }

    [Fact]
    public async Task EveryTransition_IsReported_IncludingTheTimerDrivenHalfOpen()
    {
        var changes = new System.Collections.Concurrent.ConcurrentQueue<(CircuitState From, CircuitState To)>();
        var halfOpen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var breaker = new CircuitBreaker(
            new PipelineCircuitBreakerOptions(1, TimeSpan.FromMilliseconds(20), TimeSpan.FromMinutes(1)),
            NullLogger.Instance,
            (from, to, reason) =>
            {
                changes.Enqueue((from, to));

                if (to == CircuitState.HalfOpen)
                    _ = halfOpen.TrySetResult();
            });

        _ = breaker.RecordFailure();
        await halfOpen.Task.WaitAsync(TimeSpan.FromSeconds(5));
        _ = breaker.RecordSuccess();

        changes.Should().Equal(
            (CircuitState.Closed, CircuitState.Open),
            (CircuitState.Open, CircuitState.HalfOpen),
            (CircuitState.HalfOpen, CircuitState.Closed));
    }

    [Fact]
    public void AThrowingListener_DoesNotBreakTheBreaker()
    {
        using var breaker = new CircuitBreaker(
            new PipelineCircuitBreakerOptions(1, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1)),
            NullLogger.Instance,
            (_, _, _) => throw new InvalidOperationException("observer bug"));

        var act = () => breaker.RecordFailure();

        act.Should().NotThrow().Which.NewState.Should().Be(CircuitState.Open);
        breaker.State.Should().Be(CircuitState.Open);
    }
}
