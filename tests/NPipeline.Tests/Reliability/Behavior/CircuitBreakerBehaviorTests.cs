using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Configuration;
using NPipeline.Execution.CircuitBreaking;

// Tests skipped with a defect ID pin known bugs; the phase that fixes each one removes its skip.
#pragma warning disable xUnit1004

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     Circuit breaker lifetime. Until Phase 2 adds an injectable clock this drives the manager directly, with the
///     30-minute inactivity threshold shrunk to milliseconds.
/// </summary>
public sealed class CircuitBreakerBehaviorTests
{
    [Fact(Skip = Defects.B1)]
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
}
