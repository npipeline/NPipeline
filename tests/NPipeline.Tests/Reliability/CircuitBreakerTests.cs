using AwesomeAssertions;
using Microsoft.Extensions.Time.Testing;
using NPipeline.Execution;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Reliability;

namespace NPipeline.Tests.Reliability;

/// <summary>
///     The breaker's state machine, driven directly on a fake clock.
/// </summary>
public sealed class CircuitBreakerTests
{
    private readonly FakeTimeProvider _time = new();

    [Fact]
    public void ConsecutiveTransientFailures_OpenTheBreaker()
    {
        var breaker = Create(new CircuitBreakerOptions { ConsecutiveFailures = 3 });

        Fail(breaker).Occurred.Should().BeFalse();
        Fail(breaker).Occurred.Should().BeFalse();
        var transition = Fail(breaker);

        transition.Should().Match<BreakerTransition>(t => t.From == CircuitState.Closed && t.To == CircuitState.Open);
        breaker.State.Should().Be(CircuitState.Open);
        breaker.TryAcquire(out _, out _).Should().BeFalse();
    }

    [Fact]
    public void ASuccess_ResetsTheConsecutiveCount()
    {
        var breaker = Create(new CircuitBreakerOptions { ConsecutiveFailures = 2 });

        _ = Fail(breaker);
        Succeed(breaker);
        _ = Fail(breaker);

        breaker.State.Should().Be(CircuitState.Closed);
    }

    [Fact]
    public void AReleasedAttempt_NeitherCountsNorResets()
    {
        var breaker = Create(new CircuitBreakerOptions { ConsecutiveFailures = 2 });

        _ = Fail(breaker);
        breaker.TryAcquire(out var permit, out _).Should().BeTrue();
        breaker.Release(permit);
        _ = Fail(breaker);

        breaker.State.Should().Be(CircuitState.Open, "a released attempt says nothing about the dependency either way");
    }

    [Fact]
    public void OpenBreaker_HalfOpensOnceTheOpenDurationHasPassed()
    {
        var breaker = Create(new CircuitBreakerOptions { ConsecutiveFailures = 1, OpenDuration = TimeSpan.FromSeconds(30) });
        _ = Fail(breaker);

        _time.Advance(TimeSpan.FromSeconds(29));
        breaker.TryAcquire(out _, out _).Should().BeFalse();
        breaker.TimeUntilHalfOpen().Should().Be(TimeSpan.FromSeconds(1));

        _time.Advance(TimeSpan.FromSeconds(1));
        breaker.TryAcquire(out var permit, out var transition).Should().BeTrue();

        permit.IsProbe.Should().BeTrue();
        transition.Should().Match<BreakerTransition>(t => t.From == CircuitState.Open && t.To == CircuitState.HalfOpen);
    }

    [Fact]
    public void HalfOpenBreaker_AdmitsOnlyHalfOpenProbesAtATime()
    {
        var breaker = HalfOpen(new CircuitBreakerOptions { ConsecutiveFailures = 1, HalfOpenProbes = 2, ProbeSuccesses = 3 }, out var first);

        breaker.TryAcquire(out var second, out _).Should().BeTrue();
        breaker.TryAcquire(out _, out _).Should().BeFalse("both probe slots are in use");

        _ = breaker.RecordSuccess(first);
        breaker.TryAcquire(out _, out _).Should().BeTrue("a finished probe frees its slot");
        second.IsProbe.Should().BeTrue();
    }

    [Fact]
    public void ProbeSuccesses_CloseTheBreaker()
    {
        var breaker = HalfOpen(new CircuitBreakerOptions { ConsecutiveFailures = 1, ProbeSuccesses = 2 }, out var first);

        breaker.RecordSuccess(first).Occurred.Should().BeFalse();
        breaker.TryAcquire(out var second, out _).Should().BeTrue();
        var transition = breaker.RecordSuccess(second);

        transition.Should().Match<BreakerTransition>(t => t.From == CircuitState.HalfOpen && t.To == CircuitState.Closed);
        breaker.State.Should().Be(CircuitState.Closed);
    }

    [Fact]
    public void AFailedProbe_ReopensTheBreaker_ForAnotherFullOpenDuration()
    {
        var breaker = HalfOpen(new CircuitBreakerOptions { ConsecutiveFailures = 1, OpenDuration = TimeSpan.FromSeconds(10) }, out var probe);

        var transition = breaker.RecordFailure(probe);

        transition.Should().Match<BreakerTransition>(t => t.From == CircuitState.HalfOpen && t.To == CircuitState.Open);
        breaker.TimeUntilHalfOpen().Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void AReleasedProbe_FreesItsSlotWithoutClosing()
    {
        var breaker = HalfOpen(new CircuitBreakerOptions { ConsecutiveFailures = 1 }, out var probe);

        breaker.Release(probe);

        breaker.State.Should().Be(CircuitState.HalfOpen);
        breaker.TryAcquire(out var next, out _).Should().BeTrue();
        next.IsProbe.Should().BeTrue();
    }

    [Fact]
    public void AnOutcomeFromBeforeATransition_IsIgnored()
    {
        var breaker = Create(new CircuitBreakerOptions { ConsecutiveFailures = 1 });
        breaker.TryAcquire(out var inFlight, out _).Should().BeTrue();

        _ = Fail(breaker);
        var transition = breaker.RecordSuccess(inFlight);

        transition.Occurred.Should().BeFalse();
        breaker.State.Should().Be(CircuitState.Open, "an attempt admitted before the breaker opened is not a probe");
    }

    [Fact]
    public void FailureRate_TripsOnlyOnceMinimumCallsWereSeen()
    {
        var breaker = Create(new CircuitBreakerOptions { ConsecutiveFailures = null, FailureRate = 0.5, MinimumCalls = 4 });

        _ = Fail(breaker);
        _ = Fail(breaker);
        Succeed(breaker);
        breaker.State.Should().Be(CircuitState.Closed, "three calls are fewer than MinimumCalls");

        var transition = Fail(breaker);

        transition.To.Should().Be(CircuitState.Open, "three of four calls failed");
    }

    [Fact]
    public void FailureRate_ForgetsOutcomesOlderThanTheWindow()
    {
        var breaker = Create(new CircuitBreakerOptions
        {
            ConsecutiveFailures = null, FailureRate = 0.5, MinimumCalls = 4, Window = TimeSpan.FromSeconds(10),
        });

        _ = Fail(breaker);
        _ = Fail(breaker);
        _ = Fail(breaker);
        _time.Advance(TimeSpan.FromSeconds(11));

        for (var i = 0; i < 3; i++)
        {
            Succeed(breaker);
        }

        Fail(breaker).Occurred.Should().BeFalse("the early failures fell out of the window, leaving one failure in four calls");
    }

    [Fact]
    public void AdmissionChanged_CompletesOnATransition()
    {
        var breaker = Create(new CircuitBreakerOptions { ConsecutiveFailures = 1 });
        var signal = breaker.AdmissionChanged;

        _ = Fail(breaker);

        signal.IsCompleted.Should().BeTrue();
        breaker.AdmissionChanged.IsCompleted.Should().BeFalse();
    }

    [Fact]
    public async Task ConcurrentUse_NeverExceedsTheProbeLimit()
    {
        var breaker = Create(new CircuitBreakerOptions { ConsecutiveFailures = 5, HalfOpenProbes = 3, OpenDuration = TimeSpan.FromMilliseconds(1) });
        var probesInFlight = 0;
        var maxProbesInFlight = 0;

        var workers = Enumerable.Range(0, 8).Select(worker => Task.Run(() =>
        {
            for (var i = 0; i < 5_000; i++)
            {
                if (i % 50 == 0 && worker == 0)
                    _time.Advance(TimeSpan.FromMilliseconds(1));

                if (!breaker.TryAcquire(out var permit, out _))
                    continue;

                if (permit.IsProbe)
                {
                    var now = Interlocked.Increment(ref probesInFlight);
                    InterlockedMax(ref maxProbesInFlight, now);
                    Thread.SpinWait(20);

                    // Counted down before the outcome frees the breaker's slot, so this count never overstates it.
                    _ = Interlocked.Decrement(ref probesInFlight);
                }

                _ = (i + worker) % 3 == 0
                    ? breaker.RecordFailure(permit)
                    : breaker.RecordSuccess(permit);
            }
        })).ToArray();

        await Task.WhenAll(workers);

        maxProbesInFlight.Should().BeLessThanOrEqualTo(3);
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData(0, null)]
    [InlineData(null, 0.0)]
    [InlineData(null, 1.5)]
    public void Options_WithoutAValidTripCondition_AreRejected(int? consecutive, double? rate)
    {
        var options = new CircuitBreakerOptions { ConsecutiveFailures = consecutive, FailureRate = rate };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Registry_ReturnsTheSameBreaker_WhileTheOptionsAreUnchanged()
    {
        var registry = new CircuitBreakerRegistry();
        var options = PipelineResilienceOptions.None with { CircuitBreaker = CircuitBreakerOptions.Default, Time = _time };

        var first = registry.Resolve("node", options);
        var again = registry.Resolve("node", options with { OnItemFailure = ItemFailureAction.Skip });
        var changed = registry.Resolve("node", options with { CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 9 } });

        again.Should().BeSameAs(first);
        changed.Should().NotBeSameAs(first);
        registry.Resolve("other", options).Should().NotBeSameAs(first);
        registry.Resolve("node", PipelineResilienceOptions.None).Should().BeNull();
    }

    private CircuitBreaker Create(CircuitBreakerOptions options) => new("node", options, _time);

    private CircuitBreaker HalfOpen(CircuitBreakerOptions options, out BreakerPermit probe)
    {
        var breaker = Create(options);

        while (breaker.State == CircuitState.Closed)
        {
            _ = Fail(breaker);
        }

        _time.Advance(options.OpenDuration);
        breaker.TryAcquire(out probe, out _).Should().BeTrue();
        return breaker;
    }

    private static BreakerTransition Fail(CircuitBreaker breaker)
    {
        breaker.TryAcquire(out var permit, out _).Should().BeTrue();
        return breaker.RecordFailure(permit);
    }

    private static void Succeed(CircuitBreaker breaker)
    {
        breaker.TryAcquire(out var permit, out _).Should().BeTrue();
        _ = breaker.RecordSuccess(permit);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;

        do
        {
            current = Volatile.Read(ref target);

            if (value <= current)
                return;
        } while (Interlocked.CompareExchange(ref target, value, current) != current);
    }
}
