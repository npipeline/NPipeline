using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Pipeline;

/// <summary>
///     Disposal contract for <see cref="PipelineContext" />: it must be reachable through
///     <see cref="IAsyncDisposable" /> (finding 10), must not hand its dictionaries to a shared pool while still
///     referencing them (finding 11), and must tolerate concurrent registration (finding 12).
/// </summary>
public sealed class PipelineContextDisposalTests
{
    [Fact]
    public async Task Context_IsReachableThroughIAsyncDisposable()
    {
        var context = new PipelineContext(PipelineContextConfiguration.Default);
        var tracker = new TrackingDisposable();
        context.RegisterForDisposal(tracker);

        // Before the fix the class only matched DisposeAsync by pattern, so anything holding the context as an
        // IAsyncDisposable — a DI container, a composite disposable — never disposed it and leaked every
        // registered stream.
        _ = context.Should().BeAssignableTo<IAsyncDisposable>();

        IAsyncDisposable asDisposable = context;
        await asDisposable.DisposeAsync();

        tracker.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task Disposal_RunsInReverseRegistrationOrder()
    {
        // Decorators are registered after the streams they wrap, so LIFO tears the outer layer down first.
        List<string> order = [];
        var context = new PipelineContext(PipelineContextConfiguration.Default);

        context.RegisterForDisposal(new TrackingDisposable(() => order.Add("inner")));
        context.RegisterForDisposal(new TrackingDisposable(() => order.Add("middle")));
        context.RegisterForDisposal(new TrackingDisposable(() => order.Add("outer")));

        await context.DisposeAsync();

        order.Should().Equal("outer", "middle", "inner");
    }

    [Fact]
    public async Task DisposalFailure_DoesNotStopTheRemainingDisposals()
    {
        var context = new PipelineContext(PipelineContextConfiguration.Default);
        var survivor = new TrackingDisposable();

        context.RegisterForDisposal(survivor);
        context.RegisterForDisposal(new ThrowingDisposable());

        var act = () => context.DisposeAsync().AsTask();

        _ = await act.Should().ThrowAsync<AggregateException>();
        survivor.DisposeCount.Should().Be(1, "a failure in one resource must not strand the others");
    }

    [Fact]
    public async Task Disposal_IsIdempotent()
    {
        var context = new PipelineContext(PipelineContextConfiguration.Default);
        var tracker = new TrackingDisposable();
        context.RegisterForDisposal(tracker);

        await context.DisposeAsync();
        await context.DisposeAsync();

        tracker.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task RegistrationAfterDisposal_DisposesTheResourceRatherThanLeakingIt()
    {
        var context = new PipelineContext(PipelineContextConfiguration.Default);
        await context.DisposeAsync();

        var late = new TrackingDisposable();
        context.RegisterForDisposal(late);

        // A second disposal awaits any background work the late registration started, so the disposal is
        // observable rather than unwatched fire-and-forget.
        await context.DisposeAsync();

        late.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task ConcurrentRegistration_LosesNothing()
    {
        // Terminal nodes below a fan-out drain concurrently and each register their own stream. An unguarded
        // List<T>.Add here could drop entries — a leaked stream — or corrupt the backing array outright.
        // Kept deliberately small: this assembly also holds timing-sensitive drain tests, and a heavier load here
        // perturbs them. Four threads contending on the same registration list is enough to expose an unguarded Add.
        const int threads = 4;
        const int perThread = 32;

        var context = new PipelineContext(PipelineContextConfiguration.Default);
        var trackers = Enumerable.Range(0, threads * perThread).Select(static _ => new TrackingDisposable()).ToArray();
        using Barrier gate = new(threads);

        await Task.WhenAll(Enumerable.Range(0, threads).Select(thread => Task.Run(() =>
        {
            gate.SignalAndWait();

            for (var i = 0; i < perThread; i++)
            {
                context.RegisterForDisposal(trackers[(thread * perThread) + i]);
            }
        })));

        await context.DisposeAsync();

        trackers.Should().OnlyContain(static t => t.DisposeCount == 1);
    }

    [Fact]
    public async Task RegistrationRacingDisposal_StillDisposesEverything()
    {
        // The window between "is the context disposed?" and "add to the list" is the one that leaked streams.
        var context = new PipelineContext(PipelineContextConfiguration.Default);
        var trackers = Enumerable.Range(0, 64).Select(static _ => new TrackingDisposable()).ToArray();

        var registering = Task.Run(() =>
        {
            foreach (var tracker in trackers)
            {
                context.RegisterForDisposal(tracker);
            }
        });

        await context.DisposeAsync();
        await registering;

        // Whatever landed after disposal is disposed by the late-registration path; a second disposal drains it.
        await context.DisposeAsync();

        trackers.Should().OnlyContain(static t => t.DisposeCount == 1, "every registered resource must be disposed exactly once");
    }

    /// <summary>
    ///     The context's own dictionaries used to be handed back to a process-wide pool while the context still
    ///     referenced them, so a later read or write reached a dictionary a different run already owned.
    /// </summary>
    [Theory]
    [InlineData(PipelineOptimizationProfile.Default)]
    [InlineData(PipelineOptimizationProfile.HighThroughput)]
    public async Task ContextDictionaries_AreNotSharedBetweenRuns(PipelineOptimizationProfile profile)
    {
        var first = new PipelineContext(new PipelineContextConfiguration(OptimizationProfile: profile));
        var firstItems = first.Items;
        var firstParameters = first.Parameters;
        var firstProperties = first.Properties;

        await first.DisposeAsync();

        var second = new PipelineContext(new PipelineContextConfiguration(OptimizationProfile: profile));

        try
        {
            second.Items.Should().NotBeSameAs(firstItems);
            second.Parameters.Should().NotBeSameAs(firstParameters);
            second.Properties.Should().NotBeSameAs(firstProperties);

            // Writing through the disposed context's reference must not be visible to the live one.
            firstItems["leaked"] = "value";
            second.Items.Should().NotContainKey("leaked");
        }
        finally
        {
            await second.DisposeAsync();
        }
    }

    private sealed class TrackingDisposable(Action? onDispose = null) : IAsyncDisposable
    {
        private int _disposeCount;

        public int DisposeCount => Volatile.Read(ref _disposeCount);

        public ValueTask DisposeAsync()
        {
            _ = Interlocked.Increment(ref _disposeCount);
            onDispose?.Invoke();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingDisposable : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            throw new InvalidOperationException("disposal failed");
        }
    }
}
