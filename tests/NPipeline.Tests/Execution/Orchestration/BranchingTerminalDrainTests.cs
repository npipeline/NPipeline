using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataStreams;
using NPipeline.DataFlow.Windowing;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Execution.Orchestration;

/// <summary>
///     Covers how a fan-out node's terminals are drained.
///     <para>
///         A fan-out node feeds its subscribers through a single multicast pump that only advances once every
///         subscriber has accepted the current item. Draining the terminals one after another therefore stalls the
///         pump: a bounded per-subscriber buffer deadlocks as soon as it fills, and an unbounded one grows to hold the
///         whole stream. Terminals below a fan-out must drain together.
///     </para>
/// </summary>
public sealed class BranchingTerminalDrainTests
{
    private const int SourceItemCount = 2_000;
    private const int BranchBufferCapacity = 16;

    private static readonly TimeSpan DeadlockTimeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task BoundedBranchBuffer_CompletesInsteadOfDeadlocking()
    {
        var (context, recorder) = CreateRun();

        var run = PipelineRunner.Create().RunAsync<BoundedBranchPipeline>(context, CancellationToken.None);
        var finished = await Task.WhenAny(run, Task.Delay(DeadlockTimeout));

        _ = finished.Should().BeSameAs(run, "a bounded branch buffer must not stall the multicast pump");
        await run;
    }

    [Fact]
    public async Task BoundedBranchBuffer_DeliversEveryItemToEverySink()
    {
        var (context, recorder) = CreateRun();

        await PipelineRunner.Create().RunAsync<BoundedBranchPipeline>(context, CancellationToken.None);

        _ = recorder.FirstSinkCount.Should().Be(SourceItemCount);
        _ = recorder.SecondSinkCount.Should().Be(SourceItemCount);
    }

    [Fact]
    public async Task BoundedBranchBuffer_AppliesBackpressureToTheSource()
    {
        var (context, recorder) = CreateRun();

        await PipelineRunner.Create().RunAsync<BoundedBranchPipeline>(context, CancellationToken.None);

        // With real backpressure the source may only run a buffer's worth ahead of the slowest consumer.
        // The bound is deliberately generous: it asserts "bounded", not an exact scheduling outcome.
        _ = recorder.ProducedWhenFirstItemObserved.Should()
            .BeInRange(1, BranchBufferCapacity * 20, "a bounded buffer must stop the source running away");
    }

    [Fact]
    public async Task UnboundedBranchBuffer_DoesNotPrebufferTheWholeStream()
    {
        var (context, recorder) = CreateRun();

        await PipelineRunner.Create().RunAsync<UnboundedBranchPipeline>(context, CancellationToken.None);

        _ = recorder.FirstSinkCount.Should().Be(SourceItemCount);
        _ = recorder.SecondSinkCount.Should().Be(SourceItemCount);

        _ = recorder.ProducedWhenFirstItemObserved.Should()
            .BeLessThan(SourceItemCount / 4, "consumers must start draining before the source has run to completion");
    }

    [Fact]
    public async Task LinearPipeline_StillRunsToCompletion()
    {
        var (context, recorder) = CreateRun();

        await PipelineRunner.Create().RunAsync<LinearPipeline>(context, CancellationToken.None);

        _ = recorder.FirstSinkCount.Should().Be(SourceItemCount, "a graph without a fan-out keeps the sequential path");
    }

    [Fact]
    public async Task FailingSinkBelowAFanOut_StillFailsTheRun()
    {
        var (context, _) = CreateRun();

        var act = async () => await PipelineRunner.Create().RunAsync<FailingBranchPipeline>(context, CancellationToken.None);

        _ = await act.Should().ThrowAsync<Exception>("a terminal failure must surface even when terminals run together");
    }

    private sealed class IgnoringSink : SinkNode<int>
    {
        public override Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class DisposeBeforeReadSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await using var e = input.GetAsyncEnumerator(cancellationToken);
        }
    }

    private sealed class IgnoringBranchPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<YieldingSource, int>("source");
            _ = builder.Connect(source, builder.AddSink<FirstSink, int>("first"));
            _ = builder.Connect(source, builder.AddSink<IgnoringSink, int>("ignoring"));
            _ = builder.WithBranchOptions("source", new BranchOptions(BranchBufferCapacity));
        }
    }

    private sealed class DisposeBeforeReadBranchPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<YieldingSource, int>("source");
            _ = builder.Connect(source, builder.AddSink<FirstSink, int>("first"));
            _ = builder.Connect(source, builder.AddSink<DisposeBeforeReadSink, int>("dispose-before-read"));
            _ = builder.WithBranchOptions("source", new BranchOptions(BranchBufferCapacity));
        }
    }

    [Fact]
    public async Task SinkThatIgnoresItsInput_DoesNotStallSiblings()
    {
        var (context, recorder) = CreateRun();
        var run = PipelineRunner.Create().RunAsync<IgnoringBranchPipeline>(context, CancellationToken.None);
        (await Task.WhenAny(run, Task.Delay(DeadlockTimeout))).Should().BeSameAs(run);
        await run;
        recorder.FirstSinkCount.Should().Be(SourceItemCount);
    }

    [Fact]
    public async Task SinkThatDisposesBeforeFirstMoveNext_DoesNotStallSiblings()
    {
        var (context, recorder) = CreateRun();
        var run = PipelineRunner.Create().RunAsync<DisposeBeforeReadBranchPipeline>(context, CancellationToken.None);
        (await Task.WhenAny(run, Task.Delay(DeadlockTimeout))).Should().BeSameAs(run);
        await run;
        recorder.FirstSinkCount.Should().Be(SourceItemCount);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AggregateBelowAFanOut_ReceivesEveryItem(bool bounded)
    {
        var (context, recorder) = CreateRun();

        var run = PipelineRunner.Create().RunAsync(new AggregateBranchPipeline(bounded), context, CancellationToken.None);
        (await Task.WhenAny(run, Task.Delay(DeadlockTimeout))).Should().BeSameAs(run);
        await run;

        recorder.FirstSinkCount.Should().Be(SourceItemCount);
        recorder.AggregatedCount.Should().Be(SourceItemCount, "the aggregate's lazy output is read after the aggregate node executed");
    }

    [Fact]
    public async Task SinkBelowATransformThatIgnoresItsInput_DoesNotStallSiblings()
    {
        var (context, recorder) = CreateRun();

        var run = PipelineRunner.Create().RunAsync<IgnoringChainPipeline>(context, CancellationToken.None);
        (await Task.WhenAny(run, Task.Delay(DeadlockTimeout))).Should().BeSameAs(run);
        await run;

        recorder.FirstSinkCount.Should().Be(SourceItemCount);
    }

    [Fact]
    public async Task RetriedSinkBelowAFanOut_ReadsItsBranchOnTheRetry()
    {
        var (context, recorder) = CreateRun();

        var run = PipelineRunner.Create().RunAsync<RetriedSinkBranchPipeline>(context, CancellationToken.None);
        (await Task.WhenAny(run, Task.Delay(DeadlockTimeout))).Should().BeSameAs(run);
        await run;

        recorder.FirstSinkCount.Should().Be(SourceItemCount);
        recorder.FlakyAttempts.Should().Be(2);
        recorder.SecondSinkCount.Should().Be(SourceItemCount, "the retry must read the branch the failed attempt never touched");
    }

    private sealed class CountingAggregate()
        : AggregateNode<int, int, int>(new AggregateNodeConfiguration<int>(WindowAssigner.Tumbling(TimeSpan.FromDays(1))))
    {
        public override int GetKey(int item) => 0;

        public override int CreateAccumulator() => 0;

        public override int Accumulate(int accumulator, int item) => accumulator + 1;
    }

    private sealed class AggregateResultSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            // Arrival-time windows aligned on UTC midnight can split a run in two, so sum the window counts.
            await foreach (var count in input.WithCancellation(cancellationToken))
            {
                DrainRecorder.For(context).RecordAggregated(count);
            }
        }
    }

    private sealed class AggregateBranchPipeline(bool bounded) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<YieldingSource, int>("source");
            var aggregate = builder.AddAggregate<CountingAggregate, int, int, int>("aggregate");
            _ = builder.Connect(source, aggregate);
            _ = builder.Connect(aggregate, builder.AddSink<AggregateResultSink, int>("aggregate-sink"));
            _ = builder.Connect(source, builder.AddSink<FirstSink, int>("first"));

            if (bounded)
                _ = builder.WithBranchOptions("source", new BranchOptions(BranchBufferCapacity));
        }
    }

    private sealed class PassThroughTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(item);
    }

    private sealed class IgnoringChainPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<YieldingSource, int>("source");
            var transform = builder.AddTransform<PassThroughTransform, int, int>("transform");
            _ = builder.Connect(source, transform);
            _ = builder.Connect(transform, builder.AddSink<IgnoringSink, int>("ignoring"));
            _ = builder.Connect(source, builder.AddSink<FirstSink, int>("first"));
            _ = builder.WithBranchOptions("source", new BranchOptions(BranchBufferCapacity));
        }
    }

    /// <summary>
    ///     Fails its first attempt with a transient error before reading, then counts every item.
    /// </summary>
    private sealed class FlakyBeforeReadSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            var recorder = DrainRecorder.For(context);

            if (recorder.RecordFlakyAttempt() == 1)
                throw new TimeoutException("transient, before reading");

            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                recorder.RecordSecondSinkItem();
            }
        }
    }

    private sealed class RetriedSinkBranchPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<YieldingSource, int>("source");
            var flaky = builder.AddSink<FlakyBeforeReadSink, int>("flaky");
            _ = builder.Connect(source, flaky);
            _ = builder.Connect(source, builder.AddSink<FirstSink, int>("first"));
            _ = builder.WithBranchOptions("source", new BranchOptions(BranchBufferCapacity));
            _ = builder.WithResilience(flaky, o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 1, Backoff = RetryBackoff.None } });
        }
    }

    private sealed class SlowDisposableSink : SinkNode<int>, IAsyncDisposable
    {
        public volatile bool Consuming;
        public volatile bool DisposedWhileConsuming;

        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            Consuming = true;

            try
            {
                // Ignores the token on purpose: it stands for a non-cancellable flush, and exercises the shutdown grace.
                await Task.Delay(500, CancellationToken.None);

                await foreach (var _ in input.WithCancellation(cancellationToken))
                {
                }
            }
            finally
            {
                Consuming = false;
            }
        }

        public ValueTask DisposeAsync()
        {
            DisposedWhileConsuming |= Consuming;
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task FailingTerminal_CancelsSiblingsBeforeCleanupDisposesThem()
    {
        var (context, _) = CreateRun();
        var slow = new SlowDisposableSink();
        var contextRef = context;

        var run = PipelineRunner.Create().RunAsync(new SlowAndThrowingPipeline(slow), contextRef, CancellationToken.None);
        var finished = await Task.WhenAny(run, Task.Delay(DeadlockTimeout));

        _ = finished.Should().BeSameAs(run, "a terminal failure must surface even when a sibling ignores cancellation");

        var act = async () => await run;
        _ = await act.Should().ThrowAsync<Exception>();
        slow.DisposedWhileConsuming.Should().BeFalse("a sibling must be stopped before cleanup disposes it");
    }

    private sealed class SlowAndThrowingPipeline(SlowDisposableSink slow) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<YieldingSource, int>("source");
            var slowHandle = builder.AddSink<SlowDisposableSink, int>("slow");
            var throwingHandle = builder.AddSink<ThrowingSink, int>("throwing");
            _ = builder.AddPreconfiguredNodeInstance(slowHandle.Id, slow);
            _ = builder.Connect(source, slowHandle);
            _ = builder.Connect(source, throwingHandle);
            _ = builder.WithBranchOptions("source", new BranchOptions(BranchBufferCapacity));
        }
    }

    private sealed class CooperativeCancellationSink : SinkNode<int>, IAsyncDisposable
    {
        private readonly TaskCompletionSource _observedCancellation =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task ObservedCancellation => _observedCancellation.Task;

        public volatile bool DisposedWhileConsuming;

        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                _ = _observedCancellation.TrySetResult();
            }
        }

        public ValueTask DisposeAsync()
        {
            DisposedWhileConsuming = !ObservedCancellation.IsCompleted;
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task FailingTerminal_CooperativeSiblingObservesCancellationBeforeRunReturns()
    {
        var (context, _) = CreateRun();
        var cooperative = new CooperativeCancellationSink();

        var run = PipelineRunner.Create().RunAsync(new CooperativeAndThrowingPipeline(cooperative), context, CancellationToken.None);

        var act = async () => await run;
        _ = await act.Should().ThrowAsync<Exception>();
        _ = cooperative.ObservedCancellation.IsCompleted.Should().BeTrue();
        cooperative.DisposedWhileConsuming.Should().BeFalse("the sibling must stop before cleanup disposes it");
    }

    private sealed class CooperativeAndThrowingPipeline(CooperativeCancellationSink cooperative) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SlowYieldingSource, int>("source");
            var cooperativeHandle = builder.AddSink<CooperativeCancellationSink, int>("cooperative");
            var throwingHandle = builder.AddSink<ThrowingSink, int>("throwing");
            _ = builder.AddPreconfiguredNodeInstance(cooperativeHandle.Id, cooperative);
            _ = builder.Connect(source, cooperativeHandle);
            _ = builder.Connect(source, throwingHandle);
            _ = builder.WithBranchOptions("source", new BranchOptions(BranchBufferCapacity));
        }
    }

    /// <summary>
    ///     A source whose first items flow so the throwing sink fails while the cooperative sibling is waiting.
    /// </summary>
    private sealed class SlowYieldingSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new DataStream<int>(Produce(cancellationToken), "slow-yielding-source");

        private static async IAsyncEnumerable<int> Produce([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            for (var i = 0; i < SourceItemCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return i;
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    ///     Creates a context carrying its own recorder. The recorder used to be a process-wide static, which let a
    ///     sink still draining after one test finished record into the next test's counters.
    /// </summary>
    private static (PipelineContext Context, DrainRecorder Recorder) CreateRun()
    {
        var context = new PipelineContext(PipelineContextConfiguration.Default);
        DrainRecorder recorder = new();
        context.Items[DrainRecorder.ContextKey] = recorder;
        return (context, recorder);
    }

    /// <summary>
    ///     Per-run recorder, reached by the nodes through the pipeline context.
    /// </summary>
    private sealed class DrainRecorder
    {
        public const string ContextKey = "test.drain.recorder";

        private int _aggregatedCount;
        private int _firstSinkCount;
        private int _flakyAttempts;
        private int _produced;
        private int _producedWhenFirstItemObserved = -1;
        private int _secondSinkCount;

        public int FirstSinkCount => Volatile.Read(ref _firstSinkCount);

        public int AggregatedCount => Volatile.Read(ref _aggregatedCount);

        public int FlakyAttempts => Volatile.Read(ref _flakyAttempts);

        public int SecondSinkCount => Volatile.Read(ref _secondSinkCount);

        public int ProducedWhenFirstItemObserved => Volatile.Read(ref _producedWhenFirstItemObserved);

        public static DrainRecorder For(PipelineContext context) => (DrainRecorder)context.Items[ContextKey];

        public void RecordProduced()
        {
            _ = Interlocked.Increment(ref _produced);
        }

        public void RecordFirstSinkItem()
        {
            if (Interlocked.Increment(ref _firstSinkCount) == 1)
                _ = Interlocked.CompareExchange(ref _producedWhenFirstItemObserved, Volatile.Read(ref _produced), -1);
        }

        public void RecordAggregated(int count)
        {
            _ = Interlocked.Add(ref _aggregatedCount, count);
        }

        public int RecordFlakyAttempt() => Interlocked.Increment(ref _flakyAttempts);

        public void RecordSecondSinkItem()
        {
            _ = Interlocked.Increment(ref _secondSinkCount);
        }
    }

    private sealed class YieldingSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new DataStream<int>(Produce(DrainRecorder.For(context), cancellationToken), "yielding-source");

        private static async IAsyncEnumerable<int> Produce(
            DrainRecorder recorder,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            for (var i = 0; i < SourceItemCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                recorder.RecordProduced();

                // Yield so the source can outrun an unthrottled consumer, making a buffering regression visible.
                await Task.Yield();
                yield return i;
            }
        }
    }

    private sealed class FirstSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                DrainRecorder.For(context).RecordFirstSinkItem();
            }
        }
    }

    private sealed class SecondSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                DrainRecorder.For(context).RecordSecondSinkItem();
            }
        }
    }

    private sealed class ThrowingSink : SinkNode<int>
    {
        public override Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("sink failed");
    }

    private sealed class BoundedBranchPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<YieldingSource, int>("source");
            _ = builder.Connect(source, builder.AddSink<FirstSink, int>("first"));
            _ = builder.Connect(source, builder.AddSink<SecondSink, int>("second"));
            _ = builder.WithBranchOptions("source", new BranchOptions(BranchBufferCapacity));
        }
    }

    private sealed class UnboundedBranchPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<YieldingSource, int>("source");
            _ = builder.Connect(source, builder.AddSink<FirstSink, int>("first"));
            _ = builder.Connect(source, builder.AddSink<SecondSink, int>("second"));
        }
    }

    private sealed class LinearPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<YieldingSource, int>("source");
            _ = builder.Connect(source, builder.AddSink<FirstSink, int>("first"));
        }
    }

    private sealed class FailingBranchPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<YieldingSource, int>("source");
            _ = builder.Connect(source, builder.AddSink<FirstSink, int>("first"));
            _ = builder.Connect(source, builder.AddSink<ThrowingSink, int>("throwing"));
            _ = builder.WithBranchOptions("source", new BranchOptions(BranchBufferCapacity));
        }
    }
}
