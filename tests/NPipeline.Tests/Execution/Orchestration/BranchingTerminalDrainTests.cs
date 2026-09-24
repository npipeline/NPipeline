using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

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

        private int _firstSinkCount;
        private int _produced;
        private int _producedWhenFirstItemObserved = -1;
        private int _secondSinkCount;

        public int FirstSinkCount => Volatile.Read(ref _firstSinkCount);

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
