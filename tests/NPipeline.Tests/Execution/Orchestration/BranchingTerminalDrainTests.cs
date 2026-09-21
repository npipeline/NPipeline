using AwesomeAssertions;
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
        using var recorder = new DrainRecorder();

        var run = PipelineRunner.Create().RunAsync<BoundedBranchPipeline>(CancellationToken.None);
        var finished = await Task.WhenAny(run, Task.Delay(DeadlockTimeout));

        _ = finished.Should().BeSameAs(run, "a bounded branch buffer must not stall the multicast pump");
        await run;
    }

    [Fact]
    public async Task BoundedBranchBuffer_DeliversEveryItemToEverySink()
    {
        using var recorder = new DrainRecorder();

        await PipelineRunner.Create().RunAsync<BoundedBranchPipeline>(CancellationToken.None);

        _ = recorder.FirstSinkCount.Should().Be(SourceItemCount);
        _ = recorder.SecondSinkCount.Should().Be(SourceItemCount);
    }

    [Fact]
    public async Task BoundedBranchBuffer_AppliesBackpressureToTheSource()
    {
        using var recorder = new DrainRecorder();

        await PipelineRunner.Create().RunAsync<BoundedBranchPipeline>(CancellationToken.None);

        // With real backpressure the source may only run a buffer's worth ahead of the slowest consumer.
        // The bound is deliberately generous: it asserts "bounded", not an exact scheduling outcome.
        _ = recorder.ProducedWhenFirstItemObserved.Should()
            .BeInRange(1, BranchBufferCapacity * 20, "a bounded buffer must stop the source running away");
    }

    [Fact]
    public async Task UnboundedBranchBuffer_DoesNotPrebufferTheWholeStream()
    {
        using var recorder = new DrainRecorder();

        await PipelineRunner.Create().RunAsync<UnboundedBranchPipeline>(CancellationToken.None);

        _ = recorder.FirstSinkCount.Should().Be(SourceItemCount);
        _ = recorder.SecondSinkCount.Should().Be(SourceItemCount);

        _ = recorder.ProducedWhenFirstItemObserved.Should()
            .BeLessThan(SourceItemCount / 4, "consumers must start draining before the source has run to completion");
    }

    [Fact]
    public async Task LinearPipeline_StillRunsToCompletion()
    {
        using var recorder = new DrainRecorder();

        await PipelineRunner.Create().RunAsync<LinearPipeline>(CancellationToken.None);

        _ = recorder.FirstSinkCount.Should().Be(SourceItemCount, "a graph without a fan-out keeps the sequential path");
    }

    [Fact]
    public async Task FailingSinkBelowAFanOut_StillFailsTheRun()
    {
        using var recorder = new DrainRecorder();

        var act = async () => await PipelineRunner.Create().RunAsync<FailingBranchPipeline>(CancellationToken.None);

        _ = await act.Should().ThrowAsync<Exception>("a terminal failure must surface even when terminals run together");
    }

    /// <summary>
    ///     Ambient recorder shared by the nodes, which the pipeline framework instantiates itself.
    /// </summary>
    private sealed class DrainRecorder : IDisposable
    {
        private int _firstSinkCount;
        private int _produced;
        private int _producedWhenFirstItemObserved = -1;
        private int _secondSinkCount;

        public DrainRecorder()
        {
            Current = this;
        }

        public static DrainRecorder? Current { get; private set; }

        public int FirstSinkCount => Volatile.Read(ref _firstSinkCount);

        public int SecondSinkCount => Volatile.Read(ref _secondSinkCount);

        public int ProducedWhenFirstItemObserved => Volatile.Read(ref _producedWhenFirstItemObserved);

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

        public void Dispose()
        {
            Current = null;
        }
    }

    private sealed class YieldingSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new DataStream<int>(Produce(cancellationToken), "yielding-source");
        }

        private static async IAsyncEnumerable<int> Produce(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            for (var i = 0; i < SourceItemCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                DrainRecorder.Current!.RecordProduced();

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
                DrainRecorder.Current!.RecordFirstSinkItem();
            }
        }
    }

    private sealed class SecondSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                DrainRecorder.Current!.RecordSecondSinkItem();
            }
        }
    }

    private sealed class ThrowingSink : SinkNode<int>
    {
        public override Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("sink failed");
        }
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
