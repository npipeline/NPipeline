// ReSharper disable ClassNeverInstantiated.Local

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class BranchBenchmarks
{
    private PipelineContext _ctx = null!;
    private PipelineRunner _runner = null!;

    [Params(10_000, 50_000)]
    public int ItemCount { get; set; }

    // 0 = unbounded, else per-subscriber capacity
    [Params(0, 128, 1024)]
    public int PerSubscriberCapacity { get; set; }

    // Slow sink delay in microseconds to simulate backpressure
    [Params(0, 50)]
    public int SlowSinkDelayMicros { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _runner = PipelineRunner.Create();

        _ctx = PipelineContext.Default;
        _ctx.Parameters["count"] = ItemCount;
        _ctx.Parameters["slowDelayMicros"] = SlowSinkDelayMicros;
        _ctx.Parameters["branchCap"] = PerSubscriberCapacity;
    }

    [Benchmark(Description = "Branch: Single Transform -> Two Sinks (fast+slow)")]
    public async Task RunBranch()
    {
        await _runner.RunAsync<BranchingPipeline>(_ctx);
    }

    private sealed class BranchingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<GenSource, int>("src");
            var t = b.AddTransform<PassThroughTransform, int, int>("t");
            var sinkFast = b.AddSink<FastSink, int>("fast");
            var sinkSlow = b.AddSink<SlowSink, int>("slow");

            b.Connect(src, t);
            b.Connect(t, sinkFast);
            b.Connect(t, sinkSlow);

            var cap = c.Parameters.TryGetValue("branchCap", out var v)
                ? Convert.ToInt32(v)
                : 0;

            if (cap > 0)

                // Apply per-node branching options
                b.WithBranchOptions("t", new BranchOptions(cap));
        }
    }

    private sealed class GenSource : SourceNode<int>
    {
        public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<int>(Stream(cancellationToken), "gen");

            async IAsyncEnumerable<int> Stream([EnumeratorCancellation] CancellationToken ct)
            {
                await Task.Yield();

                for (var i = 0; i < count; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return i;
                }
            }
        }
    }

    private sealed class PassThroughTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }

    private sealed class FastSink : SinkNode<int>
    {
        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                /* consume */
            }
        }
    }

    private sealed class SlowSink : SinkNode<int>
    {
        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            var delayUs = context.Parameters.TryGetValue("slowDelayMicros", out var v)
                ? Convert.ToInt32(v)
                : 0;

            var delay = TimeSpan.FromMilliseconds(delayUs / 1000.0);

            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                if (delayUs > 0)
                    await Task.Delay(delay, cancellationToken);
            }
        }
    }
}
