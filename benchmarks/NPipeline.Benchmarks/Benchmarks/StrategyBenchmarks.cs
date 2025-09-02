using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Execution.Factories;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class StrategyBenchmarks
{
    private PipelineContext _ctx = null!;

    private PipelineRunner _runner = null!;

    [Params(10_000, 50_000)]
    public int ItemCount { get; set; }

    [Params(64, 256)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        _ctx = PipelineContext.Default;
        _ctx.Parameters["count"] = ItemCount;
        _ctx.Parameters["batchSize"] = BatchSize;
    }

    // ------------------------------------------------------------------------
    // 1) Sequential vs Resilient strategy on a simple transform
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Strategy: Sequential (source -> transform -> sink)")]
    public async Task Strategy_Sequential()
    {
        await _runner.RunAsync<SequentialPipeline>(_ctx);
    }

    [Benchmark(Description = "Strategy: Resilient wrapper (source -> transform -> sink)")]
    public async Task Strategy_Resilient()
    {
        await _runner.RunAsync<ResilientPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------
    // 2) Batching vs Unbatching
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Batching: Batch -> Unbatch (batch size param)")]
    public async Task Batching_BatchThenUnbatch()
    {
        await _runner.RunAsync<BatchUnbatchPipeline>(_ctx);
    }

    private sealed class SequentialPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<GenSource, int>("src");
            var t = b.AddTransform<PassThrough, int, int>("t");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            // Default (sequential) strategy
            b.Connect(src, t).Connect(t, sink);
        }
    }

    private sealed class ResilientPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<GenSource, int>("src");
            var t = b.AddTransform<PassThrough, int, int>("t");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            // Wrap transform with resilient execution strategy
            b.WithResilience(t);

            b.Connect(src, t).Connect(t, sink);
        }
    }

    private sealed class BatchUnbatchPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<GenSource, int>("src");

            var batchSize = c.Parameters.TryGetValue("batchSize", out var v)
                ? Convert.ToInt32(v)
                : 64;

            // Batch then unbatch, to measure overheads
            var batch = b.AddBatcher<int>("batch", batchSize, TimeSpan.FromSeconds(10));

            // Adapter: IReadOnlyCollection<int> -> IEnumerable<int> for the unbatcher input
            var cast = b.AddTransform<IReadOnlyCollection<int>, IEnumerable<int>>("cast", coll => coll);

            var unbatch = b.AddUnbatcher<int>("unbatch");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            b.Connect(src, batch)
                .Connect(batch, cast)
                .Connect(cast, unbatch)
                .Connect(unbatch, sink);
        }
    }

    // ------------------------------------------------------------------------
    // Common nodes
    // ------------------------------------------------------------------------

    private sealed class GenSource : SourceNode<int>
    {
        public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<int>(Stream(cancellationToken), "gen");

            async IAsyncEnumerable<int> Stream([EnumeratorCancellation] CancellationToken ct)
            {
                // Small yield to simulate async and avoid tight CPU-bound loops
                await Task.Yield();

                for (var i = 0; i < count; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return i;
                }
            }
        }
    }

    private sealed class PassThrough : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }

    private sealed class BlackHoleSink : SinkNode<int>
    {
        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // discard
            }
        }
    }
}
