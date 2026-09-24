// ReSharper disable ClassNeverInstantiated.Local

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.Attributes;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

/// <summary>
///     Measures the fixed per-run cost of turning a definition into a graph, which for a service running a short
///     pipeline per request dominates everything else. Each shape is measured both as a plain definition, which
///     rebuilds on every run, and marked <see cref="CacheableGraphAttribute" />, which builds once.
///     <para>
///         The empty-source run benchmarks isolate that fixed cost: with no items to process, what remains is the
///         build, the runtime binding, node instantiation and plan lookup.
///     </para>
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class PipelineBuildBenchmarks
{
    private PipelineFactory _factory = null!;
    private PipelineRunner _runner = null!;

    [GlobalSetup]
    public void Setup()
    {
        _factory = new PipelineFactory();
        _runner = PipelineRunner.Create();

        // Prime both caches so the steady state is what gets measured, not the first build.
        _ = _factory.Create<CacheableSingleNode>(NewContext(0));
        _ = _factory.Create<CacheableFanOut>(NewContext(0));
        _ = _factory.Create<CacheableLinear>(NewContext(0));
    }

    // ---------------------------------------------------------------------
    // Build only
    // ---------------------------------------------------------------------

    [Benchmark(Baseline = true, Description = "Build single-node graph")]
    public Pipeline.Pipeline Build_SingleNode() => _factory.Create<SingleNode>(NewContext(0));

    [Benchmark(Description = "Build single-node graph, cached")]
    public Pipeline.Pipeline Build_SingleNode_Cached() => _factory.Create<CacheableSingleNode>(NewContext(0));

    [Benchmark(Description = "Build fan-out graph")]
    public Pipeline.Pipeline Build_FanOut() => _factory.Create<FanOut>(NewContext(0));

    [Benchmark(Description = "Build fan-out graph, cached")]
    public Pipeline.Pipeline Build_FanOut_Cached() => _factory.Create<CacheableFanOut>(NewContext(0));

    [Benchmark(Description = "Build 12-node linear graph")]
    public Pipeline.Pipeline Build_Linear12() => _factory.Create<Linear>(NewContext(0));

    [Benchmark(Description = "Build 12-node linear graph, cached")]
    public Pipeline.Pipeline Build_Linear12_Cached() => _factory.Create<CacheableLinear>(NewContext(0));

    // ---------------------------------------------------------------------
    // Whole run over an empty source: the fixed cost with no item work in it
    // ---------------------------------------------------------------------

    [Benchmark(Description = "Run empty fan-out pipeline")]
    public async Task Run_FanOut_NoItems()
    {
        await _runner.RunAsync<FanOut>(NewContext(0));
    }

    [Benchmark(Description = "Run empty fan-out pipeline, cached graph")]
    public async Task Run_FanOut_NoItems_Cached()
    {
        await _runner.RunAsync<CacheableFanOut>(NewContext(0));
    }

    [Benchmark(Description = "Run fan-out pipeline over 100 items")]
    public async Task Run_FanOut_100Items()
    {
        await _runner.RunAsync<FanOut>(NewContext(100));
    }

    [Benchmark(Description = "Run fan-out pipeline over 100 items, cached graph")]
    public async Task Run_FanOut_100Items_Cached()
    {
        await _runner.RunAsync<CacheableFanOut>(NewContext(100));
    }

    private static PipelineContext NewContext(int count) => new(new PipelineContextConfiguration(new Dictionary<string, object> { ["count"] = count }));

    private static void DefineSingleNode(PipelineBuilder builder)
    {
        var source = builder.AddSource<CountingSource, int>("source");
        var sink = builder.AddSink<BlackHoleSink, int>("sink");
        builder.Connect(source, sink);
    }

    private static void DefineFanOut(PipelineBuilder builder)
    {
        var source = builder.AddSource<CountingSource, int>("source");
        var left = builder.AddTransform<PassThrough, int, int>("left");
        var right = builder.AddTransform<PassThrough, int, int>("right");
        var leftSink = builder.AddSink<BlackHoleSink, int>("leftSink");
        var rightSink = builder.AddSink<BlackHoleSink, int>("rightSink");

        builder.Connect(source, left).Connect(left, leftSink);
        builder.Connect(source, right).Connect(right, rightSink);
    }

    private static void DefineLinear(PipelineBuilder builder)
    {
        var source = builder.AddSource<CountingSource, int>("source");
        var previous = builder.AddTransform<PassThrough, int, int>("t0");
        builder.Connect(source, previous);

        for (var i = 1; i < 10; i++)
        {
            var next = builder.AddTransform<PassThrough, int, int>($"t{i}");
            builder.Connect(previous, next);
            previous = next;
        }

        var sink = builder.AddSink<BlackHoleSink, int>("sink");
        builder.Connect(previous, sink);
    }

    private sealed class SingleNode : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context) => DefineSingleNode(builder);
    }

    [CacheableGraph]
    private sealed class CacheableSingleNode : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context) => DefineSingleNode(builder);
    }

    private sealed class FanOut : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context) => DefineFanOut(builder);
    }

    [CacheableGraph]
    private sealed class CacheableFanOut : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context) => DefineFanOut(builder);
    }

    private sealed class Linear : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context) => DefineLinear(builder);
    }

    [CacheableGraph]
    private sealed class CacheableLinear : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context) => DefineLinear(builder);
    }

    private sealed class CountingSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var value)
                ? Convert.ToInt32(value)
                : 0;

            return new DataStream<int>(Generate(count, cancellationToken), "source");
        }

        private static async IAsyncEnumerable<int> Generate(int count, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();

            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();
                yield return i;
            }
        }
    }

    private sealed class PassThrough : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken) => new(input);
    }

    private sealed class BlackHoleSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Discard.
            }
        }
    }
}
