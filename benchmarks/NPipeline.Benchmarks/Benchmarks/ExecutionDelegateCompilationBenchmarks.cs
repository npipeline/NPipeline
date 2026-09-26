using BenchmarkDotNet.Attributes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

/// <summary>
///     Measures the per-run cost of building execution plans for a graph that uses preconfigured instances, which
///     bypasses the execution-plan cache. The plan compilation depends only on node kinds and input/output types, so
///     a warmed process reuses the compiled delegates instead of calling <c>Expression.Compile()</c> per node per run.
/// </summary>
[MemoryDiagnoser]
[Orderer(BenchmarkDotNet.Order.SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class ExecutionDelegateCompilationBenchmarks
{
    private PipelineRunner _runner = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        _runner = PipelineRunner.Create();

        // Prime the compiled delegate caches so the steady state is measured.
        for (var i = 0; i < 16; i++)
        {
            await using var context = new PipelineContext();
            await _runner.RunAsync<PreconfiguredDefinition>(context);
        }
    }

    [Benchmark(Description = "1,000 runs of source(lambda) -> transform -> sink(instance)")]
    public async Task Run_PreconfiguredGraph_1000Times()
    {
        for (var i = 0; i < 1000; i++)
        {
            await using var context = new PipelineContext();
            await _runner.RunAsync<PreconfiguredDefinition>(context);
        }
    }

    private sealed class PreconfiguredDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource(() => new[] { 1, 2, 3 }, "source");
            var transform = builder.AddTransform((int value) => value + 1, "transform");
            var sink = builder.AddSink(new CountingSink(), "sink");

            builder.Connect(source, transform).Connect(transform, sink);
        }
    }

    private sealed class CountingSink : SinkNode<int>
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
