// ReSharper disable ClassNeverInstantiated.Local

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class MicroOptimizationBenchmarks
{
    private PipelineContext _ctx = null!;
    private PipelineRunner _runner = null!;

    [Params(1_000, 10_000, 100_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _runner = PipelineRunner.Create();

        _ctx = PipelineContext.Default;
        _ctx.Parameters["count"] = ItemCount;
    }

    // ------------------------------------------------------------------------
    // 1) Sequential vs Resilient execution strategies
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Strategy: Sequential execution")]
    public async Task Strategy_Sequential()
    {
        await _runner.RunAsync<SequentialPipeline>(_ctx);
    }

    [Benchmark(Description = "Strategy: Resilient execution")]
    public async Task Strategy_Resilient()
    {
        await _runner.RunAsync<ResilientPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------
    // 2) Direct Task.FromResult vs ValueTask.AsTask performance
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Direct: Task.FromResult")]
    public Task Direct_Task_FromResult()
    {
        // Direct comparison of the micro-optimization
        return Task.FromResult(ItemCount);
    }

    [Benchmark(Description = "Direct: ValueTask.AsTask")]
    public Task Direct_ValueTask_AsTask()
    {
        // Direct comparison of the previous implementation
        return new ValueTask<int>(ItemCount).AsTask();
    }

    // ------------------------------------------------------------------------
    // 3) Memory allocation patterns
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Memory: Sequential execution")]
    public async Task Memory_Sequential()
    {
        await _runner.RunAsync<SequentialPipeline>(_ctx);
    }

    [Benchmark(Description = "Memory: Resilient execution")]
    public async Task Memory_Resilient()
    {
        await _runner.RunAsync<ResilientPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------
    // 4) Simulated execution strategy comparison
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Simulated: Task.FromResult in execution context")]
    public async Task Simulated_Task_FromResult_Execution()
    {
        // Simulate the execution context with Task.FromResult
        await SimulateExecutionContext(Task.FromResult<IDataPipe<int>>(
            new StreamingDataPipe<int>(SimulatedStream(), "simulated")));
    }

    [Benchmark(Description = "Simulated: ValueTask.AsTask in execution context")]
    public async Task Simulated_ValueTask_AsTask_Execution()
    {
        // Simulate the execution context with ValueTask.AsTask
        await SimulateExecutionContext(new ValueTask<IDataPipe<int>>(
            new StreamingDataPipe<int>(SimulatedStream(), "simulated")).AsTask());
    }

    // ------------------------------------------------------------------------
    // Helper methods
    // ------------------------------------------------------------------------

    private static async Task SimulateExecutionContext(Task<IDataPipe<int>> pipeTask)
    {
        var pipe = await pipeTask;

        await foreach (var _ in pipe.WithCancellation(CancellationToken.None))
        {
            // Process item
        }
    }

    private static async IAsyncEnumerable<int> SimulatedStream([EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.Yield();

        for (var i = 0; i < 1000; i++)
        {
            ct.ThrowIfCancellationRequested();
            yield return i;
        }
    }

    // ------------------------------------------------------------------------
    // Pipeline definitions
    // ------------------------------------------------------------------------

    private sealed class SequentialPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<GenSource, int>("src");
            var t = b.AddTransform<PassThroughTransformNode<int, int>, int, int>("t");
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
            var t = b.AddTransform<PassThroughTransformNode<int, int>, int, int>("t");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            // Wrap transform with resilient execution strategy
            b.WithResilience(t);

            b.Connect(src, t).Connect(t, sink);
        }
    }

    // ------------------------------------------------------------------------
    // Common nodes
    // ------------------------------------------------------------------------

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
