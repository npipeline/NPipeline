using BenchmarkDotNet.Attributes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

/// <summary>
///     Measures per-item allocations for a synchronous transform. The per-item executor returns a
///     <see cref="System.Threading.Tasks.ValueTask{T}" />, so a transform that completes synchronously does not
///     allocate a <see cref="System.Threading.Tasks.Task" /> per item.
/// </summary>
[MemoryDiagnoser]
[Orderer(BenchmarkDotNet.Order.SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class PerItemExecutionAllocationBenchmarks
{
    private PipelineContext _context = null!;
    private PipelineRunner _runner = null!;

    [Params(10_000, 100_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _runner = PipelineRunner.Create();
        _context = PipelineContext.CreateDefault();
        _context.Parameters["count"] = ItemCount;
    }

    [Benchmark(Description = "Synchronous transform, sequential strategy")]
    public async Task SynchronousTransform()
    {
        await _runner.RunAsync<SyncPipeline>(_context);
    }

    private sealed class SyncPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<CountingSource, int>("source");
            var transform = builder.AddTransform<SyncTransform, int, int>("transform");
            var sink = builder.AddSink<BlackHoleSink, int>("sink");

            builder.Connect(source, transform).Connect(transform, sink);
        }
    }

    private sealed class CountingSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var value)
                ? Convert.ToInt32(value)
                : 0;

            return new DataStream<int>(Generate(count), "source");
        }

        private static async IAsyncEnumerable<int> Generate(int count)
        {
            for (var i = 0; i < count; i++)
            {
                yield return i;

                if ((i & 1023) == 0)
                    await Task.Yield();
            }
        }
    }

    private sealed class SyncTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken) =>
            new(input + 1);
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
