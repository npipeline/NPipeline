// ReSharper disable ClassNeverInstantiated.Local

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.Attributes.Nodes;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow.Windowing;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Factories;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class PipelineExecutionBenchmarks : IDisposable
{
    private CancellationTokenSource _cancellationTokenSource = null!;
    private PipelineContext _ctx = null!;
    private PipelineRunner _runner = null!;

    [Params(1_000, 10_000, 100_000)]
    public int ItemCount { get; set; }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _cancellationTokenSource.Dispose();
    }

    [GlobalSetup]
    public void Setup()
    {
        _runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());
        _cancellationTokenSource = new CancellationTokenSource();

        _ctx = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = ItemCount,
        }, CancellationToken: _cancellationTokenSource.Token));
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _cancellationTokenSource.Dispose();
    }

    // ------------------------------------------------------------------------
    // Required benchmarks from planning document (section 2.3)
    // ------------------------------------------------------------------------

    [Benchmark(Baseline = true, Description = "Linear pipeline processing 1000 items")]
    public async Task LinearPipeline_1000Items()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = 1000,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<LinearPipelineDefinition1000Items>(context);
    }

    [Benchmark(Description = "Branching pipeline with 500 items per branch")]
    public async Task BranchingPipeline_500ItemsPerBranch()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = 500,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<BranchingPipelineDefinition>(context);
    }

    [Benchmark(Description = "Join pipeline with 300 items from each source")]
    public async Task JoinPipeline_300ItemsEachSource()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = 300,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<JoinPipelineDefinition>(context);
    }

    // ------------------------------------------------------------------------
    // Additional useful benchmarks for different scenarios
    // ------------------------------------------------------------------------

    // Node type benchmarks
    [Benchmark(Description = "Source node performance")]
    public async Task SourceNode_Performance()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = ItemCount,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<SourceOnlyPipeline>(context);
    }

    [Benchmark(Description = "Transform node performance")]
    public async Task TransformNode_Performance()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = ItemCount,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<TransformPipeline>(context);
    }

    [Benchmark(Description = "Sink node performance")]
    public async Task SinkNode_Performance()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = ItemCount,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<SinkPipeline>(context);
    }

    [Benchmark(Description = "Aggregate node performance")]
    public async Task AggregateNode_Performance()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = ItemCount,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<AggregatePipeline>(context);
    }

    // Error handling benchmarks
    [Benchmark(Description = "Pipeline with error handling (no errors)")]
    public async Task ErrorHandling_NoErrors()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = ItemCount,
            ["errorRate"] = 0,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<ErrorHandlingPipeline>(context);
    }

    [Benchmark(Description = "Pipeline with error handling (10% error rate)")]
    public async Task ErrorHandling_WithErrors()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = ItemCount,
            ["errorRate"] = 0.1,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<ErrorHandlingPipeline>(context);
    }

    // Validation benchmarks
    [Benchmark(Description = "Pipeline with validation overhead")]
    public async Task Validation_Overhead()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = ItemCount,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<ValidationPipeline>(context);
    }

    // Cancellation benchmarks
    [Benchmark(Description = "Pipeline cancellation after 50% completion")]
    public async Task Cancellation_MidStream()
    {
        var cts = new CancellationTokenSource();

        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = ItemCount,
            ["cancelAt"] = ItemCount / 2,
        }, CancellationToken: cts.Token));

        // Start the pipeline and cancel after a short delay
        var task = _runner.RunAsync<CancellablePipeline>(context);
        await Task.Delay(1, cts.Token);
        cts.Cancel();

        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        finally
        {
            cts.Dispose();
        }
    }

    // Data volume benchmarks
    [Benchmark(Description = "Small data volume (100 items)")]
    public async Task DataVolume_Small()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = 100,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<LinearPipelineDefinition1000Items>(context);
    }

    [Benchmark(Description = "Medium data volume (10,000 items)")]
    public async Task DataVolume_Medium()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = 10_000,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<LinearPipelineDefinition1000Items>(context);
    }

    [Benchmark(Description = "Large data volume (100,000 items)")]
    public async Task DataVolume_Large()
    {
        var context = new PipelineContext(new PipelineContextConfiguration(new Dictionary<string, object>
        {
            ["count"] = 100_000,
        }, CancellationToken: _cancellationTokenSource.Token));

        await _runner.RunAsync<LinearPipelineDefinition1000Items>(context);
    }

    // ------------------------------------------------------------------------
    // Pipeline definitions
    // ------------------------------------------------------------------------

    private sealed class LinearPipelineDefinition1000Items : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<IntGeneratorSource, int>("src");
            var t1 = b.AddTransform<PassThroughTransform, int, int>("t1");
            var t2 = b.AddTransform<PassThroughTransform, int, int>("t2");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            b.Connect(src, t1).Connect(t1, t2).Connect(t2, sink);
        }
    }

    private sealed class BranchingPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<IntGeneratorSource, int>("src");
            var branch1 = b.AddTransform<PassThroughTransform, int, int>("branch1");
            var branch2 = b.AddTransform<PassThroughTransform, int, int>("branch2");
            var sink1 = b.AddSink<BlackHoleSink, int>("sink1");
            var sink2 = b.AddSink<BlackHoleSink, int>("sink2");

            b.Connect(src, branch1).Connect(branch1, sink1);
            b.Connect(src, branch2).Connect(branch2, sink2);
        }
    }

    private sealed class JoinPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src1 = b.AddSource<GeneratorSource, IntItem>("src1");
            var src2 = b.AddSource<GeneratorSource2, LongItem>("src2");
            var join = b.AddJoin<SimpleJoinNode, IntItem, LongItem, int>("join");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            // Connect sources to join (different types avoid ambiguity)
            b.Connect(src1, join);
            b.Connect(src2, join);
            b.Connect(join, sink);
        }
    }

    private sealed class SourceOnlyPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<IntGeneratorSource, int>("src");
            var sink = b.AddSink<BlackHoleSink, int>("sink");
            b.Connect(src, sink);
        }
    }

    private sealed class TransformPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<IntGeneratorSource, int>("src");
            var t = b.AddTransform<ComplexTransform, int, string>("t");
            var sink = b.AddSink<StringBlackHoleSink, string>("sink");

            b.Connect(src, t).Connect(t, sink);
        }
    }

    private sealed class SinkPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<IntGeneratorSource, int>("src");
            var sink = b.AddSink<ComplexSink, int>("sink");
            b.Connect(src, sink);
        }
    }

    private sealed class AggregatePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<IntGeneratorSource, int>("src");
            var agg = b.AddAggregate<SimpleAggregateNode, int, int, int, int>("agg");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            b.Connect(src, agg).Connect(agg, sink);
        }
    }

    private sealed class ErrorHandlingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<IntGeneratorSource, int>("src");
            var t = b.AddTransform<ErrorProneTransform, int, int>("t");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            // Add error handling
            b.WithErrorHandler(t, typeof(TestErrorHandler));

            b.Connect(src, t).Connect(t, sink);
        }
    }

    private sealed class ValidationPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<IntGeneratorSource, int>("src");
            var v = b.AddTransform<ValidationTransform, int, int>("v");
            var t = b.AddTransform<PassThroughTransform, int, int>("t");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            b.Connect(src, v).Connect(v, t).Connect(t, sink);
        }
    }

    private sealed class CancellablePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<CancellableIntSource, int>("src");
            var t = b.AddTransform<SlowTransform, int, int>("t");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            b.Connect(src, t).Connect(t, sink);
        }
    }

    // ------------------------------------------------------------------------
    // Node implementations
    // ------------------------------------------------------------------------

    private sealed class GeneratorSource : SourceNode<IntItem>
    {
        public override IDataPipe<IntItem> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<IntItem>(GenerateItems(count, cancellationToken), "generator");
        }

        private static async IAsyncEnumerable<IntItem> GenerateItems(int count, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();

            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();
                yield return new IntItem { Value = i };
            }
        }
    }

    private sealed class GeneratorSource2 : SourceNode<LongItem>
    {
        public override IDataPipe<LongItem> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<LongItem>(GenerateItems(count, cancellationToken), "generator2");
        }

        private static async IAsyncEnumerable<LongItem> GenerateItems(int count, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();

            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();
                yield return new LongItem { Value = (long)i * 10 }; // Different values to distinguish from source1
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

    private sealed class ComplexTransform : TransformNode<int, string>
    {
        public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate some processing
            var result = $"Processed_{item:D6}";
            return Task.FromResult(result);
        }
    }

    private sealed class ErrorProneTransform : TransformNode<int, int>
    {
        private readonly Random _random = new();

        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            var errorRate = context.Parameters.TryGetValue("errorRate", out var v)
                ? Convert.ToDouble(v)
                : 0.0;

            if (_random.NextDouble() < errorRate)
                throw new InvalidOperationException($"Simulated error processing item {item}");

            return Task.FromResult(item);
        }
    }

    private sealed class ValidationTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate validation overhead
            if (item < 0)
                throw new ArgumentException("Item must be non-negative");

            // Simulate complex validation
            var isValid = item % 2 == 0 || item % 3 == 0 || item % 5 == 0;

            return Task.FromResult(isValid
                ? item
                : -1);
        }
    }

    private sealed class BlackHoleSink : SinkNode<int>
    {
        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Discard items
            }
        }
    }

    private sealed class StringBlackHoleSink : SinkNode<string>
    {
        public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Discard items
            }
        }
    }

    private sealed class ComplexSink : SinkNode<int>
    {
        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            var count = 0;

            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Simulate some processing
                count++;

                if (count % 1000 == 0)

                    // Simulate periodic work
                    await Task.Yield();
            }
        }
    }

    private sealed class IntGeneratorSource : SourceNode<int>
    {
        public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<int>(GenerateIntItems(count, cancellationToken), "intGenerator");
        }

        private static async IAsyncEnumerable<int> GenerateIntItems(int count, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();

            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();
                yield return i;
            }
        }
    }

    private sealed class CancellableIntSource : SourceNode<int>
    {
        public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            var cancelAt = context.Parameters.TryGetValue("cancelAt", out var c)
                ? Convert.ToInt32(c)
                : count;

            return new StreamingDataPipe<int>(GenerateIntItems(count, cancelAt, cancellationToken), "cancellableInt");
        }

        private static async IAsyncEnumerable<int> GenerateIntItems(int count, int cancelAt, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();

            for (var i = 0; i < count; i++)
            {
                if (i >= cancelAt)
                    ct.ThrowIfCancellationRequested();

                yield return i;
            }
        }
    }

    private sealed class SlowTransform : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Add small delay to make cancellation more noticeable
            await Task.Delay(1, cancellationToken);
            return item;
        }
    }

    // Wrapper classes for join with properties
    private sealed class IntItem
    {
        public int Value { get; set; }
        public int Key => Value;
    }

    private sealed class LongItem
    {
        public long Value { get; set; }
        public int Key => (int)(Value / 10);
    }

    [KeySelector(typeof(IntItem), nameof(IntItem.Key))]
    [KeySelector(typeof(LongItem), nameof(LongItem.Key))]
    private sealed class SimpleJoinNode : KeyedJoinNode<int, IntItem, LongItem, int>
    {
        public override int CreateOutput(IntItem item1, LongItem item2)
        {
            // Simple join: just pass through the left value
            return item1.Value;
        }
    }

    private sealed class SimpleAggregateNode : AggregateNode<int, int, int>
    {
        public SimpleAggregateNode() : base(WindowAssigner.Tumbling(TimeSpan.FromMinutes(1)))
        {
        }

        public override int GetKey(int item)
        {
            return item;

            // Use item as its own key
        }

        public override int CreateAccumulator()
        {
            return 0;
        }

        public override int Accumulate(int accumulator, int item)
        {
            return accumulator + item;
        }
    }

    private sealed class TestErrorHandler : INodeErrorHandler<ITransformNode<int, int>, int>
    {
        public Task<NodeErrorDecision> HandleAsync(
            ITransformNode<int, int> node,
            int failedItem,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Skip); // Skip the error item and continue
        }
    }
}
