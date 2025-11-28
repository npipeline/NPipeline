// ReSharper disable ClassNeverInstantiated.Local

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Execution.Factories;
using NPipeline.Extensions.Parallelism;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class ParallelismScalingBenchmarks
{
    private PipelineContext _ctx = null!;
    private PipelineRunner _runner = null!;

    [Params(1_000, 10_000, 100_000)]
    public int ItemCount { get; set; }

    [Params(1, 2, 4, 8, 16)]
    public int ParallelismDegree { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());
        _ctx = PipelineContext.Default;
        _ctx.Parameters["count"] = ItemCount;
        _ctx.Parameters["parallelism"] = ParallelismDegree;
    }

    // ------------------------------------------------------------------------ 
    // 1) Sequential vs Parallel processing comparison
    // ------------------------------------------------------------------------

    [Benchmark(Baseline = true, Description = "Sequential processing (parallelism = 1)")]
    public async Task Sequential_Processing()
    {
        _ctx.Parameters["parallelism"] = 1;
        await _runner.RunAsync<SequentialProcessingPipeline>(_ctx);
    }

    [Benchmark(Description = "Parallel processing with specified degree")]
    public async Task Parallel_Processing()
    {
        await _runner.RunAsync<ParallelProcessingPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 2) Different parallelism degrees
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Low parallelism (2 threads)")]
    public async Task Parallelism_Low()
    {
        _ctx.Parameters["parallelism"] = 2;
        await _runner.RunAsync<ParallelProcessingPipeline>(_ctx);
    }

    [Benchmark(Description = "Medium parallelism (4 threads)")]
    public async Task Parallelism_Medium()
    {
        _ctx.Parameters["parallelism"] = 4;
        await _runner.RunAsync<ParallelProcessingPipeline>(_ctx);
    }

    [Benchmark(Description = "High parallelism (8 threads)")]
    public async Task Parallelism_High()
    {
        _ctx.Parameters["parallelism"] = 8;
        await _runner.RunAsync<ParallelProcessingPipeline>(_ctx);
    }

    [Benchmark(Description = "Very high parallelism (16 threads)")]
    public async Task Parallelism_VeryHigh()
    {
        _ctx.Parameters["parallelism"] = 16;
        await _runner.RunAsync<ParallelProcessingPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 3) Parallel strategies comparison
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Blocking parallel strategy")]
    public async Task ParallelStrategy_Blocking()
    {
        await _runner.RunAsync<BlockingParallelPipeline>(_ctx);
    }

    [Benchmark(Description = "Drop newest parallel strategy")]
    public async Task ParallelStrategy_DropNewest()
    {
        await _runner.RunAsync<DropNewestParallelPipeline>(_ctx);
    }

    [Benchmark(Description = "Drop oldest parallel strategy")]
    public async Task ParallelStrategy_DropOldest()
    {
        await _runner.RunAsync<DropOldestParallelPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 4) CPU-bound vs IO-bound workloads
    // ------------------------------------------------------------------------

    [Benchmark(Description = "CPU-bound parallel processing")]
    public async Task Workload_CPUBound()
    {
        await _runner.RunAsync<CPUBoundParallelPipeline>(_ctx);
    }

    [Benchmark(Description = "IO-bound parallel processing")]
    public async Task Workload_IOBound()
    {
        await _runner.RunAsync<IOBoundParallelPipeline>(_ctx);
    }

    [Benchmark(Description = "Mixed workload parallel processing")]
    public async Task Workload_Mixed()
    {
        await _runner.RunAsync<MixedWorkloadParallelPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 5) Scaling with batch sizes
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Small batch parallel processing")]
    public async Task Batching_Small()
    {
        _ctx.Parameters["batchSize"] = 50;
        await _runner.RunAsync<BatchedParallelPipeline>(_ctx);
    }

    [Benchmark(Description = "Medium batch parallel processing")]
    public async Task Batching_Medium()
    {
        _ctx.Parameters["batchSize"] = 200;
        await _runner.RunAsync<BatchedParallelPipeline>(_ctx);
    }

    [Benchmark(Description = "Large batch parallel processing")]
    public async Task Batching_Large()
    {
        _ctx.Parameters["batchSize"] = 1000;
        await _runner.RunAsync<BatchedParallelPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // Pipeline definitions
    // ------------------------------------------------------------------------

    private sealed class SequentialProcessingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, int>("src");
            var t = b.AddTransform<ProcessingTransform, int, ProcessedResult>("t");
            var sink = b.AddSink<BlackHoleSink<ProcessedResult>, ProcessedResult>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class ParallelProcessingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var parallelism = c.Parameters.TryGetValue("parallelism", out var p)
                ? Convert.ToInt32(p)
                : 1;

            var src = b.AddSource<DataGeneratorSource, int>("src");

            var t = b.AddTransform<ProcessingTransform, int, ProcessedResult>("t")
                .WithBlockingParallelism(b, parallelism);

            var sink = b.AddSink<BlackHoleSink<ProcessedResult>, ProcessedResult>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class BlockingParallelPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var parallelism = c.Parameters.TryGetValue("parallelism", out var p)
                ? Convert.ToInt32(p)
                : 1;

            var src = b.AddSource<DataGeneratorSource, int>("src");

            var t = b.AddTransform<ProcessingTransform, int, ProcessedResult>("t")
                .WithBlockingParallelism(b, parallelism);

            var sink = b.AddSink<BlackHoleSink<ProcessedResult>, ProcessedResult>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class DropNewestParallelPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var parallelism = c.Parameters.TryGetValue("parallelism", out var p)
                ? Convert.ToInt32(p)
                : 1;

            var src = b.AddSource<DataGeneratorSource, int>("src");

            var t = b.AddTransform<ProcessingTransform, int, ProcessedResult>("t")
                .WithDropNewestParallelism(b, parallelism);

            var sink = b.AddSink<BlackHoleSink<ProcessedResult>, ProcessedResult>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class DropOldestParallelPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var parallelism = c.Parameters.TryGetValue("parallelism", out var p)
                ? Convert.ToInt32(p)
                : 1;

            var src = b.AddSource<DataGeneratorSource, int>("src");

            var t = b.AddTransform<ProcessingTransform, int, ProcessedResult>("t")
                .WithDropOldestParallelism(b, parallelism);

            var sink = b.AddSink<BlackHoleSink<ProcessedResult>, ProcessedResult>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class CPUBoundParallelPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var parallelism = c.Parameters.TryGetValue("parallelism", out var p)
                ? Convert.ToInt32(p)
                : 1;

            var src = b.AddSource<DataGeneratorSource, int>("src");

            var t = b.AddTransform<CPUBoundTransform, int, ProcessedResult>("t")
                .WithBlockingParallelism(b, parallelism);

            var sink = b.AddSink<BlackHoleSink<ProcessedResult>, ProcessedResult>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class IOBoundParallelPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var parallelism = c.Parameters.TryGetValue("parallelism", out var p)
                ? Convert.ToInt32(p)
                : 1;

            var src = b.AddSource<DataGeneratorSource, int>("src");

            var t = b.AddTransform<IOBoundTransform, int, ProcessedResult>("t")
                .WithDropNewestParallelism(b, parallelism);

            var sink = b.AddSink<BlackHoleSink<ProcessedResult>, ProcessedResult>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class MixedWorkloadParallelPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var parallelism = c.Parameters.TryGetValue("parallelism", out var p)
                ? Convert.ToInt32(p)
                : 1;

            var src = b.AddSource<DataGeneratorSource, int>("src");

            var t = b.AddTransform<MixedWorkloadTransform, int, ProcessedResult>("t")
                .WithBlockingParallelism(b, parallelism);

            var sink = b.AddSink<BlackHoleSink<ProcessedResult>, ProcessedResult>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class BatchedParallelPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var parallelism = c.Parameters.TryGetValue("parallelism", out var p)
                ? Convert.ToInt32(p)
                : 1;

            var batchSize = c.Parameters.TryGetValue("batchSize", out var bs)
                ? Convert.ToInt32(bs)
                : 200;

            var src = b.AddSource<DataGeneratorSource, int>("src");
            var batch = b.AddBatcher<int>("batch", batchSize, TimeSpan.FromSeconds(10));

            var t = b.AddTransform<BatchProcessingTransform, IReadOnlyCollection<int>, ProcessedResult>("t")
                .WithBlockingParallelism(b, parallelism);

            var sink = b.AddSink<BlackHoleSink<ProcessedResult>, ProcessedResult>("sink");

            _ = b.Connect(src, batch);
            _ = b.Connect(batch, t);
            _ = b.Connect(t, sink);
        }
    }

    // ------------------------------------------------------------------------ 
    // Node implementations
    // ------------------------------------------------------------------------

    private sealed class DataGeneratorSource : SourceNode<int>
    {
        public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<int>(
                BenchmarkDataGenerators.GenerateIntegers(count, cancellationToken),
                "dataGenerator");
        }
    }

    private sealed class ProcessingTransform : TransformNode<int, ProcessedResult>
    {
        public override async Task<ProcessedResult> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate moderate processing work
            await Task.Delay(1, cancellationToken);

            return new ProcessedResult
            {
                InputValue = item,
                OutputValue = item * 2,
                ProcessingTime = DateTime.UtcNow,
                ThreadId = Thread.CurrentThread.ManagedThreadId,
            };
        }
    }

    private sealed class CPUBoundTransform : TransformNode<int, ProcessedResult>
    {
        public override Task<ProcessedResult> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate CPU-bound work
            var result = 0;

            for (var i = 0; i < 1000; i++)
            {
                result += (int)Math.Sqrt(item * i);
            }

            return Task.FromResult(new ProcessedResult
            {
                InputValue = item,
                OutputValue = result,
                ProcessingTime = DateTime.UtcNow,
                ThreadId = Thread.CurrentThread.ManagedThreadId,
            });
        }
    }

    private sealed class IOBoundTransform : TransformNode<int, ProcessedResult>
    {
        public override async Task<ProcessedResult> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate IO-bound work
            await Task.Delay(10, cancellationToken);

            return new ProcessedResult
            {
                InputValue = item,
                OutputValue = item * 2,
                ProcessingTime = DateTime.UtcNow,
                ThreadId = Thread.CurrentThread.ManagedThreadId,
            };
        }
    }

    private sealed class MixedWorkloadTransform : TransformNode<int, ProcessedResult>
    {
        private readonly Random _random = new();

        public override async Task<ProcessedResult> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Randomly choose between CPU-bound and IO-bound work
            if (_random.NextDouble() < 0.5)
            {
                // CPU-bound work
                var result = 0;

                for (var i = 0; i < 500; i++)
                {
                    result += (int)Math.Sqrt(item * i);
                }

                return new ProcessedResult
                {
                    InputValue = item,
                    OutputValue = result,
                    ProcessingTime = DateTime.UtcNow,
                    ThreadId = Thread.CurrentThread.ManagedThreadId,
                };
            }

            // IO-bound work
            await Task.Delay(5, cancellationToken);

            return new ProcessedResult
            {
                InputValue = item,
                OutputValue = item * 2,
                ProcessingTime = DateTime.UtcNow,
                ThreadId = Thread.CurrentThread.ManagedThreadId,
            };
        }
    }

    private sealed class BatchProcessingTransform : TransformNode<IReadOnlyCollection<int>, ProcessedResult>
    {
        public override async Task<ProcessedResult> ExecuteAsync(IReadOnlyCollection<int> batch, PipelineContext context, CancellationToken cancellationToken)
        {
            // Process entire batch
            var sum = 0;

            foreach (var item in batch)
            {
                sum += item;
                await Task.Yield(); // Allow for parallelism
            }

            return new ProcessedResult
            {
                InputValue = batch.Count,
                OutputValue = sum,
                ProcessingTime = DateTime.UtcNow,
                ThreadId = Thread.CurrentThread.ManagedThreadId,
            };
        }
    }

    private sealed class BlackHoleSink<T> : SinkNode<T>
    {
        public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Discard items
            }
        }
    }
}

/// <summary>
///     Represents a processed result for parallelism scaling benchmarks.
/// </summary>
public record ProcessedResult
{
    public int InputValue { get; init; }
    public int OutputValue { get; init; }
    public DateTime ProcessingTime { get; init; }
    public int ThreadId { get; init; }
}
