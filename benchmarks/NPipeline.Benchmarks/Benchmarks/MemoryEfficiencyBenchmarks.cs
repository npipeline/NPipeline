// ReSharper disable ClassNeverInstantiated.Local

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.Benchmarks.Common;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow.Windowing;
using NPipeline.Execution;
using NPipeline.Execution.Factories;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class MemoryEfficiencyBenchmarks
{
    private PipelineContext _ctx = null!;
    private PipelineRunner _runner = null!;

    [Params(1_000, 10_000, 100_000)]
    public int ItemCount { get; set; }

    [Params(64, 256, 1024)]
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
    // 1) Streaming vs Materialization patterns
    // ------------------------------------------------------------------------

    [Benchmark(Baseline = true, Description = "Streaming pipeline (no materialization)")]
    public async Task Streaming_NoMaterialization()
    {
        await _runner.RunAsync<StreamingPipeline>(_ctx);
    }

    [Benchmark(Description = "Materialization pipeline (full buffering)")]
    public async Task Materialization_FullBuffering()
    {
        await _runner.RunAsync<MaterializationPipeline>(_ctx);
    }

    [Benchmark(Description = "Hybrid streaming with selective materialization")]
    public async Task Hybrid_SelectiveMaterialization()
    {
        await _runner.RunAsync<HybridPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 2) Memory allocation patterns comparison
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Low allocation streaming transform")]
    public async Task LowAllocation_Streaming()
    {
        await _runner.RunAsync<LowAllocationStreamingPipeline>(_ctx);
    }

    [Benchmark(Description = "High allocation materializing transform")]
    public async Task HighAllocation_Materializing()
    {
        await _runner.RunAsync<HighAllocationMaterializingPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 3) Batching impact on memory usage
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Small batch size (64)")]
    public async Task Batching_SmallBatch()
    {
        _ctx.Parameters["batchSize"] = 64;
        await _runner.RunAsync<BatchingPipeline>(_ctx);
    }

    [Benchmark(Description = "Medium batch size (256)")]
    public async Task Batching_MediumBatch()
    {
        _ctx.Parameters["batchSize"] = 256;
        await _runner.RunAsync<BatchingPipeline>(_ctx);
    }

    [Benchmark(Description = "Large batch size (1024)")]
    public async Task Batching_LargeBatch()
    {
        _ctx.Parameters["batchSize"] = 1024;
        await _runner.RunAsync<BatchingPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 4) Windowing strategies memory impact
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Tumbling window (fixed size)")]
    public async Task Windowing_TumblingWindow()
    {
        await _runner.RunAsync<TumblingWindowPipeline>(_ctx);
    }

    [Benchmark(Description = "Sliding window (fixed size)")]
    public async Task Windowing_SlidingWindow()
    {
        await _runner.RunAsync<SlidingWindowPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // Pipeline definitions
    // ------------------------------------------------------------------------

    private sealed class StreamingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, ComplexDataItem>("src");
            var t = b.AddTransform<StreamingTransform, ComplexDataItem, ProcessedItem>("t");
            var sink = b.AddSink<BlackHoleSink<ProcessedItem>, ProcessedItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class MaterializationPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, ComplexDataItem>("src");
            var t = b.AddTransform<MaterializingTransform, ComplexDataItem, ProcessedItem>("t");
            var sink = b.AddSink<BlackHoleSink<ProcessedItem>, ProcessedItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class HybridPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, ComplexDataItem>("src");
            var t = b.AddTransform<HybridTransform, ComplexDataItem, ProcessedItem>("t");
            var sink = b.AddSink<BlackHoleSink<ProcessedItem>, ProcessedItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class LowAllocationStreamingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, ComplexDataItem>("src");
            var t = b.AddTransform<LowAllocationTransform, ComplexDataItem, ProcessedItem>("t");
            var sink = b.AddSink<BlackHoleSink<ProcessedItem>, ProcessedItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class HighAllocationMaterializingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, ComplexDataItem>("src");
            var t = b.AddTransform<HighAllocationTransform, ComplexDataItem, ProcessedItem>("t");
            var sink = b.AddSink<BlackHoleSink<ProcessedItem>, ProcessedItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class BatchingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var batchSize = c.Parameters.TryGetValue("batchSize", out var v)
                ? Convert.ToInt32(v)
                : 64;

            var src = b.AddSource<DataGeneratorSource, ComplexDataItem>("src");
            var batch = b.AddBatcher<ComplexDataItem>("batch", batchSize, TimeSpan.FromSeconds(10));
            var t = b.AddTransform<BatchProcessingTransform, IReadOnlyCollection<ComplexDataItem>, ProcessedItem>("t");
            var sink = b.AddSink<BlackHoleSink<ProcessedItem>, ProcessedItem>("sink");

            _ = b.Connect(src, batch);
            _ = b.Connect(batch, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class TumblingWindowPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, ComplexDataItem>("src");
            var agg = b.AddAggregate<TumblingWindowAggregate, ComplexDataItem, ProcessedItem, ProcessedItem, ProcessedItem>("agg");
            var sink = b.AddSink<BlackHoleSink<ProcessedItem>, ProcessedItem>("sink");

            _ = b.Connect(src, agg);
            _ = b.Connect(agg, sink);
        }
    }

    private sealed class SlidingWindowPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, ComplexDataItem>("src");
            var agg = b.AddAggregate<SlidingWindowAggregate, ComplexDataItem, ProcessedItem, ProcessedItem, ProcessedItem>("agg");
            var sink = b.AddSink<BlackHoleSink<ProcessedItem>, ProcessedItem>("sink");

            _ = b.Connect(src, agg);
            _ = b.Connect(agg, sink);
        }
    }

    // ------------------------------------------------------------------------ 
    // Node implementations
    // ------------------------------------------------------------------------

    private sealed class DataGeneratorSource : SourceNode<ComplexDataItem>
    {
        public override IDataPipe<ComplexDataItem> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<ComplexDataItem>(
                BenchmarkDataGenerators.GenerateComplexData(count, 1, cancellationToken),
                "dataGenerator");
        }
    }

    private sealed class StreamingTransform : TransformNode<ComplexDataItem, ProcessedItem>
    {
        public override async Task<ProcessedItem> ExecuteAsync(ComplexDataItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Process item without materializing intermediate collections
            await Task.Yield(); // Simulate some processing work

            return new ProcessedItem
            {
                Id = item.Id,
                ProcessedValue = item.Data.Length * 2,
                ProcessingTime = DateTime.UtcNow,
                MemoryEfficient = true,
            };
        }
    }

    private sealed class MaterializingTransform : TransformNode<ComplexDataItem, ProcessedItem>
    {
        public override async Task<ProcessedItem> ExecuteAsync(ComplexDataItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Materialize entire item in memory for processing
            var materializedData = item.Data.ToArray(); // Forces allocation
            var processedData = new byte[materializedData.Length];

            // Simulate intensive processing
            await Task.Delay(1, cancellationToken);

            Array.Copy(materializedData, processedData, materializedData.Length);

            return new ProcessedItem
            {
                Id = item.Id,
                ProcessedValue = processedData.Length,
                ProcessingTime = DateTime.UtcNow,
                MemoryEfficient = false,
            };
        }
    }

    private sealed class HybridTransform : TransformNode<ComplexDataItem, ProcessedItem>
    {
        public override async Task<ProcessedItem> ExecuteAsync(ComplexDataItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Selective materialization based on item properties
            if (item.Data.Length > 512) // Only materialize large items
            {
                var materializedData = item.Data.ToArray();
                var processedData = new byte[materializedData.Length];
                Array.Copy(materializedData, processedData, materializedData.Length);

                await Task.Delay(2, cancellationToken);

                return new ProcessedItem
                {
                    Id = item.Id,
                    ProcessedValue = processedData.Length,
                    ProcessingTime = DateTime.UtcNow,
                    MemoryEfficient = false,
                };
            }

            // Stream processing for small items
            await Task.Yield();

            return new ProcessedItem
            {
                Id = item.Id,
                ProcessedValue = item.Data.Length * 2,
                ProcessingTime = DateTime.UtcNow,
                MemoryEfficient = true,
            };
        }
    }

    private sealed class LowAllocationTransform : TransformNode<ComplexDataItem, ProcessedItem>
    {
        // Pre-allocated reusable buffer to minimize allocations
        private readonly byte[] _reusableBuffer = new byte[2048];

        public override async Task<ProcessedItem> ExecuteAsync(ComplexDataItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Process with minimal allocations
            var dataSize = Math.Min(item.Data.Length, _reusableBuffer.Length);

            // Copy only what we need to process
            for (var i = 0; i < dataSize; i++)
            {
                _reusableBuffer[i] = (byte)(item.Data[i] * 2);
            }

            await Task.Yield();

            return new ProcessedItem
            {
                Id = item.Id,
                ProcessedValue = dataSize,
                ProcessingTime = DateTime.UtcNow,
                MemoryEfficient = true,
            };
        }
    }

    private sealed class HighAllocationTransform : TransformNode<ComplexDataItem, ProcessedItem>
    {
        public override async Task<ProcessedItem> ExecuteAsync(ComplexDataItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Intentionally allocate multiple intermediate objects
            var buffers = new List<byte[]>();

            for (var i = 0; i < 5; i++)
            {
                buffers.Add(new byte[item.Data.Length]);
                Array.Copy(item.Data, buffers[i], item.Data.Length);
            }

            // Simulate processing with multiple allocations
            await Task.Delay(2, cancellationToken);

            var finalBuffer = new byte[item.Data.Length];

            for (var i = 0; i < item.Data.Length; i++)
            {
                finalBuffer[i] = (byte)buffers.Average(b => b.Average(x => x));
            }

            return new ProcessedItem
            {
                Id = item.Id,
                ProcessedValue = finalBuffer.Length,
                ProcessingTime = DateTime.UtcNow,
                MemoryEfficient = false,
            };
        }
    }

    private sealed class BatchProcessingTransform : TransformNode<IReadOnlyCollection<ComplexDataItem>, ProcessedItem>
    {
        public override async Task<ProcessedItem> ExecuteAsync(IReadOnlyCollection<ComplexDataItem> batch, PipelineContext context,
            CancellationToken cancellationToken)
        {
            // Process entire batch efficiently
            var results = new List<ProcessedItem>();

            foreach (var item in batch)
            {
                await Task.Yield();

                results.Add(new ProcessedItem
                {
                    Id = item.Id,
                    ProcessedValue = item.Data.Length,
                    ProcessingTime = DateTime.UtcNow,
                    MemoryEfficient = true,
                });
            }

            // Return first result for simplicity (in real scenario would process all)
            return results.FirstOrDefault() ?? new ProcessedItem();
        }
    }

    private sealed class TumblingWindowAggregate : AggregateNode<ComplexDataItem, ProcessedItem, ProcessedItem>
    {
        public TumblingWindowAggregate() : base(WindowAssigner.Tumbling(TimeSpan.FromSeconds(1)))
        {
        }

        public override ProcessedItem GetKey(ComplexDataItem item)
        {
            return new ProcessedItem { Id = item.Id % 10, ProcessedValue = 0, MemoryEfficient = true }; // Create 10 different windows
        }

        public override ProcessedItem CreateAccumulator()
        {
            return new ProcessedItem { Id = 0, ProcessedValue = 0, MemoryEfficient = true };
        }

        public override ProcessedItem Accumulate(ProcessedItem accumulator, ComplexDataItem item)
        {
            return new ProcessedItem
            {
                Id = accumulator.Id,
                ProcessedValue = accumulator.ProcessedValue + item.Data.Length,
                MemoryEfficient = accumulator.MemoryEfficient,
            };
        }
    }

    private sealed class SlidingWindowAggregate : AggregateNode<ComplexDataItem, ProcessedItem, ProcessedItem>
    {
        public SlidingWindowAggregate() : base(WindowAssigner.Sliding(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(100)))
        {
        }

        public override ProcessedItem GetKey(ComplexDataItem item)
        {
            return new ProcessedItem { Id = item.Id % 10, ProcessedValue = 0, MemoryEfficient = true }; // Create 10 different windows
        }

        public override ProcessedItem CreateAccumulator()
        {
            return new ProcessedItem { Id = 0, ProcessedValue = 0, MemoryEfficient = true };
        }

        public override ProcessedItem Accumulate(ProcessedItem accumulator, ComplexDataItem item)
        {
            return new ProcessedItem
            {
                Id = accumulator.Id,
                ProcessedValue = accumulator.ProcessedValue + item.Data.Length,
                MemoryEfficient = accumulator.MemoryEfficient,
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
///     Represents a processed data item for memory efficiency benchmarks.
/// </summary>
public record ProcessedItem
{
    public int Id { get; init; }
    public int ProcessedValue { get; init; }
    public DateTime ProcessingTime { get; init; }
    public bool MemoryEfficient { get; init; }
}
