// ReSharper disable ClassNeverInstantiated.Local

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Extensions.Parallelism;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class GCPressureBenchmarks
{
    private PipelineContext _ctx = null!;
    private PipelineRunner _runner = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int ItemCount { get; set; }

    [Params(1, 5, 10)]
    public int AllocationFactor { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _runner = PipelineRunner.Create();
        _ctx = PipelineContext.Default;
        _ctx.Parameters["count"] = ItemCount;
        _ctx.Parameters["allocationFactor"] = AllocationFactor;
    }

    // ------------------------------------------------------------------------ 
    // 1) Sustained load with different allocation patterns
    // ------------------------------------------------------------------------

    [Benchmark(Baseline = true, Description = "Low allocation sustained load")]
    public async Task SustainedLoad_LowAllocation()
    {
        _ctx.Parameters["allocationFactor"] = 1;
        await _runner.RunAsync<SustainedLoadPipeline>(_ctx);
    }

    [Benchmark(Description = "Medium allocation sustained load")]
    public async Task SustainedLoad_MediumAllocation()
    {
        _ctx.Parameters["allocationFactor"] = 5;
        await _runner.RunAsync<SustainedLoadPipeline>(_ctx);
    }

    [Benchmark(Description = "High allocation sustained load")]
    public async Task SustainedLoad_HighAllocation()
    {
        _ctx.Parameters["allocationFactor"] = 10;
        await _runner.RunAsync<SustainedLoadPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 2) Memory pressure with different object lifetimes
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Short-lived objects")]
    public async Task MemoryPressure_ShortLived()
    {
        await _runner.RunAsync<ShortLivedObjectsPipeline>(_ctx);
    }

    [Benchmark(Description = "Medium-lived objects")]
    public async Task MemoryPressure_MediumLived()
    {
        await _runner.RunAsync<MediumLivedObjectsPipeline>(_ctx);
    }

    [Benchmark(Description = "Long-lived objects")]
    public async Task MemoryPressure_LongLived()
    {
        await _runner.RunAsync<LongLivedObjectsPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 3) GC pressure with parallelism
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Sequential processing under load")]
    public async Task GCPressure_Sequential()
    {
        await _runner.RunAsync<SequentialGCPipeline>(_ctx);
    }

    [Benchmark(Description = "Parallel processing under load")]
    public async Task GCPressure_Parallel()
    {
        await _runner.RunAsync<ParallelGCPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 4) Large object allocation patterns
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Large object allocation")]
    public async Task LargeObject_Allocation()
    {
        await _runner.RunAsync<LargeObjectPipeline>(_ctx);
    }

    [Benchmark(Description = "Many small objects allocation")]
    public async Task ManySmallObjects_Allocation()
    {
        await _runner.RunAsync<ManySmallObjectsPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 5) GC behavior with batching
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Batching with GC pressure")]
    public async Task Batching_GCPressure()
    {
        await _runner.RunAsync<BatchingGCPipeline>(_ctx);
    }

    [Benchmark(Description = "Streaming with GC pressure")]
    public async Task Streaming_GCPressure()
    {
        await _runner.RunAsync<StreamingGCPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // Pipeline definitions
    // ------------------------------------------------------------------------

    private sealed class SustainedLoadPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, GCPressureTestItem>("src");
            var t = b.AddTransform<SustainedLoadTransform, GCPressureTestItem, ProcessedGCItem>("t");
            var sink = b.AddSink<GCPressureSink, ProcessedGCItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class ShortLivedObjectsPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, GCPressureTestItem>("src");
            var t = b.AddTransform<ShortLivedTransform, GCPressureTestItem, ProcessedGCItem>("t");
            var sink = b.AddSink<GCPressureSink, ProcessedGCItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class MediumLivedObjectsPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, GCPressureTestItem>("src");
            var t = b.AddTransform<MediumLivedTransform, GCPressureTestItem, ProcessedGCItem>("t");
            var sink = b.AddSink<GCPressureSink, ProcessedGCItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class LongLivedObjectsPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, GCPressureTestItem>("src");
            var t = b.AddTransform<LongLivedTransform, GCPressureTestItem, ProcessedGCItem>("t");
            var sink = b.AddSink<GCPressureSink, ProcessedGCItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class SequentialGCPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, GCPressureTestItem>("src");
            var t = b.AddTransform<GCTestTransform, GCPressureTestItem, ProcessedGCItem>("t");
            var sink = b.AddSink<GCPressureSink, ProcessedGCItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class ParallelGCPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, GCPressureTestItem>("src");

            var t = b.AddTransform<GCTestTransform, GCPressureTestItem, ProcessedGCItem>("t")
                .WithBlockingParallelism(b, 4);

            var sink = b.AddSink<GCPressureSink, ProcessedGCItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class LargeObjectPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, GCPressureTestItem>("src");
            var t = b.AddTransform<LargeObjectTransform, GCPressureTestItem, ProcessedGCItem>("t");
            var sink = b.AddSink<GCPressureSink, ProcessedGCItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class ManySmallObjectsPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, GCPressureTestItem>("src");
            var t = b.AddTransform<ManySmallObjectsTransform, GCPressureTestItem, ProcessedGCItem>("t");
            var sink = b.AddSink<GCPressureSink, ProcessedGCItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class BatchingGCPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, GCPressureTestItem>("src");
            var batch = b.AddBatcher<GCPressureTestItem>("batch", 100, TimeSpan.FromSeconds(1));
            var t = b.AddTransform<BatchGCTransform, IReadOnlyCollection<GCPressureTestItem>, ProcessedGCItem>("t");
            var sink = b.AddSink<GCPressureSink, ProcessedGCItem>("sink");

            _ = b.Connect(src, batch);
            _ = b.Connect(batch, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class StreamingGCPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<DataGeneratorSource, GCPressureTestItem>("src");
            var t = b.AddTransform<StreamingGCTransform, GCPressureTestItem, ProcessedGCItem>("t");
            var sink = b.AddSink<GCPressureSink, ProcessedGCItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    // ------------------------------------------------------------------------ 
    // Node implementations
    // ------------------------------------------------------------------------

    private sealed class DataGeneratorSource : SourceNode<GCPressureTestItem>
    {
        public override IDataPipe<GCPressureTestItem> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<GCPressureTestItem>(
                GenerateTestItems(count, cancellationToken),
                "dataGenerator");
        }

        private static async IAsyncEnumerable<GCPressureTestItem> GenerateTestItems(int count, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();

            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();

                yield return new GCPressureTestItem
                {
                    Id = i,
                    Data = new byte[1024], // 1KB per item
                    Timestamp = DateTime.UtcNow,
                };
            }
        }
    }

    private sealed class SustainedLoadTransform : TransformNode<GCPressureTestItem, ProcessedGCItem>
    {
        public override async Task<ProcessedGCItem> ExecuteAsync(GCPressureTestItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            var allocationFactor = context.Parameters.TryGetValue("allocationFactor", out var af)
                ? Convert.ToInt32(af)
                : 1;

            // Simulate sustained processing with controlled allocation
            var buffer = new byte[item.Data.Length * allocationFactor];
            Array.Copy(item.Data, buffer, item.Data.Length);

            await Task.Delay(1, cancellationToken);

            return new ProcessedGCItem
            {
                Id = item.Id,
                ProcessedAt = DateTime.UtcNow,
                AllocatedBytes = buffer.Length,
                Gen0Count = GC.CollectionCount(0),
                Gen1Count = GC.CollectionCount(1),
                Gen2Count = GC.CollectionCount(2),
            };
        }
    }

    private sealed class ShortLivedTransform : TransformNode<GCPressureTestItem, ProcessedGCItem>
    {
        public override async Task<ProcessedGCItem> ExecuteAsync(GCPressureTestItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Create many short-lived objects
            var objects = new List<object>();

            for (var i = 0; i < 10; i++)
            {
                objects.Add(new byte[100]); // Small objects that will be collected quickly
            }

            await Task.Delay(1, cancellationToken);

            return new ProcessedGCItem
            {
                Id = item.Id,
                ProcessedAt = DateTime.UtcNow,
                AllocatedBytes = objects.Sum(o => ((byte[])o).Length),
                Gen0Count = GC.CollectionCount(0),
                Gen1Count = GC.CollectionCount(1),
                Gen2Count = GC.CollectionCount(2),
            };
        }
    }

    private sealed class MediumLivedTransform : TransformNode<GCPressureTestItem, ProcessedGCItem>
    {
        private readonly List<object> _mediumLivedObjects = [];

        public override async Task<ProcessedGCItem> ExecuteAsync(GCPressureTestItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Create medium-lived objects that persist between calls
            if (_mediumLivedObjects.Count < 100)
                _mediumLivedObjects.Add(new byte[1000]);

            await Task.Delay(1, cancellationToken);

            return new ProcessedGCItem
            {
                Id = item.Id,
                ProcessedAt = DateTime.UtcNow,
                AllocatedBytes = 1000,
                Gen0Count = GC.CollectionCount(0),
                Gen1Count = GC.CollectionCount(1),
                Gen2Count = GC.CollectionCount(2),
            };
        }
    }

    private sealed class LongLivedTransform : TransformNode<GCPressureTestItem, ProcessedGCItem>
    {
        private static readonly object[] _longLivedObjects = new object[1000]; // Static to keep alive

        public override async Task<ProcessedGCItem> ExecuteAsync(GCPressureTestItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Use long-lived objects (minimal new allocations)
            var index = item.Id % _longLivedObjects.Length;
            var obj = _longLivedObjects[index];

            if (obj == null)
                _longLivedObjects[index] = new byte[10000]; // Large objects

            await Task.Delay(1, cancellationToken);

            return new ProcessedGCItem
            {
                Id = item.Id,
                ProcessedAt = DateTime.UtcNow,
                AllocatedBytes = 10000,
                Gen0Count = GC.CollectionCount(0),
                Gen1Count = GC.CollectionCount(1),
                Gen2Count = GC.CollectionCount(2),
            };
        }
    }

    private sealed class GCTestTransform : TransformNode<GCPressureTestItem, ProcessedGCItem>
    {
        public override async Task<ProcessedGCItem> ExecuteAsync(GCPressureTestItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Force some GC pressure
            var objects = new List<byte[]>();

            for (var i = 0; i < 50; i++)
            {
                objects.Add(new byte[1024]);
            }

            await Task.Delay(2, cancellationToken);

            return new ProcessedGCItem
            {
                Id = item.Id,
                ProcessedAt = DateTime.UtcNow,
                AllocatedBytes = objects.Sum(o => o.Length),
                Gen0Count = GC.CollectionCount(0),
                Gen1Count = GC.CollectionCount(1),
                Gen2Count = GC.CollectionCount(2),
            };
        }
    }

    private sealed class LargeObjectTransform : TransformNode<GCPressureTestItem, ProcessedGCItem>
    {
        public override async Task<ProcessedGCItem> ExecuteAsync(GCPressureTestItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Allocate a very large object
            var largeObject = new LargeDataObject(1024 * 1024); // 1MB object

            await Task.Delay(5, cancellationToken);

            return new ProcessedGCItem
            {
                Id = item.Id,
                ProcessedAt = DateTime.UtcNow,
                AllocatedBytes = largeObject.Data.Length,
                Gen0Count = GC.CollectionCount(0),
                Gen1Count = GC.CollectionCount(1),
                Gen2Count = GC.CollectionCount(2),
            };
        }
    }

    private sealed class ManySmallObjectsTransform : TransformNode<GCPressureTestItem, ProcessedGCItem>
    {
        public override async Task<ProcessedGCItem> ExecuteAsync(GCPressureTestItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Allocate many small objects
            var objects = new List<byte[]>();

            for (var i = 0; i < 1000; i++)
            {
                objects.Add(new byte[100]); // Many small objects
            }

            await Task.Delay(2, cancellationToken);

            return new ProcessedGCItem
            {
                Id = item.Id,
                ProcessedAt = DateTime.UtcNow,
                AllocatedBytes = objects.Sum(o => o.Length),
                Gen0Count = GC.CollectionCount(0),
                Gen1Count = GC.CollectionCount(1),
                Gen2Count = GC.CollectionCount(2),
            };
        }
    }

    private sealed class BatchGCTransform : TransformNode<IReadOnlyCollection<GCPressureTestItem>, ProcessedGCItem>
    {
        public override async Task<ProcessedGCItem> ExecuteAsync(IReadOnlyCollection<GCPressureTestItem> batch, PipelineContext context,
            CancellationToken cancellationToken)
        {
            // Process batch with minimal allocations
            var totalBytes = batch.Sum(item => item.Data.Length);

            await Task.Delay(5, cancellationToken);

            return new ProcessedGCItem
            {
                Id = batch.FirstOrDefault()?.Id ?? -1,
                ProcessedAt = DateTime.UtcNow,
                AllocatedBytes = totalBytes,
                Gen0Count = GC.CollectionCount(0),
                Gen1Count = GC.CollectionCount(1),
                Gen2Count = GC.CollectionCount(2),
            };
        }
    }

    private sealed class StreamingGCTransform : TransformNode<GCPressureTestItem, ProcessedGCItem>
    {
        public override async Task<ProcessedGCItem> ExecuteAsync(GCPressureTestItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Stream processing with minimal new allocations
            var processedBytes = item.Data.Length;

            await Task.Delay(1, cancellationToken);

            return new ProcessedGCItem
            {
                Id = item.Id,
                ProcessedAt = DateTime.UtcNow,
                AllocatedBytes = processedBytes,
                Gen0Count = GC.CollectionCount(0),
                Gen1Count = GC.CollectionCount(1),
                Gen2Count = GC.CollectionCount(2),
            };
        }
    }

    private sealed class GCPressureSink : SinkNode<ProcessedGCItem>
    {
        private readonly List<ProcessedGCItem> _processedItems = [];

        public override async Task ExecuteAsync(IDataPipe<ProcessedGCItem> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                _processedItems.Add(item);

                // Periodically trigger GC to measure pressure
                if (_processedItems.Count % 1000 == 0)
                    GC.Collect();
            }
        }

        public GCMetrics GetGCMetrics()
        {
            if (_processedItems.Count is 0)
                return new GCMetrics();

            var totalAllocated = _processedItems.Sum(i => i.AllocatedBytes);
            var avgGen0 = _processedItems.Average(i => i.Gen0Count);
            var avgGen1 = _processedItems.Average(i => i.Gen1Count);
            var avgGen2 = _processedItems.Average(i => i.Gen2Count);

            return new GCMetrics
            {
                TotalItems = _processedItems.Count,
                TotalAllocatedBytes = totalAllocated,
                AverageGen0Collections = avgGen0,
                AverageGen1Collections = avgGen1,
                AverageGen2Collections = avgGen2,
                MaxGen0Collections = _processedItems.Max(i => i.Gen0Count),
                MaxGen1Collections = _processedItems.Max(i => i.Gen1Count),
                MaxGen2Collections = _processedItems.Max(i => i.Gen2Count),
            };
        }
    }
}

/// <summary>
///     Represents a test item for GC pressure benchmarks.
/// </summary>
public record GCPressureTestItem
{
    public int Id { get; init; }
    public byte[] Data { get; init; } = [];
    public DateTime Timestamp { get; init; }
}

/// <summary>
///     Represents a processed item with GC metrics for GC pressure benchmarks.
/// </summary>
public record ProcessedGCItem
{
    public int Id { get; init; }
    public DateTime ProcessedAt { get; init; }
    public long AllocatedBytes { get; init; }
    public int Gen0Count { get; init; }
    public int Gen1Count { get; init; }
    public int Gen2Count { get; init; }
}

/// <summary>
///     Represents GC metrics collected during benchmark execution.
/// </summary>
public record GCMetrics
{
    public int TotalItems { get; init; }
    public long TotalAllocatedBytes { get; init; }
    public double AverageGen0Collections { get; init; }
    public double AverageGen1Collections { get; init; }
    public double AverageGen2Collections { get; init; }
    public int MaxGen0Collections { get; init; }
    public int MaxGen1Collections { get; init; }
    public int MaxGen2Collections { get; init; }
}

/// <summary>
///     Represents a large data object for memory allocation testing.
/// </summary>
public class LargeDataObject
{
    public LargeDataObject(int size)
    {
        Data = new byte[size];
    }

    public byte[] Data { get; }
}
