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
public class LatencyMeasurementBenchmarks
{
    private PipelineContext _ctx = null!;
    private PipelineRunner _runner = null!;

    [Params(1_000, 10_000, 100_000)]
    public int ItemCount { get; set; }

    [Params(1, 5, 10)]
    public int ProcessingDelayMs { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _runner = PipelineRunner.Create();
        _ctx = PipelineContext.Default;
        _ctx.Parameters["count"] = ItemCount;
        _ctx.Parameters["delay"] = ProcessingDelayMs;
    }

    // ------------------------------------------------------------------------ 
    // 1) First-item latency measurements
    // ------------------------------------------------------------------------

    [Benchmark(Baseline = true, Description = "First-item latency measurement")]
    public async Task FirstItemLatency_Baseline()
    {
        await _runner.RunAsync<FirstItemLatencyPipeline>(_ctx);
    }

    [Benchmark(Description = "First-item latency with batching")]
    public async Task FirstItemLatency_WithBatching()
    {
        await _runner.RunAsync<FirstItemLatencyBatchedPipeline>(_ctx);
    }

    [Benchmark(Description = "First-item latency with parallelism")]
    public async Task FirstItemLatency_WithParallelism()
    {
        await _runner.RunAsync<FirstItemLatencyParallelPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 2) P99 latency measurements
    // ------------------------------------------------------------------------

    [Benchmark(Description = "P99 latency measurement")]
    public async Task P99Latency_Baseline()
    {
        await _runner.RunAsync<P99LatencyPipeline>(_ctx);
    }

    [Benchmark(Description = "P99 latency with batching")]
    public async Task P99Latency_WithBatching()
    {
        await _runner.RunAsync<P99LatencyBatchedPipeline>(_ctx);
    }

    [Benchmark(Description = "P99 latency with parallelism")]
    public async Task P99Latency_WithParallelism()
    {
        await _runner.RunAsync<P99LatencyParallelPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 3) End-to-end latency measurements
    // ------------------------------------------------------------------------

    [Benchmark(Description = "End-to-end latency (simple pipeline)")]
    public async Task EndToEndLatency_Simple()
    {
        await _runner.RunAsync<EndToEndLatencySimplePipeline>(_ctx);
    }

    [Benchmark(Description = "End-to-end latency (complex pipeline)")]
    public async Task EndToEndLatency_Complex()
    {
        await _runner.RunAsync<EndToEndLatencyComplexPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 4) Latency under different load conditions
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Latency under low load")]
    public async Task Latency_LowLoad()
    {
        _ctx.Parameters["count"] = 100;
        await _runner.RunAsync<LoadTestPipeline>(_ctx);
    }

    [Benchmark(Description = "Latency under medium load")]
    public async Task Latency_MediumLoad()
    {
        _ctx.Parameters["count"] = 1000;
        await _runner.RunAsync<LoadTestPipeline>(_ctx);
    }

    [Benchmark(Description = "Latency under high load")]
    public async Task Latency_HighLoad()
    {
        _ctx.Parameters["count"] = 10000;
        await _runner.RunAsync<LoadTestPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 5) Latency with different processing patterns
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Latency with synchronous processing")]
    public async Task Latency_SynchronousProcessing()
    {
        await _runner.RunAsync<SynchronousProcessingPipeline>(_ctx);
    }

    [Benchmark(Description = "Latency with asynchronous processing")]
    public async Task Latency_AsynchronousProcessing()
    {
        await _runner.RunAsync<AsynchronousProcessingPipeline>(_ctx);
    }

    [Benchmark(Description = "Latency with mixed processing")]
    public async Task Latency_MixedProcessing()
    {
        await _runner.RunAsync<MixedProcessingPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // Pipeline definitions
    // ------------------------------------------------------------------------

    private sealed class FirstItemLatencyPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");
            var t = b.AddTransform<LatencyMeasuringTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t");
            var sink = b.AddSink<LatencyCollectingSink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class FirstItemLatencyBatchedPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");
            var batch = b.AddBatcher<LatencyMeasuredItem>("batch", 10, TimeSpan.FromSeconds(1));
            var t = b.AddTransform<BatchLatencyTransform, IReadOnlyCollection<LatencyMeasuredItem>, LatencyMeasuredItem>("t");
            var sink = b.AddSink<LatencyCollectingSink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, batch);
            _ = b.Connect(batch, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class FirstItemLatencyParallelPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");

            var t = b.AddTransform<LatencyMeasuringTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t")
                .WithBlockingParallelism(b, 4);

            var sink = b.AddSink<LatencyCollectingSink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class P99LatencyPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");
            var t = b.AddTransform<LatencyMeasuringTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t");
            var sink = b.AddSink<P99LatencyCollectingSink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class P99LatencyBatchedPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");
            var batch = b.AddBatcher<LatencyMeasuredItem>("batch", 50, TimeSpan.FromSeconds(1));
            var t = b.AddTransform<BatchLatencyTransform, IReadOnlyCollection<LatencyMeasuredItem>, LatencyMeasuredItem>("t");
            var sink = b.AddSink<P99LatencyCollectingSink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, batch);
            _ = b.Connect(batch, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class P99LatencyParallelPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");

            var t = b.AddTransform<LatencyMeasuringTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t")
                .WithBlockingParallelism(b, 4);

            var sink = b.AddSink<P99LatencyCollectingSink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class EndToEndLatencySimplePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");
            var t1 = b.AddTransform<LatencyMeasuringTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t1");
            var t2 = b.AddTransform<LatencyMeasuringTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t2");
            var sink = b.AddSink<EndToEndLatencySink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, t1);
            _ = b.Connect(t1, t2);
            _ = b.Connect(t2, sink);
        }
    }

    private sealed class EndToEndLatencyComplexPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");
            var t1 = b.AddTransform<LatencyMeasuringTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t1");
            var t2 = b.AddTransform<LatencyMeasuringTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t2");
            var t3 = b.AddTransform<LatencyMeasuringTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t3");
            var sink = b.AddSink<EndToEndLatencySink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, t1);
            _ = b.Connect(t1, t2);
            _ = b.Connect(t2, t3);
            _ = b.Connect(t3, sink);
        }
    }

    private sealed class LoadTestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");
            var t = b.AddTransform<LatencyMeasuringTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t");
            var sink = b.AddSink<LoadTestLatencySink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class SynchronousProcessingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");
            var t = b.AddTransform<SynchronousLatencyTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t");
            var sink = b.AddSink<LatencyCollectingSink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class AsynchronousProcessingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");
            var t = b.AddTransform<AsynchronousLatencyTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t");
            var sink = b.AddSink<LatencyCollectingSink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    private sealed class MixedProcessingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TimestampedDataSource, LatencyMeasuredItem>("src");
            var t = b.AddTransform<MixedLatencyTransform, LatencyMeasuredItem, LatencyMeasuredItem>("t");
            var sink = b.AddSink<LatencyCollectingSink, LatencyMeasuredItem>("sink");

            _ = b.Connect(src, t);
            _ = b.Connect(t, sink);
        }
    }

    // ------------------------------------------------------------------------ 
    // Node implementations
    // ------------------------------------------------------------------------

    private sealed class TimestampedDataSource : SourceNode<LatencyMeasuredItem>
    {
        public override IDataPipe<LatencyMeasuredItem> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<LatencyMeasuredItem>(
                GenerateTimestampedItems(count, cancellationToken),
                "timestampedDataSource");
        }

        private static async IAsyncEnumerable<LatencyMeasuredItem> GenerateTimestampedItems(int count, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();

            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();

                yield return new LatencyMeasuredItem
                {
                    Id = i,
                    SourceTimestamp = DateTime.UtcNow,
                    ProcessingTimestamp = null,
                    CompletionTimestamp = null,
                };
            }
        }
    }

    private sealed class LatencyMeasuringTransform : TransformNode<LatencyMeasuredItem, LatencyMeasuredItem>
    {
        private readonly Random _random = new();

        public override async Task<LatencyMeasuredItem> ExecuteAsync(LatencyMeasuredItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            var delay = context.Parameters.TryGetValue("delay", out var d)
                ? Convert.ToInt32(d)
                : 1;

            // Simulate processing time with some variance
            var actualDelay = delay + _random.Next(-1, 2);

            if (actualDelay > 0)
                await Task.Delay(actualDelay, cancellationToken);

            return new LatencyMeasuredItem
            {
                Id = item.Id,
                SourceTimestamp = item.SourceTimestamp,
                ProcessingTimestamp = DateTime.UtcNow,
                CompletionTimestamp = null,
            };
        }
    }

    private sealed class BatchLatencyTransform : TransformNode<IReadOnlyCollection<LatencyMeasuredItem>, LatencyMeasuredItem>
    {
        public override async Task<LatencyMeasuredItem> ExecuteAsync(IReadOnlyCollection<LatencyMeasuredItem> batch, PipelineContext context,
            CancellationToken cancellationToken)
        {
            // Process the batch and return the first item with updated timestamp
            await Task.Yield();

            var firstItem = batch.FirstOrDefault();

            if (firstItem is null)
                return new LatencyMeasuredItem { Id = -1 };

            return new LatencyMeasuredItem
            {
                Id = firstItem.Id,
                SourceTimestamp = firstItem.SourceTimestamp,
                ProcessingTimestamp = DateTime.UtcNow,
                CompletionTimestamp = null,
                BatchSize = batch.Count,
            };
        }
    }

    private sealed class SynchronousLatencyTransform : TransformNode<LatencyMeasuredItem, LatencyMeasuredItem>
    {
        public override Task<LatencyMeasuredItem> ExecuteAsync(LatencyMeasuredItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Synchronous processing without async delay
            Thread.SpinWait(1000); // Small CPU-bound work

            return Task.FromResult(new LatencyMeasuredItem
            {
                Id = item.Id,
                SourceTimestamp = item.SourceTimestamp,
                ProcessingTimestamp = DateTime.UtcNow,
                CompletionTimestamp = null,
            });
        }
    }

    private sealed class AsynchronousLatencyTransform : TransformNode<LatencyMeasuredItem, LatencyMeasuredItem>
    {
        public override async Task<LatencyMeasuredItem> ExecuteAsync(LatencyMeasuredItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            var delay = context.Parameters.TryGetValue("delay", out var d)
                ? Convert.ToInt32(d)
                : 1;

            // Asynchronous processing with await
            await Task.Delay(delay, cancellationToken);

            return new LatencyMeasuredItem
            {
                Id = item.Id,
                SourceTimestamp = item.SourceTimestamp,
                ProcessingTimestamp = DateTime.UtcNow,
                CompletionTimestamp = null,
            };
        }
    }

    private sealed class MixedLatencyTransform : TransformNode<LatencyMeasuredItem, LatencyMeasuredItem>
    {
        private readonly Random _random = new();

        public override async Task<LatencyMeasuredItem> ExecuteAsync(LatencyMeasuredItem item, PipelineContext context, CancellationToken cancellationToken)
        {
            var delay = context.Parameters.TryGetValue("delay", out var d)
                ? Convert.ToInt32(d)
                : 1;

            // Randomly choose between sync and async processing
            if (_random.NextDouble() < 0.5)
            {
                // Synchronous processing
                Thread.SpinWait(500);
            }
            else
            {
                // Asynchronous processing
                await Task.Delay(delay, cancellationToken);
            }

            return new LatencyMeasuredItem
            {
                Id = item.Id,
                SourceTimestamp = item.SourceTimestamp,
                ProcessingTimestamp = DateTime.UtcNow,
                CompletionTimestamp = null,
            };
        }
    }

    private sealed class LatencyCollectingSink : SinkNode<LatencyMeasuredItem>
    {
        private readonly List<TimeSpan> _firstItemLatencies = [];

        public override async Task ExecuteAsync(IDataPipe<LatencyMeasuredItem> input, PipelineContext context, CancellationToken cancellationToken)
        {
            var firstItem = true;

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                if (firstItem)
                {
                    var firstItemLatency = item.ProcessingTimestamp - item.SourceTimestamp;

                    if (firstItemLatency.HasValue)
                        _firstItemLatencies.Add(firstItemLatency.Value);

                    firstItem = false;
                }

                // Mark completion timestamp
                item.CompletionTimestamp = DateTime.UtcNow;
            }
        }

        public TimeSpan GetAverageFirstItemLatency()
        {
            if (_firstItemLatencies.Count is 0)
                return TimeSpan.Zero;

            return TimeSpan.FromTicks((long)_firstItemLatencies.Average(l => l.Ticks));
        }
    }

    private sealed class P99LatencyCollectingSink : SinkNode<LatencyMeasuredItem>
    {
        private readonly List<TimeSpan> _latencies = [];

        public override async Task ExecuteAsync(IDataPipe<LatencyMeasuredItem> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                var latency = item.ProcessingTimestamp - item.SourceTimestamp;

                if (latency.HasValue)
                    _latencies.Add(latency.Value);

                // Mark completion timestamp
                item.CompletionTimestamp = DateTime.UtcNow;
            }
        }

        public TimeSpan GetP99Latency()
        {
            if (_latencies.Count is 0)
                return TimeSpan.Zero;

            var sortedLatencies = _latencies.OrderBy(l => l.Ticks).ToList();
            var p99Index = (int)Math.Ceiling(sortedLatencies.Count * 0.99) - 1;
            return sortedLatencies[Math.Max(0, p99Index)];
        }
    }

    private sealed class EndToEndLatencySink : SinkNode<LatencyMeasuredItem>
    {
        private readonly List<TimeSpan> _endToEndLatencies = [];

        public override async Task ExecuteAsync(IDataPipe<LatencyMeasuredItem> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                // Calculate end-to-end latency from source to final completion
                var endToEndLatency = DateTime.UtcNow - item.SourceTimestamp;
                _endToEndLatencies.Add(endToEndLatency);
            }
        }

        public TimeSpan GetAverageEndToEndLatency()
        {
            if (_endToEndLatencies.Count is 0)
                return TimeSpan.Zero;

            return TimeSpan.FromTicks((long)_endToEndLatencies.Average(l => l.Ticks));
        }
    }

    private sealed class LoadTestLatencySink : SinkNode<LatencyMeasuredItem>
    {
        private readonly List<TimeSpan> _latencies = [];

        public override async Task ExecuteAsync(IDataPipe<LatencyMeasuredItem> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                var latency = item.ProcessingTimestamp - item.SourceTimestamp;

                if (latency.HasValue)
                    _latencies.Add(latency.Value);

                // Mark completion timestamp
                item.CompletionTimestamp = DateTime.UtcNow;
            }
        }

        public (TimeSpan AverageLatency, TimeSpan P95Latency, TimeSpan P99Latency) GetLatencyMetrics()
        {
            if (_latencies.Count is 0)
                return (TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero);

            var sortedLatencies = _latencies.OrderBy(l => l.Ticks).ToList();
            var averageLatency = TimeSpan.FromTicks((long)_latencies.Average(l => l.Ticks));

            var p95Index = (int)Math.Ceiling(sortedLatencies.Count * 0.95) - 1;
            var p99Index = (int)Math.Ceiling(sortedLatencies.Count * 0.99) - 1;

            var p95Latency = sortedLatencies[Math.Max(0, p95Index)];
            var p99Latency = sortedLatencies[Math.Max(0, p99Index)];

            return (averageLatency, p95Latency, p99Latency);
        }
    }
}

/// <summary>
///     Represents an item with timestamp measurements for latency benchmarks.
/// </summary>
public record LatencyMeasuredItem
{
    public int Id { get; init; }
    public DateTime SourceTimestamp { get; init; }
    public DateTime? ProcessingTimestamp { get; set; }
    public DateTime? CompletionTimestamp { get; set; }
    public int? BatchSize { get; set; }
}
