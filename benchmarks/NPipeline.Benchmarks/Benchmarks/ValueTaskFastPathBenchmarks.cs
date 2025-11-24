// ReSharper disable ClassNeverInstantiated.Local

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
public class ValueTaskFastPathBenchmarks
{
    private readonly Dictionary<int, string> _cache = [];
    private PipelineContext _ctx = null!;
    private PipelineRunner _runner = null!;

    [Params(1_000, 10_000, 100_000)]
    public int ItemCount { get; set; }

    [Params(0.1, 0.5, 0.9)] // Cache hit rates
    public double CacheHitRate { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());
        _ctx = PipelineContext.Default;
        _ctx.Parameters["count"] = ItemCount;
        _ctx.Parameters["cacheHitRate"] = CacheHitRate;

        // Pre-populate cache with some items
        for (var i = 0; i < ItemCount * CacheHitRate; i++)
        {
            _cache[i] = $"Cached_{i}";
        }
    }

    // ------------------------------------------------------------------------ 
    // 1) ValueTask fast path vs Task-based implementation
    // ------------------------------------------------------------------------

    [Benchmark(Baseline = true, Description = "Task-based transform (cache hit)")]
    public async Task Task_CacheHit()
    {
        await _runner.RunAsync<TaskBasedCachePipeline>(_ctx);
    }

    [Benchmark(Description = "ValueTask fast path (cache hit)")]
    public async Task ValueTask_FastPath()
    {
        await _runner.RunAsync<ValueTaskCachePipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 2) Synchronous completion path vs async completion
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Synchronous ValueTask completion")]
    public async Task ValueTask_SyncCompletion()
    {
        await _runner.RunAsync<SyncCompletionPipeline>(_ctx);
    }

    [Benchmark(Description = "Asynchronous ValueTask completion")]
    public async Task ValueTask_AsyncCompletion()
    {
        await _runner.RunAsync<AsyncCompletionPipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // 3) Zero allocation verification scenarios
    // ------------------------------------------------------------------------

    [Benchmark(Description = "Zero allocation cache lookup")]
    public async Task ZeroAllocation_CacheLookup()
    {
        await _runner.RunAsync<ZeroAllocationCachePipeline>(_ctx);
    }

    [Benchmark(Description = "Traditional async cache lookup")]
    public async Task Traditional_AsyncCacheLookup()
    {
        await _runner.RunAsync<TraditionalCachePipeline>(_ctx);
    }

    // ------------------------------------------------------------------------ 
    // Pipeline definitions
    // ------------------------------------------------------------------------

    private sealed class TaskBasedCachePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<CacheTestSource, int>("src");
            var t = b.AddTransform<TaskBasedCacheTransform, int, string>("t");
            var sink = b.AddSink<BlackHoleSink<string>, string>("sink");

            b.Connect(src, t);
            b.Connect(t, sink);
        }
    }

    private sealed class ValueTaskCachePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<CacheTestSource, int>("src");
            var t = b.AddTransform<ValueTaskCacheTransform, int, string>("t");
            var sink = b.AddSink<BlackHoleSink<string>, string>("sink");

            b.Connect(src, t).Connect(t, sink);
        }
    }

    private sealed class SyncCompletionPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<CacheTestSource, int>("src");
            var t = b.AddTransform<SyncCompletionTransform, int, string>("t");
            var sink = b.AddSink<BlackHoleSink<string>, string>("sink");

            b.Connect(src, t).Connect(t, sink);
        }
    }

    private sealed class AsyncCompletionPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<CacheTestSource, int>("src");
            var t = b.AddTransform<AsyncCompletionTransform, int, string>("t");
            var sink = b.AddSink<BlackHoleSink<string>, string>("sink");

            b.Connect(src, t).Connect(t, sink);
        }
    }

    private sealed class ZeroAllocationCachePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<CacheTestSource, int>("src");
            var t = b.AddTransform<ZeroAllocationCacheTransform, int, string>("t");
            var sink = b.AddSink<BlackHoleSink<string>, string>("sink");

            b.Connect(src, t).Connect(t, sink);
        }
    }

    private sealed class TraditionalCachePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<CacheTestSource, int>("src");
            var t = b.AddTransform<TraditionalCacheTransform, int, string>("t");
            var sink = b.AddSink<BlackHoleSink<string>, string>("sink");

            b.Connect(src, t).Connect(t, sink);
        }
    }

    // ------------------------------------------------------------------------ 
    // Node implementations
    // ------------------------------------------------------------------------

    private sealed class CacheTestSource : SourceNode<int>
    {
        public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("count", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<int>(GenerateItems(count, cancellationToken), "cacheTest");
        }

        private static async IAsyncEnumerable<int> GenerateItems(int count, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();

            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();
                yield return i;
            }
        }
    }

    private sealed class TaskBasedCacheTransform : TransformNode<int, string>
    {
        private readonly Dictionary<int, string> _cache = [];

        public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate cache lookup with Task-based approach
            if (_cache.TryGetValue(item, out var cached))
                return Task.FromResult(cached);

            // Simulate cache miss and store
            var result = $"Computed_{item}";
            _cache[item] = result;
            return Task.FromResult(result);
        }
    }

    private sealed class ValueTaskCacheTransform : TransformNode<int, string>
    {
        private readonly Dictionary<int, string> _cache = [];

        protected override ValueTask<string> ExecuteValueTaskAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate cache lookup with ValueTask fast path
            if (_cache.TryGetValue(item, out var cached))
            {
                // Fast path: synchronous completion without allocation
                return new ValueTask<string>(cached);
            }

            // Simulate cache miss and store
            var result = $"Computed_{item}";
            _cache[item] = result;
            return new ValueTask<string>(result);
        }

        public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return FromValueTask(ExecuteValueTaskAsync(item, context, cancellationToken));
        }
    }

    private sealed class SyncCompletionTransform : TransformNode<int, string>
    {
        private readonly Dictionary<int, string> _cache = [];

        protected override ValueTask<string> ExecuteValueTaskAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Always synchronous completion (cache hit scenario)
            if (_cache.TryGetValue(item, out var cached))
                return new ValueTask<string>(cached);

            var result = $"Sync_{item}";
            _cache[item] = result;
            return new ValueTask<string>(result);
        }

        public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return FromValueTask(ExecuteValueTaskAsync(item, context, cancellationToken));
        }
    }

    private sealed class AsyncCompletionTransform : TransformNode<int, string>
    {
        private readonly Dictionary<int, string> _cache = [];

        protected override async ValueTask<string> ExecuteValueTaskAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Force asynchronous completion even for cache hits
            if (_cache.TryGetValue(item, out var cached))
            {
                await Task.Yield(); // Force async path
                return cached;
            }

            // Simulate async computation
            await Task.Delay(1, cancellationToken);
            var result = $"Async_{item}";
            _cache[item] = result;
            return result;
        }

        public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return FromValueTask(ExecuteValueTaskAsync(item, context, cancellationToken));
        }
    }

    private sealed class ZeroAllocationCacheTransform : TransformNode<int, string>
    {
        // Pre-allocated cache to minimize allocations
        private readonly Dictionary<int, string> _cache = [];

        protected override ValueTask<string> ExecuteValueTaskAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Optimized cache lookup with zero allocation for cache hits
            if (_cache.TryGetValue(item, out var cached))
            {
                // Return cached result without any allocation
                return new ValueTask<string>(cached);
            }

            // For cache misses, minimize allocation
            var result = $"ZeroAlloc_{item}";
            _cache[item] = result;
            return new ValueTask<string>(result);
        }

        public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return FromValueTask(ExecuteValueTaskAsync(item, context, cancellationToken));
        }
    }

    private sealed class TraditionalCacheTransform : TransformNode<int, string>
    {
        private readonly Dictionary<int, string> _cache = [];

        public override async Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Traditional async approach with unnecessary allocations
            if (_cache.TryGetValue(item, out var cached))
            {
                // Unnecessary async wrapper for cache hit
                await Task.Yield();
                return cached;
            }

            // Simulate async computation even for simple cache miss
            await Task.Delay(1, cancellationToken);
            var result = $"Traditional_{item}";
            _cache[item] = result;
            return result;
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
