using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Extensions.Parallelism;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Benchmarks.Benchmarks;

/// <summary>
///     Item-retry throughput baseline for the resilience redesign (plans/resilience-improvements.md, Phase 0).
///     Classification and breaker bookkeeping must not regress <see cref="RetryScenario.RetryZeroFailures" />.
/// </summary>
/// <remarks>
///     Retry delays are pinned to zero so the numbers measure the retry machinery, not time spent sleeping.
/// </remarks>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.Declared)]
public class ItemRetryBenchmarks
{
    public enum RetryScenario
    {
        /// <summary>No retry configured: the default policy fails any item error.</summary>
        NoRetry,

        /// <summary>A retrying policy is configured but no item ever fails.</summary>
        RetryZeroFailures,

        /// <summary>One item in a hundred fails once with a transient error, then succeeds on retry.</summary>
        OnePercentTransient,
    }

    private const string ScenarioKey = "retry.scenario";
    private const string ParallelKey = "retry.parallel";
    private const string CountKey = "retry.count";

    private const int ItemCount = 10_000;

    private PipelineRunner _runner = null!;

    [Params(RetryScenario.NoRetry, RetryScenario.RetryZeroFailures, RetryScenario.OnePercentTransient)]
    public RetryScenario Scenario { get; set; }

    [Params(false, true)]
    public bool Parallel { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _runner = PipelineRunner.Create();
    }

    [Benchmark(Description = "Item retry: source -> transform -> sink")]
    public async Task ItemRetry()
    {
        await using var context = PipelineContext.CreateDefault();
        context.Parameters[ScenarioKey] = Scenario;
        context.Parameters[ParallelKey] = Parallel;
        context.Parameters[CountKey] = ItemCount;

        await _runner.RunAsync<RetryPipeline>(context);
    }

    private sealed class RetryPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var scenario = (RetryScenario)c.Parameters[ScenarioKey];

            var src = b.AddSource<GenSource, int>("src");
            var t = b.AddTransform<OnePercentFlakyTransform, int, int>("t");
            var sink = b.AddSink<BlackHoleSink, int>("sink");

            b.Connect(src, t).Connect(t, sink);

            if ((bool)c.Parameters[ParallelKey])
                b.WithExecutionStrategy(t, new ParallelExecutionStrategy(4));

            // The default policy and classifier decide, so the retry cases include classification.
            b.WithResilience(o => scenario == RetryScenario.NoRetry
                ? PipelineResilienceOptions.None
                : o with { ItemRetry = ItemRetryOptions.Default with { Backoff = RetryBackoff.None } });
        }
    }

    private sealed class GenSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = (int)context.Parameters[CountKey];
            return new DataStream<int>(Stream(cancellationToken), "gen");

            async IAsyncEnumerable<int> Stream([EnumeratorCancellation] CancellationToken ct)
            {
                await Task.Yield();

                for (var i = 0; i < count; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return i;
                }
            }
        }
    }

    /// <summary>
    ///     Fails every hundredth item on its first attempt under <see cref="RetryScenario.OnePercentTransient" />, and
    ///     never otherwise. Instantiated per run, so each run starts with no failures recorded.
    /// </summary>
    private sealed class OnePercentFlakyTransform : TransformNode<int, int>
    {
        private readonly ConcurrentDictionary<int, byte> _failedOnce = new();

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item % 100 == 0
                && (RetryScenario)context.Parameters[ScenarioKey] == RetryScenario.OnePercentTransient
                && _failedOnce.TryAdd(item, 0))
                throw new TimeoutException("transient");

            return ValueTask.FromResult(item);
        }
    }

    private sealed class BlackHoleSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // discard
            }
        }
    }
}
