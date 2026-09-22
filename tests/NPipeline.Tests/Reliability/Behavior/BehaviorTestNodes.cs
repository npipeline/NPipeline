using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     Skip reasons for behavior tests that pin a known defect. Each test fails against today's code; the phase that
///     fixes the defect removes the skip. IDs refer to the defect register in <c>plans/resilience-improvements.md</c>.
/// </summary>
internal static class Defects
{
    public const string C1 = "C1 (Phase 2): the Default profile never retries item failures";
    public const string C2 = "C2 (Phase 1): DeadLetter without a dead-letter sink silently drops the item";
    public const string C3 = "C3 (Phase 2): MaxItemRetries caps a policy's own retry rule";
    public const string C5 = "C5 (Phase 1): the Parallelism item-retry loop applies no backoff";
    public const string C6 = "C6 (Phase 2): per-node delay configuration is ignored";
    public const string R1 = "R1 (Phase 5): a resilient node buffers its whole streaming input before processing";
    public const string R2 = "R2 (Phase 5): a resilient node fails with zero errors once its input exceeds MaxMaterializedItems";
    public const string B1 = "B1 (Phase 1): an in-use circuit breaker is evicted and disposed after the inactivity threshold";
}

/// <summary>
///     Runs a pipeline whose shape is supplied by the test, so each test can hand the builder node instances it
///     keeps a reference to and inspect afterwards.
/// </summary>
internal sealed class BehaviorPipeline(Action<PipelineBuilder> define) : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        define(builder);
    }

    public static async Task RunAsync(Action<PipelineBuilder> define, CancellationToken cancellationToken = default)
    {
        var runner = PipelineRunner.Create();

        // The token goes on the context: node execution observes the context's token, not the one RunAsync takes.
        await using var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cancellationToken));
        await runner.RunAsync(new BehaviorPipeline(define), context, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>
///     A forward-only source, which is what every real connector produces. Unlike the in-memory test source it cannot
///     be re-enumerated, so the resilient strategy has to buffer it to support restarts.
/// </summary>
internal sealed class StreamingSource<T>(Func<CancellationToken, IAsyncEnumerable<T>> produce) : SourceNode<T>
{
    public static StreamingSource<T> Of(IEnumerable<T> items)
    {
        return new StreamingSource<T>(ct => Yield(items, ct));
    }

    /// <summary>
    ///     Yields <paramref name="items" /> and then never completes, like a Kafka topic with no more traffic.
    /// </summary>
    public static StreamingSource<T> Unbounded(IEnumerable<T> items)
    {
        return new StreamingSource<T>(ct => YieldThenWait(items, ct));
    }

    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        return new DataStream<T>(produce(cancellationToken), "streaming-source");
    }

    private static async IAsyncEnumerable<T> Yield(IEnumerable<T> items, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private static async IAsyncEnumerable<T> YieldThenWait(IEnumerable<T> items, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>
///     A sink that records what it receives and signals when the first item arrives.
/// </summary>
internal sealed class CollectingSink<T> : SinkNode<T>
{
    private readonly ConcurrentQueue<T> _items = new();
    private readonly TaskCompletionSource _firstItem = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public IReadOnlyList<T> Items => [.. _items];

    public Task FirstItemReceived => _firstItem.Task;

    public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            _items.Enqueue(item);
            _ = _firstItem.TrySetResult();
        }
    }
}

internal sealed class CollectingDeadLetterSink : IDeadLetterSink
{
    private readonly ConcurrentQueue<DeadLetterEnvelope> _envelopes = new();

    public IReadOnlyList<DeadLetterEnvelope> Envelopes => [.. _envelopes];

    public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
    {
        _envelopes.Enqueue(envelope);
        return Task.CompletedTask;
    }
}

/// <summary>
///     Fails each item a fixed number of times with a transient exception, then passes it through.
/// </summary>
internal sealed class FlakyTransform(int failuresPerItem) : TransformNode<int, int>
{
    private readonly ConcurrentDictionary<int, int> _attempts = new();

    /// <summary>
    ///     Total calls to <see cref="TransformAsync" />, across all items.
    /// </summary>
    public int TotalAttempts => _attempts.Values.Sum();

    public int AttemptsFor(int item)
    {
        return _attempts.GetValueOrDefault(item);
    }

    public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
    {
        var attempt = _attempts.AddOrUpdate(item, 1, (_, n) => n + 1);

        if (attempt <= failuresPerItem)
            throw new TimeoutException($"transient failure {attempt} for item {item}");

        return ValueTask.FromResult(item);
    }
}

/// <summary>
///     A policy that answers every item failure with a fixed decision and records the delays it is asked for.
/// </summary>
internal sealed class FixedDecisionPolicy(ResilienceDecision itemDecision) : ResiliencePolicyBase
{
    private readonly ConcurrentQueue<int> _delayRequests = new();

    public IReadOnlyList<int> DelayRequests => [.. _delayRequests];

    public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
        PipelineContext context, string nodeId, int retryAttempt, CancellationToken cancellationToken)
    {
        return Task.FromResult(itemDecision);
    }

    public override ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, RetryKind retryKind, int attemptNumber,
        CancellationToken cancellationToken)
    {
        _delayRequests.Enqueue(attemptNumber);
        return base.GetRetryDelayAsync(context, retryKind, attemptNumber, cancellationToken);
    }
}

/// <summary>
///     Enables the resilient strategy, which refuses to run under the default policy, without ever restarting.
/// </summary>
internal sealed class RestartOnFailurePolicy : ResiliencePolicyBase
{
    public override Task<ResilienceDecision> DecidePipelineFailureAsync(string nodeId, Exception exception, PipelineContext context,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(ResilienceDecision.RestartNode);
    }
}
