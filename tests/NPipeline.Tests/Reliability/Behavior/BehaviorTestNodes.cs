using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     Skip reasons for behavior tests that pin a known defect. Each test fails against today's code; the phase that
///     fixes the defect removes the skip. IDs refer to the defect register in <c>plans/resilience-improvements.md</c>.
/// </summary>
internal static class Defects
{
    public const string R1 = "R1 (Phase 5): a resilient node buffers its whole streaming input before processing";
    public const string R2 = "R2 (Phase 5): a resilient node fails with zero errors once its input exceeds MaxMaterializedItems";
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

    public static async Task RunAsync(Action<PipelineBuilder> define, IExecutionObserver? observer = null,
        CancellationToken cancellationToken = default)
    {
        var runner = PipelineRunner.Create();

        await using var context = new PipelineContext();

        if (observer is not null)
            context.Observability.ExecutionObserver = observer;

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
///     A source whose first <see cref="OpenStream" /> fails, which makes the node-retry layer (L3) run it again.
/// </summary>
internal sealed class FailsToOpenOnceSource(IEnumerable<int> items) : SourceNode<int>
{
    private int _opens;

    public int Opens => _opens;

    public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        if (Interlocked.Increment(ref _opens) == 1)
            throw new TimeoutException("transient failure opening the source");

        return StreamingSource<int>.Of(items).OpenStream(context, cancellationToken);
    }
}

/// <summary>
///     Records the retry events raised to <see cref="IExecutionObserver.OnRetry" />.
/// </summary>
internal sealed class RecordingObserver : IExecutionObserver
{
    private readonly ConcurrentQueue<NodeRetryEvent> _retries = new();

    public IReadOnlyList<NodeRetryEvent> Retries => [.. _retries];

    public void OnRetry(NodeRetryEvent e)
    {
        _retries.Enqueue(e);
    }

    public void OnNodeStarted(NodeExecutionStarted e)
    {
    }

    public void OnNodeCompleted(NodeExecutionCompleted e)
    {
    }

    public void OnDrop(QueueDropEvent e)
    {
    }

    public void OnQueueMetrics(QueueMetricsEvent e)
    {
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
///     A policy that answers every item failure with a fixed decision, whatever the node's options say.
/// </summary>
internal sealed class FixedDecisionPolicy(ResilienceDecision itemDecision) : ResiliencePolicyBase
{
    public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
    {
        return ValueTask.FromResult(itemDecision);
    }
}

/// <summary>
///     A backoff that records the retry numbers it is asked for and never waits.
/// </summary>
internal sealed class RecordingBackoff
{
    private readonly ConcurrentQueue<int> _requests = new();

    public IReadOnlyList<int> Requests => [.. _requests];

    public RetryBackoff Backoff => RetryBackoff.Custom(retry =>
    {
        _requests.Enqueue(retry);
        return TimeSpan.Zero;
    });
}
