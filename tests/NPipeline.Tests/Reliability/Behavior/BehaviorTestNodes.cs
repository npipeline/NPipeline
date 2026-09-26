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
///     Runs a pipeline whose shape is supplied by the test, so each test can hand the builder node instances it
///     keeps a reference to and inspect afterwards.
/// </summary>
internal sealed class BehaviorPipeline(Action<PipelineBuilder> define) : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        define(builder);
    }

    /// <param name="define">Builds the pipeline.</param>
    /// <param name="observer">Receives the run's execution events.</param>
    /// <param name="runner">
    ///     The runner to use. Pass the same one to several runs to share its <see cref="PipelineFactory" />, and with it
    ///     the definition's circuit breakers. Default: a new runner per run.
    /// </param>
    /// <param name="cancellationToken">Cancels the run.</param>
    public static async Task RunAsync(Action<PipelineBuilder> define, IExecutionObserver? observer = null,
        PipelineRunner? runner = null, CancellationToken cancellationToken = default)
    {
        runner ??= PipelineRunner.Create();

        await using var context = new PipelineContext();

        if (observer is not null)
            context.Observability.ExecutionObserver = observer;

        await runner.RunAsync(new BehaviorPipeline(define), context, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>
///     A forward-only source, which is what every real connector produces. Unlike the in-memory test source it cannot
///     be re-enumerated, so a restarted node has to hold what it may need to process again.
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

    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
        new DataStream<T>(produce(cancellationToken), "streaming-source");

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
///     A source that opens its connection lazily, on the first read, and whose read throws a transient exception.
///     Each start of the stream is one open, so a downstream node that re-enumerates the source is observable.
/// </summary>
internal sealed class FailsOnFirstReadSource : SourceNode<int>
{
    private int _opens;

    public int Opens => _opens;

    public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
        new DataStream<int>(Produce(cancellationToken), "fails-on-first-read");

    private async IAsyncEnumerable<int> Produce([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _opens);

        await Task.CompletedTask.ConfigureAwait(false);

        yield return Throw();

        static int Throw() => throw new TimeoutException("transient failure reading the source");
    }
}

/// <summary>
///     Records the resilience events raised to an <see cref="IExecutionObserver" />.
/// </summary>
internal sealed class RecordingObserver : IExecutionObserver
{
    private readonly ConcurrentQueue<CircuitStateChangedEvent> _circuitChanges = new();
    private readonly ConcurrentQueue<RetryExhaustedEvent> _exhaustions = new();
    private readonly ConcurrentQueue<NodeRetryEvent> _retries = new();

    public IReadOnlyList<NodeRetryEvent> Retries => [.. _retries];

    public IReadOnlyList<RetryExhaustedEvent> Exhaustions => [.. _exhaustions];

    public IReadOnlyList<CircuitStateChangedEvent> CircuitChanges => [.. _circuitChanges];

    public void OnRetry(NodeRetryEvent e)
    {
        _retries.Enqueue(e);
    }

    public void OnRetryExhausted(RetryExhaustedEvent e)
    {
        _exhaustions.Enqueue(e);
    }

    public void OnCircuitStateChanged(CircuitStateChangedEvent e)
    {
        _circuitChanges.Enqueue(e);
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
    private readonly TaskCompletionSource _firstItem = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ConcurrentQueue<T> _items = new();

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

    public int AttemptsFor(int item) => _attempts.GetValueOrDefault(item);

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
    public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken) =>
        ValueTask.FromResult(itemDecision);
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
