using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Parallelism;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using ParallelOptions = NPipeline.Extensions.Parallelism.ParallelOptions;

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     Node restart (L2) over forward-only inputs, which is what every real connector produces. A restart resumes at
///     the checkpoint, the first input item whose outcome was not delivered, instead of replaying the whole input.
/// </summary>
public sealed class RestartBehaviorTests
{
    public static TheoryData<string> InOrderStrategies => ["sequential", "parallel-ordered"];

    public static TheoryData<string> AllStrategies => ["sequential", "parallel-ordered", "parallel-unordered", "parallel-drop-oldest", "parallel-drop-newest"];

    [Fact]
    public async Task ResilientNode_OverAnUnboundedSource_ProducesOutputBeforeItsInputEnds()
    {
        var sink = new CollectingSink<int>();
        using var cts = new CancellationTokenSource();

        var run = BehaviorPipeline.RunAsync(b => Wire(b, StreamingSource<int>.Unbounded([1, 2, 3]), new FailsOnceOn(), sink, "sequential",
            new NodeRestartOptions { MaxRestarts = 1, MaxReplayWindow = 100, Backoff = RetryBackoff.None }), cancellationToken: cts.Token);

        try
        {
            // The source never completes, so if the node waits for the end of its input nothing ever arrives (R1).
            var arrived = await Task.WhenAny(sink.FirstItemReceived, Task.Delay(TimeSpan.FromSeconds(5))) == sink.FirstItemReceived;

            arrived.Should().BeTrue("a resilient node must stream, not buffer its whole input first");
        }
        finally
        {
            await cts.CancelAsync();

            try
            {
                await run;
            }
            catch (OperationCanceledException)
            {
                // Expected: the test stops the never-ending pipeline.
            }
        }
    }

    [Theory]
    [MemberData(nameof(AllStrategies))]
    public async Task ResilientNode_WithZeroFailures_SucceedsOnAnInputLongerThanTheReplayWindow(string strategy)
    {
        const int window = 100;
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b => Wire(b, StreamingSource<int>.Of(Enumerable.Range(0, window * 20)), new FailsOnceOn(), sink, strategy,
            new NodeRestartOptions { MaxRestarts = 1, MaxReplayWindow = window }));

        // The window bounds what is held for a restart. It is not a limit on the input's length (R2).
        sink.Items.Should().HaveCount(window * 20);
    }

    [Theory]
    [MemberData(nameof(InOrderStrategies))]
    public async Task ARestart_ResumesAtTheCheckpoint_AndDeliversEachOutputExactlyOnce(string strategy)
    {
        var transform = new FailsOnceOn(5, 12, 13);
        var sink = new CollectingSink<int>();
        var observer = new RecordingObserver();

        await BehaviorPipeline.RunAsync(b => Wire(b, StreamingSource<int>.Of(Enumerable.Range(1, 20)), transform, sink, strategy,
            new NodeRestartOptions { MaxRestarts = 3, Backoff = RetryBackoff.None }), observer: observer);

        // R3: nothing delivered before a failure is delivered again, and nothing is lost.
        sink.Items.Should().Equal(Enumerable.Range(1, 20));
        // In parallel, items 12 and 13 can fail in the same run, so one restart may cover both.
        observer.Retries.Should().HaveCountGreaterThanOrEqualTo(2).And.OnlyContain(e => e.Kind == RetryKind.NodeRestart);

        if (strategy == "sequential")
        {
            observer.Retries.Should().HaveCount(3);

            // Only the item that failed is processed again.
            transform.CallsFor(1).Should().Be(1);
            transform.CallsFor(5).Should().Be(2);
            transform.CallsFor(20).Should().Be(1);
        }
    }

    [Theory]
    [MemberData(nameof(AllStrategies))]
    public async Task ARestart_LosesNoItem_WhateverTheStrategy(string strategy)
    {
        const int window = 16;
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b => Wire(b, StreamingSource<int>.Of(Enumerable.Range(1, 200)), new FailsOnceOn(17, 90, 91, 150), sink, strategy,
            new NodeRestartOptions { MaxRestarts = 4, MaxReplayWindow = window, Backoff = RetryBackoff.None }));

        // Out-of-order strategies deliver at least once: an output delivered ahead of the checkpoint comes again after a
        // restart. The checkpoint cannot pass the failed item, and reading stops a replay window past the checkpoint, so
        // each failure repeats fewer outputs than the window holds.
        sink.Items.Distinct().Should().BeEquivalentTo(Enumerable.Range(1, 200));
        sink.Items.Count.Should().BeLessThanOrEqualTo(200 + 4 * window);
    }

    [Fact]
    public async Task ARestart_NeverDeadLettersOrSkipsAnItemTwice()
    {
        var deadLetters = new CollectingDeadLetterSink();
        var transform = new FailsOnceOn(8) { AlwaysInvalid = [3, 9] };
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b => WireDeadLettering(b, transform, sink, deadLetters, "sequential"));

        // A dead-lettered item's outcome is delivered, so the checkpoint moves past it and a restart does not repeat it.
        sink.Items.Should().Equal(1, 2, 4, 5, 6, 7, 8, 10);
        deadLetters.Envelopes.Select(e => e.Item).Should().Equal(3, 9);
    }

    [Fact]
    public async Task AnOrderedParallelRestart_DeliversOutputsOnce_ButMayDeadLetterAnInFlightItemAgain()
    {
        var deadLetters = new CollectingDeadLetterSink();
        var transform = new FailsOnceOn(8) { AlwaysInvalid = [3, 9] };
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b => WireDeadLettering(b, transform, sink, deadLetters, "parallel-ordered"));

        // Workers run ahead of the checkpoint, and dead-lettering happens when an item is processed, so an item in
        // flight at the failure (such as 9, processed before 8 was delivered) can be dead-lettered by both runs.
        sink.Items.Should().Equal(1, 2, 4, 5, 6, 7, 8, 10);
        deadLetters.Envelopes.Select(e => (int)e.Item).Distinct().Should().BeEquivalentTo([3, 9]);
    }

    private static void WireDeadLettering(PipelineBuilder builder, FailsOnceOn transform, CollectingSink<int> sink, CollectingDeadLetterSink deadLetters,
        string strategy)
    {
        Wire(builder, StreamingSource<int>.Of(Enumerable.Range(1, 10)), transform, sink, strategy,
            new NodeRestartOptions { MaxRestarts = 1, Backoff = RetryBackoff.None });

        // Invalid items are dead-lettered; the transient failure on item 8 fails the stream and restarts it.
        _ = builder.AddDeadLetterSink(deadLetters).AddResiliencePolicy(new DeadLetterInvalidData());
    }

    [Fact]
    public async Task AFailureOfTheInputItself_IsNotRestarted()
    {
        var observer = new RecordingObserver();
        var sink = new CollectingSink<int>();
        var source = new StreamingSource<int>(ct => FailAfter([1, 2], new TimeoutException("the source's connection dropped"), ct));

        var act = () => BehaviorPipeline.RunAsync(b => Wire(b, source, new FailsOnceOn(), sink, "sequential",
            new NodeRestartOptions { MaxRestarts = 3, Backoff = RetryBackoff.None }), observer: observer);

        // The input cannot be read again, so a restart could only fail the same way or lose items.
        var thrown = await act.Should().ThrowAsync<Exception>();
        Flatten(thrown.Which).Should().Contain(e => e is TimeoutException && e.Message == "the source's connection dropped");
        sink.Items.Should().Equal(1, 2);
        observer.Retries.Should().BeEmpty();
    }

    [Fact]
    public async Task ExhaustedRestarts_SurfaceARetryExhaustedExceptionNamingTheNode()
    {
        var act = () => BehaviorPipeline.RunAsync(b => Wire(b, StreamingSource<int>.Of([1, 2, 3]), new FlakyTransform(failuresPerItem: 100),
            new CollectingSink<int>(), "sequential", new NodeRestartOptions { MaxRestarts = 2, Backoff = RetryBackoff.None }));

        var thrown = await act.Should().ThrowAsync<Exception>();

        // The node reading the failed stream reports the exhaustion as its cause, without any hand-off through the context.
        Flatten(thrown.Which).OfType<RetryExhaustedException>().Should().ContainSingle()
            .Which.Should().Match<RetryExhaustedException>(e => e.NodeId == "transform" && e.AttemptCount == 3);
    }

    [Fact]
    public async Task ResetAfterItems_GivesALongRunningStreamItsRestartsBack()
    {
        var sink = new CollectingSink<int>();

        // One restart allowed, three failures spread out: without the reset the second failure would exhaust it.
        await BehaviorPipeline.RunAsync(b => Wire(b, StreamingSource<int>.Of(Enumerable.Range(1, 30)), new FailsOnceOn(5, 15, 25), sink, "sequential",
            new NodeRestartOptions { MaxRestarts = 1, ResetAfterItems = 5, Backoff = RetryBackoff.None }));

        sink.Items.Should().Equal(Enumerable.Range(1, 30));
    }

    [Fact]
    public async Task WithoutResetAfterItems_RestartsAreCountedOverTheWholeStream()
    {
        var act = () => BehaviorPipeline.RunAsync(b => Wire(b, StreamingSource<int>.Of(Enumerable.Range(1, 30)), new FailsOnceOn(5, 15), new CollectingSink<int>(),
            "sequential", new NodeRestartOptions { MaxRestarts = 1, Backoff = RetryBackoff.None }));

        var thrown = await act.Should().ThrowAsync<Exception>();
        Flatten(thrown.Which).Should().Contain(e => e is RetryExhaustedException);
    }

    [Fact]
    public async Task TheRestartPolicy_IsToldTheCheckpointAndWhatWasDelivered()
    {
        var policy = new RecordingRestartPolicy();

        await BehaviorPipeline.RunAsync(b =>
        {
            Wire(b, StreamingSource<int>.Of(Enumerable.Range(1, 10)), new FailsOnceOn(7), new CollectingSink<int>(), "sequential",
                new NodeRestartOptions { MaxRestarts = 1, Backoff = RetryBackoff.None });

            _ = b.AddResiliencePolicy(policy);
        });

        // Items 1-6 were delivered; item 7, at index 6, is where the restart resumes.
        policy.Failures.Should().ContainSingle().Which.Should().Match<StreamFailure>(f => f.Checkpoint == 6 && f.Delivered == 6 && f.Attempt == 1);
    }

    private static void Wire<TTransform>(PipelineBuilder builder, StreamingSource<int> source, TTransform transform, CollectingSink<int> sink,
        string strategy, NodeRestartOptions restart)
        where TTransform : ITransformNode<int, int>
    {
        var s = builder.AddSource<StreamingSource<int>, int>("source");
        var t = builder.AddTransform<TTransform, int, int>("transform");
        var k = builder.AddSink<CollectingSink<int>, int>("sink");

        _ = builder.AddPreconfiguredNodeInstance(s.Id, source)
            .AddPreconfiguredNodeInstance(t.Id, transform)
            .AddPreconfiguredNodeInstance(k.Id, sink)
            .Connect(s, t)
            .Connect(t, k)
            .WithResilience(t, o => o with { ItemRetry = ItemRetryOptions.None, NodeRestart = restart });

        if (strategy == "sequential")
            return;

        _ = builder.WithExecutionStrategy(t, new ParallelExecutionStrategy(4));

        _ = builder.WithParallelOptions(t, strategy switch
        {
            "parallel-unordered" => new ParallelOptions(4, PreserveOrdering: false),
            "parallel-drop-oldest" => new ParallelOptions(4, MaxQueueLength: 10_000, QueuePolicy: BoundedQueuePolicy.DropOldest),
            "parallel-drop-newest" => new ParallelOptions(4, MaxQueueLength: 10_000, QueuePolicy: BoundedQueuePolicy.DropNewest),
            _ => new ParallelOptions(4, MaxQueueLength: 8),
        });
    }

    private static IEnumerable<Exception> Flatten(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            yield return current;
        }
    }

    private static async IAsyncEnumerable<int> FailAfter(IEnumerable<int> items, Exception failure, [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var item in items)
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }

        await Task.Yield();
        throw failure;
    }

    /// <summary>
    ///     Passes items through, failing the first call for each listed item with a transient exception. Items in
    ///     <see cref="AlwaysInvalid" /> fail every time with <see cref="InvalidDataException" />.
    /// </summary>
    private sealed class FailsOnceOn(params int[] failOnce) : TransformNode<int, int>
    {
        private readonly ConcurrentDictionary<int, int> _calls = new();

        public int[] AlwaysInvalid { get; init; } = [];

        public int CallsFor(int item)
        {
            return _calls.GetValueOrDefault(item);
        }

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            var calls = _calls.AddOrUpdate(item, 1, (_, n) => n + 1);

            if (AlwaysInvalid.Contains(item))
                throw new InvalidDataException($"item {item} is invalid");

            if (calls == 1 && failOnce.Contains(item))
                throw new TimeoutException($"transient failure on item {item}");

            return ValueTask.FromResult(item);
        }
    }

    /// <summary>
    ///     Dead-letters invalid items; every other decision follows the node's options.
    /// </summary>
    private sealed class DeadLetterInvalidData : ResiliencePolicyBase
    {
        public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            return failure.Exception is InvalidDataException
                ? ValueTask.FromResult(ResilienceDecision.DeadLetter)
                : base.DecideItemFailureAsync(failure, cancellationToken);
        }
    }

    private sealed class RecordingRestartPolicy : ResiliencePolicyBase
    {
        private readonly ConcurrentQueue<StreamFailure> _failures = new();

        public IReadOnlyList<StreamFailure> Failures => [.. _failures];

        public override ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
        {
            _failures.Enqueue(failure);
            return base.DecideRestartAsync(failure, cancellationToken);
        }
    }
}
