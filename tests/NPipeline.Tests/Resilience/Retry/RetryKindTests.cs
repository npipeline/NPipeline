using System.Collections.Concurrent;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Resilience.Retry;

/// <summary>
///     Retrying one item and restarting a whole node have very different costs, but both used to share one delay
///     strategy and arrive at the policy as an indistinguishable attempt number. Each layer now has its own backoff,
///     and each reports its own <see cref="RetryKind" />.
/// </summary>
public sealed class RetryKindTests
{
    [Fact]
    public async Task AnItemRetry_UsesTheItemBackoff_AndReportsAnItemRetry()
    {
        var itemBackoff = new ConcurrentQueue<int>();
        var restartBackoff = new ConcurrentQueue<int>();
        var observer = new RecordingObserver();

        await RunAsync(new FlakyItemTransform(failuresBeforeSuccess: 2), itemBackoff, restartBackoff, observer, wrapForRestart: false);

        itemBackoff.Should().Equal(1, 2);
        restartBackoff.Should().BeEmpty();
        observer.Kinds.Should().Equal(RetryKind.ItemRetry, RetryKind.ItemRetry);
    }

    [Fact]
    public async Task ANodeRestart_UsesTheRestartBackoff_AndReportsANodeRestart()
    {
        var itemBackoff = new ConcurrentQueue<int>();
        var restartBackoff = new ConcurrentQueue<int>();
        var observer = new RecordingObserver();

        await RunAsync(new FailsOnceTransform(), itemBackoff, restartBackoff, observer, wrapForRestart: true);

        restartBackoff.Should().Equal(1);
        itemBackoff.Should().BeEmpty();
        observer.Kinds.Should().Equal(RetryKind.NodeRestart);
    }

    private static async Task RunAsync<TTransform>(TTransform transform, ConcurrentQueue<int> itemBackoff, ConcurrentQueue<int> restartBackoff,
        IExecutionObserver observer, bool wrapForRestart) where TTransform : ITransformNode<int, int>
    {
        await using var context = new PipelineContext();
        context.Observability.ExecutionObserver = observer;

        await PipelineRunner.Create().RunAsync(new InlinePipeline(b =>
        {
            var source = b.AddSource<ListSource, int>("source");
            var t = b.AddTransform<TTransform, int, int>("transform");
            var sink = b.AddSink<DiscardSink, int>("sink");

            _ = b.AddPreconfiguredNodeInstance(source.Id, new ListSource([1]))
                .AddPreconfiguredNodeInstance(t.Id, transform)
                .AddPreconfiguredNodeInstance(sink.Id, new DiscardSink())
                .Connect(source, t)
                .Connect(t, sink)
                .WithResilience(o => o with
                {
                    ItemRetry = new ItemRetryOptions { MaxRetries = 3, Backoff = Recording(itemBackoff), Classifier = RetryClassifier.All },
                    NodeRestart = new NodeRestartOptions { MaxRestarts = 3, Backoff = Recording(restartBackoff) },
                });

            if (wrapForRestart)
            {
                // A restart only happens when the stream itself fails, so the item layer must not absorb the failure.
                _ = b.WithResilience(t, o => o with { ItemRetry = ItemRetryOptions.None });
                _ = b.WithResilience(t);
            }
        }), context, CancellationToken.None);
    }

    private static RetryBackoff Recording(ConcurrentQueue<int> requests)
    {
        return RetryBackoff.Custom(retry =>
        {
            requests.Enqueue(retry);
            return TimeSpan.Zero;
        });
    }

    private sealed class InlinePipeline(Action<PipelineBuilder> define) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            define(builder);
        }
    }

    private sealed class ListSource(IReadOnlyList<int> items) : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>(items);
        }
    }

    private sealed class DiscardSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
            }
        }
    }

    private sealed class FlakyItemTransform(int failuresBeforeSuccess) : TransformNode<int, int>
    {
        private int _attempts;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (_attempts++ < failuresBeforeSuccess)
                throw new InvalidOperationException("fails");

            return ValueTask.FromResult(item);
        }
    }

    private sealed class FailsOnceTransform : TransformNode<int, int>
    {
        private int _attempts;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (_attempts++ == 0)
                throw new InvalidOperationException("fails once");

            return ValueTask.FromResult(item);
        }
    }

    private sealed class RecordingObserver : IExecutionObserver
    {
        private readonly ConcurrentQueue<RetryKind> _kinds = new();

        public IReadOnlyList<RetryKind> Kinds => [.. _kinds];

        public void OnRetry(NodeRetryEvent e)
        {
            _kinds.Enqueue(e.Kind);
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
}
