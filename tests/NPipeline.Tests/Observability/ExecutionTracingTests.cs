using System.Collections.Concurrent;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Observability;

/// <summary>
///     Verifies the spans the execution strategies start: an item's transform span ends before the item is handed
///     downstream, and a restarted node has one resilience span covering the whole run.
/// </summary>
public sealed class ExecutionTracingTests
{
    [Fact]
    public async Task ItemTransformSpan_DoesNotIncludeDownstreamProcessingTime()
    {
        // Arrange
        var tracer = new RecordingTracer();
        var context = new PipelineContext(new PipelineContextConfiguration { Tracer = tracer });

        // Act - the sink sleeps far longer than the transform, so a span covering downstream work would be slow.
        await PipelineRunner.Create().RunAsync(new SlowDownstreamPipeline(), context);

        // Assert
        var spans = tracer.Spans.Where(static s => s.Name == "Item.Transform").ToList();
        _ = spans.Should().NotBeEmpty();

        foreach (var span in spans)
            _ = span.Duration.Should().BeLessThan(TimeSpan.FromMilliseconds(80), "a transform span must not cover the downstream sink's work");
    }

    [Fact]
    public async Task NodeResilienceSpan_SpansTheRun_AndCarriesTheRestartException()
    {
        // Arrange
        var tracer = new RecordingTracer();
        var context = new PipelineContext(new PipelineContextConfiguration { Tracer = tracer });

        // Act
        await PipelineRunner.Create().RunAsync(new RestartingPipeline(), context);

        // Assert - exactly one resilience span, and the restart's exception is recorded on it.
        var resilience = tracer.Spans.Should().ContainSingle(static s => s.Name == "Node.Resilience").Which;
        _ = resilience.Exceptions.Should().ContainSingle().Which.Should().BeOfType<InvalidOperationException>();
        _ = resilience.Duration.Should().BeGreaterThan(TimeSpan.Zero, "the span must stay open across the run");
    }

    private sealed class SlowDownstreamPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSourceNode, int>("source");
            var transform = builder.AddTransform<TestTransformNode, int, int>("transform");
            var sink = builder.AddSink<SlowSinkNode, int>("sink");

            _ = builder.Connect(source, transform).Connect(transform, sink);
        }
    }

    private sealed class RestartingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSourceNode, int>("source");
            var transform = builder.AddTransform<RestartOnceTransformNode, int, int>("transform");
            var sink = builder.AddSink<DrainSinkNode, int>("sink");

            _ = builder.Connect(source, transform).Connect(transform, sink);
            builder.WithResilience(transform, o => o with
            {
                NodeRestart = new NodeRestartOptions { MaxRestarts = 1, MaxReplayWindow = 16, Backoff = RetryBackoff.None },
            });
        }
    }

    private sealed class TestSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1, 2, 3, 4, 5], "source-output");
    }

    private sealed class TestTransformNode : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(item * 2);
    }

    private sealed class RestartOnceTransformNode : TransformNode<int, int>
    {
        private int _failures;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == 3 && Interlocked.Exchange(ref _failures, 1) == 0)
                throw new InvalidOperationException("restart the node");

            return ValueTask.FromResult(item * 2);
        }
    }

    private sealed class SlowSinkNode : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
                await Task.Delay(100, cancellationToken);
        }
    }

    private sealed class DrainSinkNode : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }

    private sealed record SpanRecord(string Name, DateTimeOffset Started, DateTimeOffset Stopped, IReadOnlyList<Exception> Exceptions)
    {
        public TimeSpan Duration => Stopped - Started;
    }

    private sealed class RecordingTracer : IPipelineTracer
    {
        private readonly ConcurrentStack<RecordingActivity> _active = new();
        private readonly ConcurrentQueue<SpanRecord> _spans = new();

        public IReadOnlyList<SpanRecord> Spans => [.. _spans];

        public IPipelineActivity? CurrentActivity => _active.TryPeek(out var activity) ? activity : null;

        public IPipelineActivity StartActivity(string name)
        {
            var activity = new RecordingActivity(name, this);
            _active.Push(activity);
            return activity;
        }

        private void Complete(RecordingActivity activity)
        {
            _ = _active.TryPop(out _);
            _spans.Enqueue(new SpanRecord(activity.Name, activity.StartedAt, DateTimeOffset.UtcNow, [.. activity.Exceptions]));
        }

        private sealed class RecordingActivity(string name, RecordingTracer owner) : IPipelineActivity
        {
            private int _disposed;

            public string Name { get; } = name;

            public DateTimeOffset StartedAt { get; } = DateTimeOffset.UtcNow;

            public ConcurrentQueue<Exception> Exceptions { get; } = new();

            public void SetTag(string key, object value)
            {
            }

            public void RecordException(Exception exception) => Exceptions.Enqueue(exception);

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                    owner.Complete(this);
            }
        }
    }
}
