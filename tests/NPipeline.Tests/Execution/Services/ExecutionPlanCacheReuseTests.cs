using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Execution.Services;

/// <summary>
///     Guards the instance-independence of cached <see cref="NPipeline.Execution.Plans.NodeExecutionPlan" />s.
///     <para>
///         Execution plans are cached across runs keyed on the pipeline definition and graph structure. Because each run
///         creates fresh node instances and disposes them on completion, a plan that captured its node instance would make
///         every run after the first execute the first run's already-disposed nodes.
///     </para>
/// </summary>
public sealed class ExecutionPlanCacheReuseTests
{
    [Fact]
    public async Task RunningTheSameDefinitionTwice_UsesEachRunsOwnNodeInstances()
    {
        using var recorder = new InstanceRecorder();

        var runner = PipelineRunner.Create();
        await runner.RunAsync<RecordingPipeline>(CancellationToken.None);
        await runner.RunAsync<RecordingPipeline>(CancellationToken.None);

        _ = recorder.SourcesConstructed.Should().Be(2, "each run instantiates its own nodes");
        _ = recorder.SourceInstancesExecuted.Should().Equal([1, 2], "each run must execute the source it constructed");
        _ = recorder.SinkInstancesExecuted.Should().Equal([1, 2], "each run must execute the sink it constructed");
    }

    [Fact]
    public async Task RunningTheSameDefinitionTwice_NeverExecutesADisposedNode()
    {
        using var recorder = new InstanceRecorder();

        var runner = PipelineRunner.Create();
        await runner.RunAsync<RecordingPipeline>(CancellationToken.None);
        await runner.RunAsync<RecordingPipeline>(CancellationToken.None);

        _ = recorder.UseAfterDisposeCount.Should().Be(0, "a cached plan must not retain nodes disposed by an earlier run");
    }

    [Fact]
    public async Task RunningTheSameDefinitionTwice_ProducesTheSameOutputBothTimes()
    {
        using var recorder = new InstanceRecorder();

        var runner = PipelineRunner.Create();
        await runner.RunAsync<RecordingPipeline>(CancellationToken.None);
        var firstRun = recorder.TakeConsumedItems();

        await runner.RunAsync<RecordingPipeline>(CancellationToken.None);
        var secondRun = recorder.TakeConsumedItems();

        _ = firstRun.Should().Equal([2, 4, 6]);
        _ = secondRun.Should().Equal([2, 4, 6], "plan reuse must not change observable behaviour");
    }

    /// <summary>
    ///     Ambient recorder shared by the nodes, which the pipeline framework instantiates itself.
    /// </summary>
    private sealed class InstanceRecorder : IDisposable
    {
        private static readonly object Gate = new();
        private readonly List<int> _consumed = [];

        public InstanceRecorder()
        {
            lock (Gate)
            {
                Current = this;
            }
        }

        public static InstanceRecorder? Current { get; private set; }

        public int SourcesConstructed { get; private set; }

        public int SinksConstructed { get; private set; }

        public List<int> SourceInstancesExecuted { get; } = [];

        public List<int> SinkInstancesExecuted { get; } = [];

        public int UseAfterDisposeCount { get; private set; }

        public int NextSourceId()
        {
            return ++SourcesConstructed;
        }

        public int NextSinkId()
        {
            return ++SinksConstructed;
        }

        public void RecordSourceExecuted(int instanceId)
        {
            SourceInstancesExecuted.Add(instanceId);
        }

        public void RecordSinkExecuted(int instanceId)
        {
            SinkInstancesExecuted.Add(instanceId);
        }

        public void RecordUseAfterDispose()
        {
            UseAfterDisposeCount++;
        }

        public void RecordConsumed(int value)
        {
            _consumed.Add(value);
        }

        public List<int> TakeConsumedItems()
        {
            var snapshot = new List<int>(_consumed);
            _consumed.Clear();
            return snapshot;
        }

        public void Dispose()
        {
            lock (Gate)
            {
                Current = null;
            }
        }
    }

    private sealed class RecordingSource : SourceNode<int>
    {
        private readonly int _instanceId = InstanceRecorder.Current!.NextSourceId();
        private bool _disposed;

        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            var recorder = InstanceRecorder.Current!;

            if (_disposed)
                recorder.RecordUseAfterDispose();

            recorder.RecordSourceExecuted(_instanceId);
            return new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1, 2, 3], "numbers");
        }

        public override ValueTask DisposeAsync()
        {
            _disposed = true;
            return base.DisposeAsync();
        }
    }

    private sealed class DoublingTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult<int>(item * 2);
        }
    }

    private sealed class RecordingSink : SinkNode<int>
    {
        private readonly int _instanceId = InstanceRecorder.Current!.NextSinkId();
        private bool _disposed;

        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            var recorder = InstanceRecorder.Current!;

            if (_disposed)
                recorder.RecordUseAfterDispose();

            recorder.RecordSinkExecuted(_instanceId);

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                recorder.RecordConsumed(item);
            }
        }

        public override ValueTask DisposeAsync()
        {
            _disposed = true;
            return base.DisposeAsync();
        }
    }

    private sealed class RecordingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<RecordingSource, int>("source");
            var transform = builder.AddTransform<DoublingTransform, int, int>("double");
            var sink = builder.AddSink<RecordingSink, int>("sink");

            _ = builder.Connect(source, transform);
            _ = builder.Connect(transform, sink);
        }
    }
}
