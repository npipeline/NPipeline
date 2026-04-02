using System.Collections.Concurrent;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability.Metrics;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

/// <summary>
///     Integration tests verifying nested composite pipeline observability and lineage behavior.
/// </summary>
public class NestedObservabilityAndLineageTests
{
    [Fact]
    public async Task ChildNodeLifecycleEvents_ShouldBeEmitted_WhenObserverIsInherited()
    {
        // Arrange
        var observer = new RecordingExecutionObserver();
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.ExecutionObserver = observer;

        // Act
        await runner.RunAsync<ObservableParentPipeline>(context);

        // Assert — parent pipeline nodes produce events
        observer.StartedNodeIds.Should().Contain("source");
        observer.CompletedNodeIds.Should().Contain("source");
        observer.StartedNodeIds.Should().Contain("composite");
        observer.CompletedNodeIds.Should().Contain("composite");
        observer.StartedNodeIds.Should().Contain("sink");
        observer.CompletedNodeIds.Should().Contain("sink");
    }

    [Fact]
    public async Task ChildNodeMetrics_ShouldBePersisted_WhenObservabilityCollectorIsUsed()
    {
        // Arrange — NodeMetrics directly supports PipelineName
        var parentMetric = new NodeMetrics(
            "parent-source",
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow.AddSeconds(1),
            1000,
            true,
            10,
            10,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            PipelineName: "ParentPipeline");

        var childMetric = new NodeMetrics(
            "child-transform",
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow.AddSeconds(1),
            500,
            true,
            3,
            3,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            PipelineName: "ChildSubPipeline");

        // Assert
        parentMetric.PipelineName.Should().Be("ParentPipeline");
        parentMetric.NodeId.Should().Be("parent-source");

        childMetric.PipelineName.Should().Be("ChildSubPipeline");
        childMetric.NodeId.Should().Be("child-transform");
        childMetric.ItemsProcessed.Should().Be(3);
    }

    [Fact]
    public async Task ChildLineageRecords_ShouldBeQueryableByChildNodeIdentity()
    {
        // Arrange
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test-data", "source");

        // Act — record hops with pipeline name context
        collector.RecordHop(packet.LineageId, new LineageHop(
            "source", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false));

        collector.RecordHop(packet.LineageId, new LineageHop(
            "transform", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false,
            PipelineName: "ChildSubPipeline"));

        collector.RecordHop(packet.LineageId, new LineageHop(
            "output", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false,
            PipelineName: "ChildSubPipeline"));

        // Assert
        var lineageInfo = collector.GetLineageInfo(packet.LineageId);
        lineageInfo.Should().NotBeNull();

        // Traversal path should include pipeline-qualified segments for child nodes
        lineageInfo!.TraversalPath.Should().Contain("source");
        lineageInfo.TraversalPath.Should().Contain("ChildSubPipeline::transform");
        lineageInfo.TraversalPath.Should().Contain("ChildSubPipeline::output");

        // Hops should carry pipeline name
        lineageInfo.LineageHops.Should().HaveCount(3);
        lineageInfo.LineageHops[0].PipelineName.Should().BeNull();
        lineageInfo.LineageHops[1].PipelineName.Should().Be("ChildSubPipeline");
        lineageInfo.LineageHops[2].PipelineName.Should().Be("ChildSubPipeline");
    }

    [Fact]
    public async Task ParentAndChildLifecycleEvents_ShouldNotConflict()
    {
        // Arrange
        var observer = new RecordingExecutionObserver();
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.ExecutionObserver = observer;

        // Act
        await runner.RunAsync<ObservableParentPipeline>(context);

        // Assert — all parent node events should have both start and complete
        foreach (var nodeId in observer.StartedNodeIds)
        {
            observer.CompletedNodeIds.Should().Contain(nodeId,
                $"Node '{nodeId}' was started but never completed");
        }

        // No duplicate started events for the same node
        var parentNodeIds = new[] { "source", "composite", "sink" };

        foreach (var nodeId in parentNodeIds)
        {
            observer.StartedNodeIds.Count(id => id == nodeId).Should().Be(1,
                $"Node '{nodeId}' should have exactly one start event");
        }
    }

    [Fact]
    public void CompositeContextConfiguration_InheritAll_ShouldIncludeNewFlags()
    {
        // Arrange & Act
        var config = CompositeContextConfiguration.InheritAll;

        // Assert
        config.InheritRunIdentity.Should().BeTrue();
        config.InheritLineageSink.Should().BeTrue();
        config.InheritExecutionObserver.Should().BeTrue();
        config.InheritDeadLetterDecorator.Should().BeTrue();
    }

    [Fact]
    public void CompositeContextConfiguration_Default_ShouldHaveNewFlagsFalse()
    {
        // Arrange & Act
        var config = CompositeContextConfiguration.Default;

        // Assert
        config.InheritRunIdentity.Should().BeFalse();
        config.InheritLineageSink.Should().BeFalse();
        config.InheritExecutionObserver.Should().BeFalse();
        config.InheritDeadLetterDecorator.Should().BeFalse();
    }

    [Fact]
    public async Task InheritExecutionObserver_WhenTrue_ShouldPassObserverToChildContext()
    {
        // Arrange
        var observer = new RecordingExecutionObserver();
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.ExecutionObserver = observer;

        // Act
        await runner.RunAsync<ObservableInheritingParentPipeline>(context);

        // Assert — parent nodes should be observed
        observer.StartedNodeIds.Should().Contain("source");
        observer.StartedNodeIds.Should().Contain("composite");
        observer.StartedNodeIds.Should().Contain("sink");
    }

    [Fact]
    public async Task InheritRunIdentity_WhenTrue_ShouldPropagateParentRunIdToChildPipeline()
    {
        // Arrange
        ParentRunIdCaptureTransform.CapturedRunId = Guid.Empty;
        RunIdCaptureTransform.CapturedRunId = Guid.Empty;
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();

        // Act
        await runner.RunAsync<RunIdInheritingParentPipeline>(context);

        // Assert
        ParentRunIdCaptureTransform.CapturedRunId.Should().NotBe(Guid.Empty);
        RunIdCaptureTransform.CapturedRunId.Should().Be(ParentRunIdCaptureTransform.CapturedRunId);
    }

    [Fact]
    public async Task InheritRunIdentity_WhenFalse_ShouldUseIndependentChildRunId()
    {
        // Arrange
        ParentRunIdCaptureTransform.CapturedRunId = Guid.Empty;
        RunIdCaptureTransform.CapturedRunId = Guid.Empty;
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();

        // Act
        await runner.RunAsync<RunIdIsolatedParentPipeline>(context);

        // Assert
        ParentRunIdCaptureTransform.CapturedRunId.Should().NotBe(Guid.Empty);
        RunIdCaptureTransform.CapturedRunId.Should().NotBe(Guid.Empty);
        RunIdCaptureTransform.CapturedRunId.Should().NotBe(ParentRunIdCaptureTransform.CapturedRunId);
    }

    [Fact]
    public async Task SetPreconfiguredNodeInstance_ShouldReplaceExistingInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var source = builder.AddSource<TestSource, int>("source");
        var transform = builder.AddTransform<DoubleTransform, int, int>("transform");
        var sink = builder.AddSink<TestSink, int>("sink");
        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Add initial instance
        var initialTransform = new DoubleTransform();
        builder.AddPreconfiguredNodeInstance("transform", initialTransform);

        // Act — replace with a new instance
        var replacementTransform = new DoubleTransform();
        builder.SetPreconfiguredNodeInstance("transform", replacementTransform, replaceExisting: true);

        // Assert — build should succeed with the replacement
        var buildResult = builder.TryBuild(out var pipeline, out var errors);
        buildResult.Should().BeTrue();
        pipeline.Should().NotBeNull();
        pipeline!.Graph.PreconfiguredNodeInstances["transform"].Should().BeSameAs(replacementTransform);
    }

    [Fact]
    public void SetPreconfiguredNodeInstance_WithoutReplace_ShouldThrowOnDifferentInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var source = builder.AddSource<TestSource, int>("source");
        var transform = builder.AddTransform<DoubleTransform, int, int>("transform");
        var sink = builder.AddSink<TestSink, int>("sink");
        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        var initialTransform = new DoubleTransform();
        builder.AddPreconfiguredNodeInstance("transform", initialTransform);

        // Act & Assert
        var replacementTransform = new DoubleTransform();
        var act = () => builder.SetPreconfiguredNodeInstance("transform", replacementTransform, replaceExisting: false);
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void NodeMetrics_ShouldSupportPipelineNameProperty()
    {
        // Arrange & Act
        var metrics = new NodeMetrics(
            "test-node",
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow,
            100,
            true,
            50,
            50,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            PipelineName: "TestPipeline");

        // Assert
        metrics.PipelineName.Should().Be("TestPipeline");
        metrics.NodeId.Should().Be("test-node");
    }

    [Fact]
    public void LineageHop_ShouldSupportPipelineNameProperty()
    {
        // Arrange & Act
        var hop = new LineageHop(
            "test-node",
            HopDecisionFlags.Emitted,
            ObservedCardinality.One,
            1, 1, null, false,
            PipelineName: "TestPipeline");

        // Assert
        hop.PipelineName.Should().Be("TestPipeline");
        hop.NodeId.Should().Be("test-node");
    }

    // ——————————————————————————————————————
    // Test infrastructure
    // ——————————————————————————————————————

    private sealed class RecordingExecutionObserver : IExecutionObserver
    {
        private readonly ConcurrentBag<string> _startedNodeIds = [];
        private readonly ConcurrentBag<string> _completedNodeIds = [];

        public IReadOnlyList<string> StartedNodeIds => [.. _startedNodeIds];
        public IReadOnlyList<string> CompletedNodeIds => [.. _completedNodeIds];

        public void OnNodeStarted(NodeExecutionStarted e) => _startedNodeIds.Add(e.NodeId);
        public void OnNodeCompleted(NodeExecutionCompleted e) => _completedNodeIds.Add(e.NodeId);
        public void OnRetry(NodeRetryEvent e) { }
        public void OnDrop(QueueDropEvent e) { }
        public void OnQueueMetrics(QueueMetricsEvent e) { }
    }

    private sealed class TestSource : ISourceNode<int>
    {
        public IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
            => new InMemoryDataStream<int>([1, 2, 3], "TestSource");

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DoubleTransform : TransformNode<int, int>
    {
        public override Task<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken)
            => Task.FromResult(input * 2);
    }

    private sealed class RunIdCaptureTransform : TransformNode<int, int>
    {
        public static Guid CapturedRunId { get; set; }

        public override Task<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            CapturedRunId = context.RunId;
            return Task.FromResult(input);
        }
    }

    private sealed class ParentRunIdCaptureTransform : TransformNode<int, int>
    {
        public static Guid CapturedRunId { get; set; }

        public override Task<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            CapturedRunId = context.RunId;
            return Task.FromResult(input);
        }
    }

    private sealed class TestSink : ISinkNode<int>
    {
        public static readonly List<int> ReceivedItems = [];

        public async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            ReceivedItems.Clear();

            await foreach (var item in input.WithCancellation(cancellationToken))
                ReceivedItems.Add(item);
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class SimpleChildPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var input = builder.AddCompositeInput<int>("input");
            var transform = builder.AddTransform<DoubleTransform, int, int>("double");
            var output = builder.AddCompositeOutput<int>("output");

            builder.Connect(input, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class ObservableParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSource, int>("source");

            var composite = builder.AddComposite<int, int, SimpleChildPipeline>(
                "composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class ObservableInheritingParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSource, int>("source");

            var composite = builder.AddComposite<int, int, SimpleChildPipeline>(
                "composite",
                new CompositeContextConfiguration
                {
                    InheritExecutionObserver = true,
                    InheritLineageSink = true,
                });

            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class RunIdCaptureChildPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var input = builder.AddCompositeInput<int>("input");
            var capture = builder.AddTransform<RunIdCaptureTransform, int, int>("capture-runid");
            var output = builder.AddCompositeOutput<int>("output");

            builder.Connect(input, capture);
            builder.Connect(capture, output);
        }
    }

    private sealed class RunIdInheritingParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSource, int>("source");
            var captureParent = builder.AddTransform<ParentRunIdCaptureTransform, int, int>("capture-parent-runid");
            var composite = builder.AddComposite<int, int, RunIdCaptureChildPipeline>(
                "composite",
                new CompositeContextConfiguration
                {
                    InheritRunIdentity = true,
                });
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, captureParent);
            builder.Connect(captureParent, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class RunIdIsolatedParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSource, int>("source");
            var captureParent = builder.AddTransform<ParentRunIdCaptureTransform, int, int>("capture-parent-runid");
            var composite = builder.AddComposite<int, int, RunIdCaptureChildPipeline>(
                "composite",
                new CompositeContextConfiguration
                {
                    InheritRunIdentity = false,
                });
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, captureParent);
            builder.Connect(captureParent, composite);
            builder.Connect(composite, sink);
        }
    }
}
