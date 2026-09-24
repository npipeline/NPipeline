using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Graph.Validation;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using Xunit.Abstractions;

namespace NPipeline.Tests.Core.Builder;

public sealed class PipelineBuilderTests(ITestOutputHelper output)
{
    [Fact]
    public void AddNode_ShouldAddNodeToBuilderAndReturnCorrectHandle()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder().WithoutExtendedValidation();

        // Act
        var sourceHandle = builder.AddSource<TestSourceNode, string>("source");
        var pipeline = builder.Build();

        // Assert
        sourceHandle.Should().Be(new SourceNodeHandle<string>("source"));
        pipeline.Graph.Nodes.Should().ContainSingle();
        pipeline.Graph.Nodes[0].Id.Should().Be("source");
        pipeline.Graph.Nodes[0].NodeType.Should().Be<TestSourceNode>();
    }

    [Fact]
    public void AddNode_WhenNodeNameIsDuplicated_ShouldFailValidationAtBuildTime()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder().WithoutExtendedValidation().WithoutEarlyNameValidation();
        builder.AddSource<TestSourceNode, string>("My Source");
        builder.AddTransform<TestTransformNode, string, int>("My Source");

        // Act
        Action act = () => builder.Build();

        // Assert
        act.Should().Throw<PipelineValidationException>()
            .WithMessage("*Node names must be unique*")
            .And.Result.Errors.Should().Contain(e =>
                e.Contains("Node names must be unique") &&
                e.Contains("My Source"));
    }

    [Fact]
    public void Connect_WhenNodesAreCompatible_ShouldAddEdge()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<TestSourceNode, string>("source");
        var transform = builder.AddTransform<TestTransformNode, string, int>("transform");

        // Act
        builder.Connect(source, transform);
        var pipeline = builder.Build();

        // Assert
        pipeline.Graph.Edges.Should().ContainSingle();
        pipeline.Graph.Edges[0].SourceNodeId.Should().Be("source");
        pipeline.Graph.Edges[0].TargetNodeId.Should().Be("transform");
    }

    [Fact]
    public void Build_WhenNoNodesAreAdded_ShouldThrowInvalidOperationException()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder().WithoutExtendedValidation();

        // Act
        Action act = () => builder.Build();

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*NP0101*");
    }

    [Fact]
    public void Build_ShouldReturnPipelineWithCorrectGraph()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<TestSourceNode, string>("source");
        var transform = builder.AddTransform<TestTransformNode, string, int>("transform");
        var sink = builder.AddSink<TestSinkNode, int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Act
        var pipeline = builder.Build();

        // Assert
        pipeline.Graph.Nodes.Should().HaveCount(3);
        pipeline.Graph.Edges.Should().HaveCount(2);
    }

    [Fact]
    public void TryBuild_WhenGraphValid_ShouldReturnTrueAndPipeline()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<TestSourceNode, string>("source");
        var transform = builder.AddTransform<TestTransformNode, string, int>("transform");
        builder.Connect(source, transform);

        // Act
        var ok = builder.TryBuild(out var pipeline, out var result);

        // Assert
        ok.Should().BeTrue();
        pipeline.Should().NotBeNull();
        result.IsValid.Should().BeTrue();
    }

    [Fact]
    public void TryBuild_WhenGraphInvalid_ShouldReturnFalseAndErrors()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder().WithoutExtendedValidation();

        // Add transformation with no source inbound and connect nothing to trigger validation error.
        builder.AddTransform<TestTransformNode, string, int>("transformOnly");

        // Act
        var ok = builder.TryBuild(out var pipeline, out var result);

        // Assert
        ok.Should().BeFalse();
        pipeline.Should().BeNull();
        result.IsValid.Should().BeFalse();
        result.Errors.Should().NotBeEmpty();
    }

    [Fact]
    public void WithExecutionStrategy_ShouldSetStrategyOnNode()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<TestSourceNode, string>("source");
        var transform = builder.AddTransform<TestTransformNode, string, int>("transform");
        builder.Connect(source, transform);
        var strategy = new SequentialExecutionStrategy();

        // Act
        builder.WithExecutionStrategy(transform, strategy);
        var pipeline = builder.Build();

        // Assert
        var nodeDef = pipeline.Graph.Nodes.Single(n => n.Id == "transform");
        nodeDef.ExecutionStrategy.Should().Be(strategy);
    }

    [Fact]
    public void NodeRestartOptions_WrapTheTransformForRestart()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<TestSourceNode, string>("source");
        var transform = builder.AddTransform<TestTransformNode, string, int>("transform");
        builder.Connect(source, transform);

        // Act
        builder.WithResilience(transform, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 2 } });
        var pipeline = builder.Build();

        // Assert
        var nodeDef = pipeline.Graph.Nodes.Single(n => n.Id == "transform");

        nodeDef.ExecutionStrategy.Should().BeOfType<ResilientExecutionStrategy>()
            .Which.InnerStrategy.Should().BeNull("the node had no strategy of its own, so its default is resolved when it runs");
    }

    [Fact]
    public void NodeRestartOptions_KeepTheConfiguredStrategyAsTheInnerOne()
    {
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<TestSourceNode, string>("source");
        var strategy = new SequentialExecutionStrategy();
        var transform = builder.AddTransform<TestTransformNode, string, int>("transform").WithExecutionStrategy(builder, strategy);
        builder.Connect(source, transform);

        builder.WithResilience(transform, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 1 } });
        var pipeline = builder.Build();

        pipeline.Graph.Nodes.Single(n => n.Id == "transform").ExecutionStrategy.Should().BeOfType<ResilientExecutionStrategy>()
            .Which.InnerStrategy.Should().BeSameAs(strategy);
    }

    [Fact]
    public void WithoutRestarts_TheTransformIsNotWrapped()
    {
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<TestSourceNode, string>("source");
        var transform = builder.AddTransform<TestTransformNode, string, int>("transform");
        builder.Connect(source, transform);

        var pipeline = builder.Build();

        pipeline.Graph.Nodes.Single(n => n.Id == "transform").ExecutionStrategy.Should().BeNull();
    }

    [Fact]
    public void SetNodeResiliencePolicy_ShouldSetPolicyAnnotationOnNode()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<TestSourceNode, string>("source");
        var transform = builder.AddTransform<TestTransformNode, string, int>("transform");
        builder.Connect(source, transform);

        // Act
        builder.AddResiliencePolicy(transform, new TestResiliencePolicy());
        var pipeline = builder.Build();

        // Assert
        var key = ExecutionAnnotationKeys.NodeResiliencePolicyForNode("transform");
        pipeline.Graph.ExecutionOptions.NodeExecutionAnnotations.Should().ContainKey(key);
    }

    // Test Node Implementations
    private sealed class TestSourceNode : SourceNode<string>
    {
        public override IDataStream<string> OpenStream(PipelineContext context, CancellationToken cancellationToken) => throw new NotImplementedException();
    }

    private sealed class TestTransformNode : TransformNode<string, int>
    {
        public override ValueTask<int> TransformAsync(string item, PipelineContext context, CancellationToken cancellationToken) =>
            throw new NotImplementedException();
    }

    private sealed class TestSinkNode : SinkNode<int>
    {
        public override Task ConsumeAsync(IDataStream<int> input, PipelineContext context,
            CancellationToken cancellationToken) =>
            throw new NotImplementedException();
    }

    private sealed class TestResiliencePolicy : IResiliencePolicy
    {
        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);
    }

    private sealed class AutoSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1]);
    }

    private sealed class AutoTransformNode : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(item);
    }

    private sealed class AutoSinkNode : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }
}
