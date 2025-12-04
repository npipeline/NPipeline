using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Graph.Validation;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit.Abstractions;

namespace NPipeline.Tests.Core.Builder;

public sealed class PipelineBuilderTests(ITestOutputHelper output)
{
    [Fact]
    public void AddNode_ShouldAddNodeToBuilderAndReturnCorrectHandle()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder();

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
        var builder = new PipelineBuilder();
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
        var builder = new PipelineBuilder();
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
        var builder = new PipelineBuilder();

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
        var builder = new PipelineBuilder();
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
        var builder = new PipelineBuilder();
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
        var builder = new PipelineBuilder();

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
        var builder = new PipelineBuilder();
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
    public void WithResilience_ShouldWrapStrategyInResilientStrategy()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder();
        var source = builder.AddSource<TestSourceNode, string>("source");
        var transform = builder.AddTransform<TestTransformNode, string, int>("transform");
        builder.Connect(source, transform);

        // Act
        builder.WithResilience(transform);
        var pipeline = builder.Build();

        // Assert
        var nodeDef = pipeline.Graph.Nodes.Single(n => n.Id == "transform");
        nodeDef.ExecutionStrategy.Should().BeOfType<ResilientExecutionStrategy>();
    }

    [Fact]
    public void WithErrorHandler_ShouldSetErrorHandlerTypeOnNode()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var builder = new PipelineBuilder();
        var source = builder.AddSource<TestSourceNode, string>("source");
        var transform = builder.AddTransform<TestTransformNode, string, int>("transform");
        builder.Connect(source, transform);

        // Act
        transform.WithErrorHandler<string, int, TestErrorHandler>(builder);
        var pipeline = builder.Build();

        // Assert
        var nodeDef = pipeline.Graph.Nodes.Single(n => n.Id == "transform");
        nodeDef.ErrorHandlerType.Should().Be<TestErrorHandler>();
    }


    // Test Node Implementations
    private sealed class TestSourceNode : SourceNode<string>
    {
        public override IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }

    private sealed class TestTransformNode : TransformNode<string, int>
    {
        public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }

    private sealed class TestSinkNode : SinkNode<int>
    {
        public override Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }

    private sealed class TestErrorHandler : INodeErrorHandler<ITransformNode<string, int>, string>
    {
        public Task<NodeErrorDecision> HandleAsync(ITransformNode<string, int> node, string failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Fail);
        }
    }

    private sealed class AutoSourceNode : SourceNode<int>
    {
        public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>([1]);
        }
    }

    private sealed class AutoTransformNode : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }

    private sealed class AutoSinkNode : SinkNode<int>
    {
        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }
}
