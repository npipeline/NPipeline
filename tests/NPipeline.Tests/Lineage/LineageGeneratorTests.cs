using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Lineage;

/// <summary>
///     Tests for LineageGenerator static utility.
///     Validates lineage report generation from pipeline graphs with various node and edge configurations.
///     Covers 11 statements in LineageGenerator.
/// </summary>
public sealed class LineageGeneratorTests
{
    [Fact]
    public void Generate_WithMinimalGraph_CreatesReport()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        var pipelineName = "TestPipeline";
        var runId = Guid.NewGuid();

        // Act
        var report = LineageGenerator.Generate(pipelineName, graph, runId);

        // Assert
        _ = report.Should().NotBeNull();
        _ = report.Pipeline.Should().Be(pipelineName);
        _ = report.RunId.Should().Be(runId);
    }

    [Fact]
    public void Generate_WithSingleSourceNode_ReportsNode()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        var runId = Guid.NewGuid();

        // Act
        var report = LineageGenerator.Generate("Pipeline", graph, runId);

        // Assert
        _ = report.Nodes.Should().NotBeEmpty();
        _ = report.Nodes.Should().ContainSingle();
        _ = report.Nodes[0].Type.Should().Be("DummySource");
    }

    [Fact]
    public void Generate_WithPipelineNameVariations_IncludesInReport()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        string[] pipelineNames = ["Pipeline1", "TestPipeline", "CustomPipeline"];

        // Act & Assert
        foreach (var name in pipelineNames)
        {
            var report = LineageGenerator.Generate(name, graph, Guid.NewGuid());
            _ = report.Pipeline.Should().Be(name);
        }
    }

    [Fact]
    public void Generate_WithDifferentRunIds_IncludesInReport()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        var runId1 = Guid.NewGuid();
        var runId2 = Guid.NewGuid();

        // Act
        var report1 = LineageGenerator.Generate("Pipeline", graph, runId1);
        var report2 = LineageGenerator.Generate("Pipeline", graph, runId2);

        // Assert
        _ = report1.RunId.Should().Be(runId1);
        _ = report2.RunId.Should().Be(runId2);
        _ = report1.RunId.Should().NotBe(report2.RunId);
    }

    [Fact]
    public void Generate_WithSourceNode_IncludesNodeDetails()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;

        // Act
        var report = LineageGenerator.Generate("Pipeline", graph, Guid.NewGuid());

        // Assert
        _ = report.Nodes.Should().HaveCount(1);
        var node = report.Nodes[0];
        _ = node.Type.Should().Be("DummySource");
        _ = node.InputType.Should().BeNull();
        _ = node.OutputType.Should().Contain("Int");
    }

    [Fact]
    public void Generate_WithTransformNode_IncludesInputAndOutputTypes()
    {
        // Arrange
        PipelineBuilder builder = new();
        var source = builder.AddSource<DummySource, int>("source");
        var transform = builder.AddTransform<DummyTransform, int, string>("transform");
        _ = builder.Connect(source, transform);
        var graph = builder.Build().Graph;

        // Act
        var report = LineageGenerator.Generate("Pipeline", graph, Guid.NewGuid());

        // Assert
        _ = report.Nodes.Should().HaveCountGreaterThanOrEqualTo(2);
        var transformNode = report.Nodes.First(n => n.Type == "DummyTransform");
        _ = transformNode.InputType.Should().Contain("Int");
        _ = transformNode.OutputType.Should().Contain("String");
    }

    [Fact]
    public void Generate_NodeIds_PreservedFromGraph()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        var expectedNodeId = graph.Nodes.First().Id;

        // Act
        var report = LineageGenerator.Generate("Pipeline", graph, Guid.NewGuid());

        // Assert
        _ = report.Nodes.Should().HaveCount(1);
        _ = report.Nodes[0].Id.Should().Be(expectedNodeId);
    }

    [Fact]
    public void Generate_WithConnectedNodes_ReportsEdges()
    {
        // Arrange
        PipelineBuilder builder = new();
        var source = builder.AddSource<DummySource, int>("source");
        var transform = builder.AddTransform<DummyTransform, int, string>("transform");
        builder.Connect(source, transform);
        var graph = builder.Build().Graph;

        // Act
        var report = LineageGenerator.Generate("Pipeline", graph, Guid.NewGuid());

        // Assert
        _ = report.Edges.Should().NotBeEmpty();
    }

    [Fact]
    public void Generate_EdgeConnectionsPreserved()
    {
        // Arrange
        PipelineBuilder builder = new();
        var source = builder.AddSource<DummySource, int>("source");
        var transform = builder.AddTransform<DummyTransform, int, string>("transform");
        _ = builder.Connect(source, transform);
        var graph = builder.Build().Graph;

        // Act
        var report = LineageGenerator.Generate("Pipeline", graph, Guid.NewGuid());

        // Assert
        _ = report.Edges.Should().NotBeEmpty();
        var edge = report.Edges[0];
        _ = edge.From.Should().NotBeNullOrEmpty();
        _ = edge.To.Should().NotBeNullOrEmpty();
    }

    #region Test Fixtures

    private sealed class DummySource : ISourceNode<int>
    {
        public IDataPipe<int> ExecuteAsync(
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return new StreamingDataPipe<int>(Stream());

            static async IAsyncEnumerable<int> Stream()
            {
                yield return 1;

                await Task.CompletedTask;
            }
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DummyTransform : ITransformNode<int, string>
    {
        public IExecutionStrategy ExecutionStrategy { get; set; } = new SequentialExecutionStrategy();
        public INodeErrorHandler? ErrorHandler { get; set; }

        public Task<string> ExecuteAsync(
            int item,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(item.ToString());
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    #endregion
}
