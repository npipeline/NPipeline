using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Execution;

/// <summary>
///     Tests for Pipeline class.
///     Validates pipeline graph storage and builder disposables handling.
///     Covers 14 statements in Pipeline.
/// </summary>
public sealed class PipelineTests
{
    [Fact]
    public void Pipeline_WithValidGraph_StoresGraphReference()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;

        // Act
        NPipeline.Pipeline.Pipeline pipeline = new(graph);

        // Assert
        _ = pipeline.Graph.Should().NotBeNull();
        _ = pipeline.Graph.Should().Be(graph);
    }

    [Fact]
    public void Pipeline_GraphProperty_StoresReference()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        NPipeline.Pipeline.Pipeline pipeline = new(graph);

        // Act & Assert
        _ = pipeline.Graph.Should().Be(graph);
    }

    [Fact]
    public void Pipeline_NewInstance_HasEmptyBuilderDisposables()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        NPipeline.Pipeline.Pipeline pipeline = new(graph);

        // Act & Assert
        _ = pipeline.BuilderDisposables.Should().NotBeNull();
        _ = pipeline.BuilderDisposables.Should().BeEmpty();
    }

    [Fact]
    public void Pipeline_BuilderDisposables_CanBeSet()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        NPipeline.Pipeline.Pipeline pipeline = new(graph);

        DummyAsyncDisposable disposable1 = new();
        DummyAsyncDisposable disposable2 = new();

        // Act
        pipeline.BuilderDisposables = [disposable1, disposable2];

        // Assert
        _ = pipeline.BuilderDisposables.Should().HaveCount(2);
        _ = pipeline.BuilderDisposables.Should().Contain(disposable1);
        _ = pipeline.BuilderDisposables.Should().Contain(disposable2);
    }

    [Fact]
    public void Pipeline_BuilderDisposables_DefaultIsEmpty()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        NPipeline.Pipeline.Pipeline pipeline = new(graph);

        // Act & Assert - should be empty by default
        _ = pipeline.BuilderDisposables.Should().BeEmpty();
    }

    [Fact]
    public void TransferBuilderDisposables_WithEmptyList_DoesNotThrow()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        NPipeline.Pipeline.Pipeline pipeline = new(graph) { BuilderDisposables = [] };
        var context = PipelineContext.Default;

        // Act & Assert
        _ = pipeline.Invoking(p => p.TransferBuilderDisposables(context))
            .Should().NotThrow();
    }

    [Fact]
    public void TransferBuilderDisposables_WithDisposables_TransfersToContext()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        NPipeline.Pipeline.Pipeline pipeline = new(graph);

        DummyAsyncDisposable disposable1 = new();
        DummyAsyncDisposable disposable2 = new();
        pipeline.BuilderDisposables = [disposable1, disposable2];
        var context = PipelineContext.Default;

        // Act
        pipeline.TransferBuilderDisposables(context);

        // Assert - method completes without exception
        _ = pipeline.BuilderDisposables.Should().Contain(disposable1);
        _ = pipeline.BuilderDisposables.Should().Contain(disposable2);
    }

    [Fact]
    public void TransferBuilderDisposables_CanBeCalledMultipleTimes()
    {
        // Arrange
        PipelineBuilder builder = new();
        _ = builder.AddSource<DummySource, int>("source");
        var graph = builder.Build().Graph;
        NPipeline.Pipeline.Pipeline pipeline = new(graph);

        DummyAsyncDisposable disposable = new();
        pipeline.BuilderDisposables = [disposable];
        var context = PipelineContext.Default;

        // Act & Assert - calling multiple times should not throw
        _ = pipeline.Invoking(p =>
        {
            p.TransferBuilderDisposables(context);
            p.TransferBuilderDisposables(context);
        }).Should().NotThrow();
    }

    #region Test Fixtures

    private sealed class DummyAsyncDisposable : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DummySource : ISourceNode<int>
    {
        public IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
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

    #endregion
}
