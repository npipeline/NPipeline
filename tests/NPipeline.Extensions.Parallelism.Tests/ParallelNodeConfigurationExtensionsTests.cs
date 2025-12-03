using AwesomeAssertions;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism.Tests;

/// <summary>
///     Tests for fluent parallelism configuration extension methods on transform node handles.
/// </summary>
public sealed class ParallelNodeConfigurationExtensionsTests
{
    #region Fluent Chaining Tests

    [Fact]
    public void FluentChaining_MultipleParallelConfigurations_Chainable()
    {
        // Arrange
        PipelineBuilder builder = new();

        var handle = builder
            .AddTransform<TestTransformNode, int, string>()
            .WithBlockingParallelism(builder, 4);

        // Act & Assert
        _ = builder.NodeState.ExecutionAnnotations.Should().ContainKey(handle.Id);
    }

    #endregion

    #region DefaultQueueLength Tests

    [Fact]
    public void DefaultQueueLength_ShouldBe100()
    {
        // Arrange & Act
        var defaultQueueLength = ParallelNodeConfigurationExtensions.DefaultQueueLength;

        // Assert
        defaultQueueLength.Should().Be(100);
    }

    #endregion

    #region Setup

    private sealed class TestTransformNode : TransformNode<int, string>
    {
        public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item.ToString());
        }
    }

    #endregion

    #region WithBlockingParallelism Tests

    [Fact]
    public void WithBlockingParallelism_WithValidDegree_SetsOptions()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        handle.WithBlockingParallelism(builder, 4);

        // Assert
        _ = builder.NodeState.ExecutionAnnotations.Should().ContainKey(handle.Id);
    }

    [Fact]
    public void WithBlockingParallelism_ReturnsHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        var result = handle.WithBlockingParallelism(builder, 4);

        // Assert
        _ = result.Should().Be(handle);
    }

    [Fact]
    public void WithBlockingParallelism_WithNullBuilder_ThrowsArgumentNullException()
    {
        // Arrange
        TransformNodeHandle<int, string> handle = new("test");

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithBlockingParallelism(null!, 4));
    }

    [Fact]
    public void WithBlockingParallelism_WithZeroDegree_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act & Assert
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => handle.WithBlockingParallelism(builder, 0));
    }

    [Fact]
    public void WithBlockingParallelism_WithNegativeDegree_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act & Assert
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => handle.WithBlockingParallelism(builder, -1));
    }

    #endregion

    #region WithDropOldestParallelism Tests

    [Fact]
    public void WithDropOldestParallelism_WithValidDegree_SetsOptions()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        handle.WithDropOldestParallelism(builder, 4);

        // Assert
        _ = builder.NodeState.ExecutionAnnotations.Should().ContainKey(handle.Id);
    }

    [Fact]
    public void WithDropOldestParallelism_ReturnsHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        var result = handle.WithDropOldestParallelism(builder, 4);

        // Assert
        _ = result.Should().Be(handle);
    }

    [Fact]
    public void WithDropOldestParallelism_WithNullBuilder_ThrowsArgumentNullException()
    {
        // Arrange
        TransformNodeHandle<int, string> handle = new("test");

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithDropOldestParallelism(null!, 4));
    }

    [Fact]
    public void WithDropOldestParallelism_WithZeroDegree_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act & Assert
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => handle.WithDropOldestParallelism(builder, 0));
    }

    #endregion

    #region WithDropNewestParallelism Tests

    [Fact]
    public void WithDropNewestParallelism_WithValidDegree_SetsOptions()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        handle.WithDropNewestParallelism(builder, 4);

        // Assert
        _ = builder.NodeState.ExecutionAnnotations.Should().ContainKey(handle.Id);
    }

    [Fact]
    public void WithDropNewestParallelism_ReturnsHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        var result = handle.WithDropNewestParallelism(builder, 4);

        // Assert
        _ = result.Should().Be(handle);
    }

    [Fact]
    public void WithDropNewestParallelism_WithNullBuilder_ThrowsArgumentNullException()
    {
        // Arrange
        TransformNodeHandle<int, string> handle = new("test");

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithDropNewestParallelism(null!, 4));
    }

    #endregion

    #region WithParallelism Tests

    [Fact]
    public void WithParallelism_WithCustomOptions_SetsOptions()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();
        ParallelOptions options = new(8, QueuePolicy: BoundedQueuePolicy.Block);
        ParallelExecutionStrategyBase strategy = new BlockingParallelStrategy();

        // Act
        handle.WithParallelism(builder, options, strategy);

        // Assert
        _ = builder.NodeState.ExecutionAnnotations.Should().ContainKey(handle.Id);
    }

    [Fact]
    public void WithParallelism_ReturnsHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();
        ParallelOptions options = new(8);
        ParallelExecutionStrategyBase strategy = new BlockingParallelStrategy();

        // Act
        var result = handle.WithParallelism(builder, options, strategy);

        // Assert
        _ = result.Should().Be(handle);
    }

    [Fact]
    public void WithParallelism_WithNullBuilder_ThrowsArgumentNullException()
    {
        // Arrange
        TransformNodeHandle<int, string> handle = new("test");
        ParallelOptions options = new(8);
        ParallelExecutionStrategyBase strategy = new BlockingParallelStrategy();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithParallelism(null!, options, strategy));
    }

    [Fact]
    public void WithParallelism_WithNullOptions_ThrowsArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();
        ParallelExecutionStrategyBase strategy = new BlockingParallelStrategy();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithParallelism(builder, null!, strategy));
    }

    [Fact]
    public void WithParallelism_WithNullStrategy_ThrowsArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = new();
        var handle = builder.AddTransform<TestTransformNode, int, string>();
        ParallelOptions options = new(8);

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithParallelism(builder, options, null!));
    }

    #endregion
}
