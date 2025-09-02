using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Builder;

/// <summary>
///     Tests for fluent configuration extension methods on node handles.
/// </summary>
public sealed class NodeConfigurationExtensionsTests
{
    #region Setup

    private sealed class TestSourceNode : SourceNode<int>
    {
        public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            return new ListDataPipe<int>(new[] { 1, 2, 3 });
        }
    }

    private sealed class TestTransformNode : TransformNode<int, string>
    {
        public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item.ToString());
        }
    }

    private sealed class TestSinkNode : SinkNode<string>
    {
        public List<string> Items { get; } = new();

        public override async Task ExecuteAsync(
            IDataPipe<string> input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                Items.Add(item);
            }
        }
    }

    private sealed class TestErrorHandler : INodeErrorHandler<ITransformNode<int, string>, int>
    {
        public Task<NodeErrorDecision> HandleAsync(
            ITransformNode<int, string> node,
            int item,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Skip);
        }
    }

    #endregion

    #region WithRetries Tests

    [Fact]
    public void WithRetries_OnTransformHandle_ReturnsHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        var result = handle.WithRetries(builder, 3);

        // Assert
        _ = result.Should().Be(handle);
    }

    [Fact]
    public void WithRetries_WithValidParameters_ConfiguresRetryOptions()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        handle.WithRetries(builder, 5, 100);

        // Assert
        _ = builder.NodeState.RetryOverrides.Should().ContainKey(handle.Id);
        var retryOptions = builder.NodeState.RetryOverrides[handle.Id];
        _ = retryOptions.MaxItemRetries.Should().Be(5);
    }

    [Fact]
    public void WithRetries_WithNullBuilder_ThrowsArgumentNullException()
    {
        // Arrange
        var handle = new TransformNodeHandle<int, string>("test");

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithRetries(null!, 3));
    }

    [Fact]
    public void WithRetries_WithNegativeRetries_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act & Assert
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => handle.WithRetries(builder, -1));
    }

    [Fact]
    public void WithRetries_OnSourceHandle_ReturnsHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddSource<TestSourceNode, int>();

        // Act
        var result = handle.WithRetries(builder, 2);

        // Assert
        _ = result.Should().Be(handle);
    }

    [Fact]
    public void WithRetries_OnSinkHandle_ReturnsHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddSink<TestSinkNode, string>();

        // Act
        var result = handle.WithRetries(builder, 2);

        // Assert
        _ = result.Should().Be(handle);
    }

    #endregion

    #region WithErrorHandler Tests

    [Fact]
    public void WithErrorHandler_WithGenericTypeParameter_ConfiguresErrorHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        handle.WithErrorHandler<int, string, TestErrorHandler>(builder);

        // Assert
        _ = builder.NodeState.Nodes.Should().ContainKey(handle.Id);
        var nodeDef = builder.NodeState.Nodes[handle.Id];
        _ = nodeDef.ErrorHandlerType.Should().NotBeNull();
        _ = nodeDef.ErrorHandlerType!.Name.Should().Be(nameof(TestErrorHandler));
    }

    [Fact]
    public void WithErrorHandler_WithErrorHandlerType_ConfiguresErrorHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        handle.WithErrorHandler(builder, typeof(TestErrorHandler));

        // Assert
        _ = builder.NodeState.Nodes.Should().ContainKey(handle.Id);
        var nodeDef = builder.NodeState.Nodes[handle.Id];
        _ = nodeDef.ErrorHandlerType.Should().NotBeNull();
        _ = nodeDef.ErrorHandlerType!.Name.Should().Be(nameof(TestErrorHandler));
    }

    [Fact]
    public void WithErrorHandler_WithNullBuilder_ThrowsArgumentNullException()
    {
        // Arrange
        var handle = new TransformNodeHandle<int, string>("test");

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithErrorHandler<int, string, TestErrorHandler>(null!));
    }

    [Fact]
    public void WithErrorHandler_OnSinkHandle_ReturnsHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddSink<TestSinkNode, string>();

        // Act
        var result = handle.WithErrorHandler<string, TestErrorHandler>(builder);

        // Assert
        _ = result.Should().Be(handle);
    }

    [Fact]
    public void WithErrorHandler_ReturnsHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        var result = handle.WithErrorHandler<int, string, TestErrorHandler>(builder);

        // Assert
        _ = result.Should().Be(handle);
    }

    #endregion

    #region WithExecutionStrategy Tests

    [Fact]
    public void WithExecutionStrategy_WithValidStrategy_ConfiguresStrategy()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();
        var strategy = new SequentialExecutionStrategy();

        // Act
        handle.WithExecutionStrategy(builder, strategy);

        // Assert
        _ = builder.NodeState.Nodes.Should().ContainKey(handle.Id);
        var nodeDef = builder.NodeState.Nodes[handle.Id];
        _ = nodeDef.ExecutionStrategy.Should().Be(strategy);
    }

    [Fact]
    public void WithExecutionStrategy_ReturnsHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();
        var strategy = new SequentialExecutionStrategy();

        // Act
        var result = handle.WithExecutionStrategy(builder, strategy);

        // Assert
        _ = result.Should().Be(handle);
    }

    [Fact]
    public void WithExecutionStrategy_WithNullBuilder_ThrowsArgumentNullException()
    {
        // Arrange
        var handle = new TransformNodeHandle<int, string>("test");
        var strategy = new SequentialExecutionStrategy();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithExecutionStrategy(null!, strategy));
    }

    [Fact]
    public void WithExecutionStrategy_WithNullStrategy_ThrowsArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithExecutionStrategy(builder, null!));
    }

    #endregion

    #region WithResilience Tests

    [Fact]
    public void WithResilience_ConfiguresResilientStrategy()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        handle.WithResilience(builder);

        // Assert
        _ = builder.NodeState.Nodes.Should().ContainKey(handle.Id);
        var nodeDef = builder.NodeState.Nodes[handle.Id];
        _ = nodeDef.ExecutionStrategy.Should().NotBeNull();
    }

    [Fact]
    public void WithResilience_ReturnsHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        var result = handle.WithResilience(builder);

        // Assert
        _ = result.Should().Be(handle);
    }

    [Fact]
    public void WithResilience_WithNullBuilder_ThrowsArgumentNullException()
    {
        // Arrange
        var handle = new TransformNodeHandle<int, string>("test");

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => handle.WithResilience(null!));
    }

    #endregion

    #region Fluent Chaining Tests

    [Fact]
    public void FluentChaining_MultipleConfigurations_AllApplied()
    {
        // Arrange
        var builder = new PipelineBuilder();

        var handle = builder
            .AddTransform<TestTransformNode, int, string>()
            .WithRetries(builder, 3, 50)
            .WithErrorHandler<int, string, TestErrorHandler>(builder);

        // Act & Assert
        _ = builder.NodeState.RetryOverrides.Should().ContainKey(handle.Id);
        _ = builder.NodeState.Nodes[handle.Id].ErrorHandlerType.Should().NotBeNull();
        _ = builder.NodeState.Nodes[handle.Id].ErrorHandlerType!.Name.Should().Be(nameof(TestErrorHandler));
    }

    [Fact]
    public void FluentChaining_AllMethods_ReturnSameHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var originalHandle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        var result = originalHandle
            .WithRetries(builder, 3)
            .WithErrorHandler<int, string, TestErrorHandler>(builder)
            .WithResilience(builder);

        // Assert
        _ = result.Should().Be(originalHandle);
    }

    #endregion
}
