using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Core.Builder;

/// <summary>
///     Tests for fluent configuration extension methods on node handles.
/// </summary>
public sealed class NodeConfigurationExtensionsTests
{
    #region Setup

    private sealed class TestSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataStream<int>(new[] { 1, 2, 3 });
        }
    }

    private sealed class TestTransformNode : TransformNode<int, string>
    {
        public override ValueTask<string> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult<string>(item.ToString());
    }

    private sealed class TestSinkNode : SinkNode<string>
    {
        public List<string> Items { get; } = new();

        public override async Task ConsumeAsync(
            IDataStream<string> input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                Items.Add(item);
            }
        }
    }

    private sealed class TestErrorHandler : IResiliencePolicy
    {
        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Skip);
    }

    #endregion

    #region Per-node WithResilience Tests

    [Fact]
    public void WithResilienceForNode_RecordsTheNodesConfiguration()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        builder.WithResilience(handle, o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 5 } });

        // Assert
        _ = builder.NodeState.ResilienceOverrides.Should().ContainKey(handle.Id);
        _ = builder.NodeState.ResilienceOverrides[handle.Id](PipelineResilienceOptions.None).ItemRetry.MaxRetries.Should().Be(5);
    }

    [Fact]
    public void WithResilienceForNode_ComposesRepeatedCalls()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        builder.WithResilience(handle, o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 5 } });
        builder.WithResilience(handle, o => o with { OnItemFailure = ItemFailureAction.Skip });

        // Assert
        var options = builder.NodeState.ResilienceOverrides[handle.Id](PipelineResilienceOptions.None);
        _ = options.ItemRetry.MaxRetries.Should().Be(5);
        _ = options.OnItemFailure.Should().Be(ItemFailureAction.Skip);
    }

    [Fact]
    public void WithResilienceForNode_WithUnknownNode_Throws()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act & Assert
        _ = Assert.Throws<InvalidOperationException>(() =>
            builder.WithResilience(new TransformNodeHandle<int, string>("missing"), o => o));
    }

    [Fact]
    public void WithResilienceForNode_WithNullConfigure_Throws()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => builder.WithResilience(handle, null!));
    }

    #endregion

    #region AddResiliencePolicyForNode Tests

    [Fact]
    public void AddResiliencePolicyForNode_WithValidPolicy_ConfiguresAnnotation()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();
        var policy = new TestErrorHandler();

        // Act
        builder.AddResiliencePolicy(handle, policy);

        // Assert
        var key = ExecutionAnnotationKeys.NodeResiliencePolicyForNode(handle.Id);
        _ = builder.NodeState.ExecutionAnnotations.Should().ContainKey(key);
        _ = builder.NodeState.ExecutionAnnotations[key].Should().BeSameAs(policy);
    }

    [Fact]
    public void AddResiliencePolicyForNode_WithNullHandle_ThrowsArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var act = () => builder.AddResiliencePolicy(null!, new TestErrorHandler());

        // Assert
        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddResiliencePolicyForNode_WithNullPolicy_ThrowsArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => builder.AddResiliencePolicy(handle, null!));
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

    #region Fluent Chaining Tests

    [Fact]
    public void FluentChaining_MultipleConfigurations_AllApplied()
    {
        // Arrange
        var builder = new PipelineBuilder();

        var handle = builder.AddTransform<TestTransformNode, int, string>();

        builder
            .WithResilience(handle, o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 3 } })
            .AddResiliencePolicy(handle, new TestErrorHandler());

        // Act & Assert
        _ = builder.NodeState.ResilienceOverrides.Should().ContainKey(handle.Id);
        var key = ExecutionAnnotationKeys.NodeResiliencePolicyForNode(handle.Id);
        _ = builder.NodeState.ExecutionAnnotations.Should().ContainKey(key);
        _ = builder.NodeState.ExecutionAnnotations[key].Should().BeOfType<TestErrorHandler>();
    }

    [Fact]
    public void FluentChaining_AllMethods_ReturnSameHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var originalHandle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        var result = originalHandle
            .WithExecutionStrategy(builder, new SequentialExecutionStrategy());

        // Assert
        _ = result.Should().Be(originalHandle);
    }

    #endregion
}
