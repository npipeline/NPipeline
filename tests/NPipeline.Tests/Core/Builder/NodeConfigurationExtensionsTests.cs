using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

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
        public override Task<string> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item.ToString());
        }
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
        public Task<ResilienceDecision> DecideNodeFailureAsync(
            NodeDefinition nodeDefinition,
            INode node,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(
            string nodeId,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn item,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Skip);
        }

        public ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
        {
            return context.GetRetryDelayStrategy().GetDelayAsync(attemptNumber, cancellationToken);
        }

        public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
        {
            return DefaultResiliencePolicy.Instance.GetCircuitBreaker(context, nodeId);
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

    [Fact]
    public void WithRetry_UsesBuilderOptimizationProfileDefaults()
    {
        // Arrange
        var builder = new PipelineBuilder()
            .WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        handle.WithRetry(builder);

        // Assert
        _ = builder.NodeState.RetryOverrides.Should().ContainKey(handle.Id);
        var retryOptions = builder.NodeState.RetryOverrides[handle.Id];
        _ = retryOptions.Should().Be(PipelineRetryOptions.ForProfile(PipelineOptimizationProfile.HighThroughput));
    }

    [Fact]
    public void WithRetry_WithExplicitProfile_UsesSpecifiedProfileDefaults()
    {
        // Arrange
        var builder = new PipelineBuilder()
            .WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act
        handle.WithRetry(builder, PipelineOptimizationProfile.Default);

        // Assert
        _ = builder.NodeState.RetryOverrides.Should().ContainKey(handle.Id);
        var retryOptions = builder.NodeState.RetryOverrides[handle.Id];
        _ = retryOptions.MaxItemRetries.Should().Be(3);
        _ = retryOptions.MaxMaterializedItems.Should().Be(10_000);
    }

    #endregion

    #region SetNodeResiliencePolicy Tests

    [Fact]
    public void SetNodeResiliencePolicy_WithValidPolicy_ConfiguresAnnotation()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();
        var policy = new TestErrorHandler();

        // Act
        builder.SetNodeResiliencePolicy(handle, policy);

        // Assert
        var key = ExecutionAnnotationKeys.NodeResiliencePolicyForNode(handle.Id);
        _ = builder.NodeState.ExecutionAnnotations.Should().ContainKey(key);
        _ = builder.NodeState.ExecutionAnnotations[key].Should().BeSameAs(policy);
    }

    [Fact]
    public void SetNodeResiliencePolicy_WithNullHandle_ThrowsArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var act = () => builder.SetNodeResiliencePolicy(null!, new TestErrorHandler());

        // Assert
        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void SetNodeResiliencePolicy_WithNullPolicy_ThrowsArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var handle = builder.AddTransform<TestTransformNode, int, string>();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => builder.SetNodeResiliencePolicy(handle, null!));
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
            .WithRetries(builder, 3, 50);

        builder.SetNodeResiliencePolicy(handle, new TestErrorHandler());

        // Act & Assert
        _ = builder.NodeState.RetryOverrides.Should().ContainKey(handle.Id);
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
            .WithRetries(builder, 3)
            .WithResilience(builder);

        // Assert
        _ = result.Should().Be(originalHandle);
    }

    #endregion
}
