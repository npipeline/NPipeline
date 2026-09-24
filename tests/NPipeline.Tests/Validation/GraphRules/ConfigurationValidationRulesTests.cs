using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Extensions.Parallelism;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Graph.Validation;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using ParallelOptions = NPipeline.Extensions.Parallelism.ParallelOptions;
using BoundedQueuePolicy = NPipeline.Extensions.Parallelism.BoundedQueuePolicy;

namespace NPipeline.Tests.Validation.GraphRules;

public sealed class ResilienceConfigurationRuleTests
{
    [Fact]
    public void NodeRestart_OnAStrategyThatCannotResume_FailsTheBuild()
    {
        var builder = new PipelineBuilder();
        var transform = Wire(builder);

        // D-5: restarting from the beginning instead of the checkpoint would deliver items twice, so there is no fallback.
        builder.WithExecutionStrategy(transform, new NonResumableStrategy());
        builder.WithResilience(transform, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3 } });

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeFalse();

        result.Issues.Should().ContainSingle(i => i.Severity == ValidationSeverity.Error && i.Category == "Resilience")
            .Which.Message.Should().Contain(ErrorCodes.NodeRestartRequiresResumableStrategy).And.Contain(nameof(NonResumableStrategy));
    }

    [Fact]
    public void NodeRestart_FromThePipelineOptions_OnAStrategyThatCannotResume_FailsTheBuild()
    {
        var builder = new PipelineBuilder();
        var transform = Wire(builder);

        builder.WithExecutionStrategy(transform, new NonResumableStrategy());
        builder.WithResilience(o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 1 } });

        builder.TryBuild(out _, out var result).Should().BeFalse();
        result.Issues.Should().Contain(i => i.Message.Contains(ErrorCodes.NodeRestartRequiresResumableStrategy));
    }

    [Fact]
    public void NodeRestart_OnAResumableStrategy_Builds()
    {
        var builder = new PipelineBuilder();
        var transform = Wire(builder);

        builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy(2));
        builder.WithResilience(transform, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3 } });

        var ok = builder.TryBuild(out var pipeline, out var result);

        ok.Should().BeTrue();
        result.Issues.Should().NotContain(i => i.Category == "Resilience");
        pipeline!.Graph.Nodes.Single(n => n.Id == "transform").ExecutionStrategy.Should().BeOfType<ResilientExecutionStrategy>();
    }

    [Fact]
    public void AStrategyThatCannotResume_IsFineWithoutRestarts()
    {
        var builder = new PipelineBuilder();
        var transform = Wire(builder);

        builder.WithExecutionStrategy(transform, new NonResumableStrategy());

        builder.TryBuild(out _, out var result).Should().BeTrue();
        result.Issues.Should().NotContain(i => i.Category == "Resilience");
    }

    [Fact]
    public void ResilientNode_WithAZeroReplayWindow_FailsTheBuild()
    {
        var builder = new PipelineBuilder();
        var transform = Wire(builder);

        builder.WithResilience(transform, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3, MaxReplayWindow = 0 } });

        var act = () => builder.TryBuild(out _, out _);

        act.Should().Throw<InvalidOperationException>().WithMessage("*node 'transform'*");
    }

    [Theory]
    [InlineData(PipelineOptimizationProfile.Default)]
    [InlineData(PipelineOptimizationProfile.HighThroughput)]
    public void CircuitBreaker_ShouldNotWarn_EvenWithNothingToRetry(PipelineOptimizationProfile profile)
    {
        // Breakers outlive a run, so one on a node that retries nothing still makes the next run fail fast.
        var builder = new PipelineBuilder().WithOptimizationProfile(profile);
        _ = Wire(builder);

        builder.WithResilience(o => o with { CircuitBreaker = CircuitBreakerOptions.Default });

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();
        result.Issues.Should().NotContain(i => i.Category == "Resilience");
    }

    [Fact]
    public void CircuitBreaker_OnANonTransformNode_ShouldFailTheBuild()
    {
        var builder = new PipelineBuilder();
        var transform = Wire(builder);
        _ = transform;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.CreateDefault(), "other-source", [1]);
        var sink = builder.AddInMemorySink<int>("other-sink");
        builder.Connect(source, sink);
        builder.WithResilience(source, o => o with { CircuitBreaker = CircuitBreakerOptions.Default });

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeFalse();
        result.Issues.Should().Contain(i => i.Severity == ValidationSeverity.Error && i.Message.Contains("CircuitBreaker"));
    }

    private static TransformNodeHandle<int, int> Wire(PipelineBuilder builder)
    {
        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.CreateDefault(), "source", [1]);
        var transform = builder.AddTransform<ResilientTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
        return transform;
    }

    [Fact]
    public void NonResilientNode_ShouldNotTriggerRule()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.CreateDefault(), "source", [1]);
        var transform = builder.AddTransform<RegularTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // No resilience configuration - rule should not apply
        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();
        result.Issues.Should().NotContain(i => i.Category == "Resilience");
    }

    private sealed class ResilientTransform : ITransformNode<int, int>
    {
        public ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(item);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RegularTransform : ITransformNode<int, int>
    {
        public ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(item);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NonResumableStrategy : IExecutionStrategy
    {
        public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(IDataStream<TIn> input,
            ITransformNode<TIn, TOut> node, PipelineContext context, string nodeId, CancellationToken cancellationToken) =>
            SequentialExecutionStrategy.Instance.ExecuteAsync(input, node, context, nodeId, cancellationToken);
    }
}

public sealed class ParallelConfigurationRuleTests
{
    [Fact]
    public void ParallelNode_HighParallelismWithoutQueueLimit_ShouldWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.CreateDefault(), "source", new[] { 1, 2, 3 });
        var transform = builder.AddTransform<ParallelTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // High parallelism without queue limit
        var parallelOptions = new ParallelOptions(
            8);

        builder.SetNodeExecutionOption(transform.Id, parallelOptions);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();

        result.Issues.Should()
            .Contain(i => i.Category == "Parallelism" && i.Message.Contains("no queue limit"));
    }

    [Fact]
    public void ParallelNode_OrderPreservingWithHighParallelism_ShouldWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.CreateDefault(), "source", new[] { 1, 2, 3 });
        var transform = builder.AddTransform<ParallelTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Order-preserving with high parallelism
        var parallelOptions = new ParallelOptions(
            16,
            100);

        builder.SetNodeExecutionOption(transform.Id, parallelOptions);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();

        result.Issues.Should()
            .Contain(i => i.Category == "Parallelism" && i.Message.Contains("preserves ordering"));
    }

    [Fact]
    public void ParallelNode_VeryHighParallelism_ShouldWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.CreateDefault(), "source", new[] { 1, 2, 3 });
        var transform = builder.AddTransform<ParallelTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Very high parallelism (way beyond processor count)
        var parallelOptions = new ParallelOptions(
            Environment.ProcessorCount * 10,
            1000,
            PreserveOrdering: false);

        builder.SetNodeExecutionOption(transform.Id, parallelOptions);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();

        result.Issues.Should()
            .Contain(i => i.Category == "Parallelism" && i.Message.Contains("very high parallelism"));
    }

    [Fact]
    public void ParallelNode_DropPolicyWithoutQueueLimit_ShouldWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.CreateDefault(), "source", new[] { 1, 2, 3 });
        var transform = builder.AddTransform<ParallelTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Drop policy without queue length
        var parallelOptions = new ParallelOptions(
            4,
            null,
            BoundedQueuePolicy.DropOldest,
            PreserveOrdering: false);

        builder.SetNodeExecutionOption(transform.Id, parallelOptions);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();

        result.Issues.Should()
            .Contain(i => i.Category == "Parallelism" && i.Message.Contains("drop queue policy"));
    }

    [Fact]
    public void ParallelNode_ProperConfiguration_ShouldNotWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.CreateDefault(), "source", new[] { 1, 2, 3 });
        var transform = builder.AddTransform<ParallelTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Proper configuration
        var parallelOptions = new ExecutionOptionAnnotation
        {
            MaxDegreeOfParallelism = 4,
            MaxQueueLength = 100,
            QueuePolicy = 0, // Block
            PreserveOrdering = false,
        };

        builder.SetNodeExecutionOption(transform.Id, parallelOptions);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();
        result.Issues.Should().NotContain(i => i.Category == "Parallelism");
    }

    [Fact]
    public void NonParallelNode_ShouldNotTriggerRule()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.CreateDefault(), "source", new[] { 1, 2, 3 });
        var transform = builder.AddTransform<ParallelTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // No parallel options - rule should not apply
        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();
        result.Issues.Should().NotContain(i => i.Category == "Parallelism");
    }

    private sealed class ParallelTransform : ITransformNode<int, int>
    {
        public ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(item);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // Mock parallel options annotation for testing
    private sealed class ExecutionOptionAnnotation
    {
        public int? MaxDegreeOfParallelism { get; set; }
        public int? MaxQueueLength { get; set; }
        public int QueuePolicy { get; set; }
        public bool PreserveOrdering { get; set; }
    }
}
