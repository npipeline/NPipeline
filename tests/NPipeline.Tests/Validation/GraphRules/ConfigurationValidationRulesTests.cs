using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
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
    public void ResilientNode_WithNoRestartsAndTheDefaultPolicy_ShouldWarn()
    {
        var builder = new PipelineBuilder();
        var transform = Wire(builder);

        builder.WithResilience(transform);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue("Build should succeed, validation issues are warnings");
        result.Issues.Should().Contain(i => i.Category == "Resilience" && i.Message.Contains("MaxRestarts is 0"));
    }

    [Fact]
    public void ResilientNode_WithNoRestartsButACustomPolicy_ShouldNotWarn()
    {
        var builder = new PipelineBuilder();
        var transform = Wire(builder);

        // The custom policy may restart the node whatever its options say.
        builder.AddResiliencePolicy<DummyResiliencePolicy>();
        builder.WithResilience(transform);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();
        result.Issues.Should().NotContain(i => i.Category == "Resilience");
    }

    [Fact]
    public void ResilientNode_WithRestarts_ShouldNotWarn()
    {
        var builder = new PipelineBuilder();
        var transform = Wire(builder);

        builder.WithResilience(transform, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3 } });
        builder.WithResilience(transform);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();
        result.Issues.Should().NotContain(i => i.Category == "Resilience");
    }

    [Fact]
    public void ResilientNode_WithAZeroReplayWindow_FailsTheBuild()
    {
        var builder = new PipelineBuilder();
        var transform = Wire(builder);

        builder.WithResilience(transform, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3, MaxReplayWindow = 0 } });
        builder.WithResilience(transform);

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
        public ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult<int>(item);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RegularTransform : ITransformNode<int, int>
    {
        public ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult<int>(item);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DummyResiliencePolicy : IResiliencePolicy
    {
        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

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
        public ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult<int>(item);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
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
