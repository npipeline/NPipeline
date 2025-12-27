using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using ParallelOptions = NPipeline.Extensions.Parallelism.ParallelOptions;
using BoundedQueuePolicy = NPipeline.Extensions.Parallelism.BoundedQueuePolicy;

namespace NPipeline.Tests.Validation.GraphRules;

public sealed class ResilienceConfigurationRuleTests
{
    [Fact]
    public void ResilientNode_WithoutErrorHandler_ShouldWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1]);
        var transform = builder.AddTransform<ResilientTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Wrap with resilience but no error handler
        builder.WithResilience(transform);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue("Build should succeed, validation issues are warnings");

        result.Issues.Should()
            .Contain(i => i.Category == "Resilience" && i.Message.Contains("no IPipelineErrorHandler"));
    }

    [Fact]
    public void ResilientNode_WithoutRetryOptions_ShouldWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1]);
        var transform = builder.AddTransform<ResilientTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Add error handler but no retry options - this will use context default which has null MaxMaterializedItems
        builder.AddPipelineErrorHandler<DummyErrorHandler>();
        builder.WithResilience(transform);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();

        // When no retry options are configured, the context default (which has null MaxMaterializedItems) is used
        result.Issues.Should()
            .Contain(i => i.Category == "Resilience" && i.Message.Contains("MaxMaterializedItems is null"));
    }

    [Fact]
    public void ResilientNode_WithZeroMaxNodeRestartAttempts_ShouldWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1]);
        var transform = builder.AddTransform<ResilientTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure retry options with zero restart attempts
        builder.WithRetryOptions(opts => opts.With(maxNodeRestartAttempts: 0, maxMaterializedItems: 1000));
        builder.AddPipelineErrorHandler<DummyErrorHandler>();
        builder.WithResilience(transform);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();

        result.Issues.Should()
            .Contain(i => i.Category == "Resilience" && i.Message.Contains("MaxNodeRestartAttempts is 0"));
    }

    [Fact]
    public void ResilientNode_WithNullMaxMaterializedItems_ShouldWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1]);
        var transform = builder.AddTransform<ResilientTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure retry options with null MaxMaterializedItems
        builder.WithRetryOptions(opts => opts.With(maxNodeRestartAttempts: 3, maxMaterializedItems: null));
        builder.AddPipelineErrorHandler<DummyErrorHandler>();
        builder.WithResilience(transform);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();

        result.Issues.Should()
            .Contain(i => i.Category == "Resilience" && i.Message.Contains("MaxMaterializedItems is null"));
    }

    [Fact]
    public void ResilientNode_WithZeroMaxMaterializedItems_ShouldWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1]);
        var transform = builder.AddTransform<ResilientTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure retry options with zero MaxMaterializedItems
        builder.WithRetryOptions(opts => opts.With(maxNodeRestartAttempts: 3, maxMaterializedItems: 0));
        builder.AddPipelineErrorHandler<DummyErrorHandler>();
        builder.WithResilience(transform);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();

        result.Issues.Should()
            .Contain(i => i.Category == "Resilience" && i.Message.Contains("MaxMaterializedItems is 0"));
    }

    [Fact]
    public void ResilientNode_WithCompleteConfig_ShouldNotWarn()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1]);
        var transform = builder.AddTransform<ResilientTransform, int, int>("transform");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure everything properly
        builder.AddPipelineErrorHandler<DummyErrorHandler>();
        builder.WithRetryOptions(opts => opts.With(maxNodeRestartAttempts: 3, maxMaterializedItems: 1000));
        builder.WithResilience(transform);

        var ok = builder.TryBuild(out _, out var result);

        ok.Should().BeTrue();
        result.Issues.Should().NotContain(i => i.Category == "Resilience" && i.Message.Contains("no IPipelineErrorHandler"));
        result.Issues.Should().NotContain(i => i.Category == "Resilience" && i.Message.Contains("MaxNodeRestartAttempts"));
        result.Issues.Should().NotContain(i => i.Category == "Resilience" && i.Message.Contains("MaxMaterializedItems"));
    }

    [Fact]
    public void NonResilientNode_ShouldNotTriggerRule()
    {
        var builder = new PipelineBuilder()
            ;

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1]);
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
        public IExecutionStrategy ExecutionStrategy { get; set; } = new ResilientExecutionStrategy(new SequentialExecutionStrategy());
        public INodeErrorHandler? ErrorHandler { get; set; }

        public Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RegularTransform : ITransformNode<int, int>
    {
        public IExecutionStrategy ExecutionStrategy { get; set; } = new SequentialExecutionStrategy();
        public INodeErrorHandler? ErrorHandler { get; set; }

        public Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DummyErrorHandler : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(
            string nodeId, Exception error, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
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

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", new[] { 1, 2, 3 });
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

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", new[] { 1, 2, 3 });
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

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", new[] { 1, 2, 3 });
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

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", new[] { 1, 2, 3 });
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

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", new[] { 1, 2, 3 });
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

        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", new[] { 1, 2, 3 });
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
        public IExecutionStrategy ExecutionStrategy { get; set; } = new SequentialExecutionStrategy();
        public INodeErrorHandler? ErrorHandler { get; set; }

        public Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
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
