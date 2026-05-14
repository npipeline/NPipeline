// ReSharper disable ClassNeverInstantiated.Local

using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Tests.ErrorHandling;

/// <summary>
///     Comprehensive tests for resilience policy resolution. Covers node-level policy override,
///     global policy fallback, missing policy behavior, and decision propagation.
/// </summary>
public sealed class ErrorHandlerResolutionTests
{
    private static void ResetHandlerState()
    {
        GlobalTestErrorHandler.GlobalCallCount = 0;
        NodeLevelTestErrorHandler.GlobalCallCount = 0;
        FailNodeLevelErrorHandler.GlobalCallCount = 0;
        FailGlobalErrorHandler.GlobalCallCount = 0;
        SkipDecisionHandler.GlobalCallCount = 0;
        SkipDecisionHandler.LastReturnedDecision = null;
        FailDecisionHandler.GlobalCallCount = 0;
        FailDecisionHandler.LastReturnedDecision = null;
    }

    #region Node-level Policy Override Tests

    [Fact]
    public async Task ErrorHandlerResolution_NodeLevelHandlerOverridesGlobal_SkipsFailedItem()
    {
        // Arrange
        ResetHandlerState();
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<GlobalTestErrorHandler>();
        services.AddSingleton<NodeLevelTestErrorHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Set up test data with one item that will fail
        context.SetSourceData(["ok", "fail", "ok2"]);

        // Act
        await runner.RunAsync<NodeLevelOverridePipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.Items.Should().BeEquivalentTo("ok", "ok2");

        // Verify node-level policy was called, not global policy
        var nodeHandler = sp.GetRequiredService<NodeLevelTestErrorHandler>();
        var globalHandler = sp.GetRequiredService<GlobalTestErrorHandler>();

        nodeHandler.CallCount.Should().Be(1);
        globalHandler.CallCount.Should().Be(0);
    }

    [Fact]
    public async Task ErrorHandlerResolution_NodeLevelHandlerOverridesGlobal_FailsPipeline()
    {
        // Arrange
        ResetHandlerState();
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<GlobalTestErrorHandler>();
        services.AddSingleton<FailNodeLevelErrorHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Set up test data with one item that will fail
        context.SetSourceData(["fail-me"]);

        // Act & Assert
        await runner.Awaiting(r => r.RunAsync<NodeLevelFailPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException))
            .WithMessage("*fail-me*");

        // Verify node-level policy was called, not global policy
        var nodeHandler = sp.GetRequiredService<FailNodeLevelErrorHandler>();
        var globalHandler = sp.GetRequiredService<GlobalTestErrorHandler>();

        nodeHandler.CallCount.Should().Be(1);
        globalHandler.CallCount.Should().Be(0);
    }

    #endregion

    #region Global Policy Fallback Tests

    [Fact]
    public async Task ErrorHandlerResolution_NoNodeLevelHandler_UsesGlobalFallback_SkipsFailedItem()
    {
        // Arrange
        ResetHandlerState();
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<GlobalTestErrorHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Set up test data with one item that will fail
        context.SetSourceData(["ok", "fail", "ok2"]);

        // Act
        await runner.RunAsync<GlobalFallbackPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.Items.Should().BeEquivalentTo("ok", "ok2");

        // Verify global policy was called
        var globalHandler = sp.GetRequiredService<GlobalTestErrorHandler>();
        globalHandler.CallCount.Should().Be(1);
    }

    [Fact]
    public async Task ErrorHandlerResolution_NoNodeLevelHandler_UsesGlobalFallback_FailsPipeline()
    {
        // Arrange
        ResetHandlerState();
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<FailGlobalErrorHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Set up test data with one item that will cause pipeline to fail
        context.SetSourceData(["fail-pipeline"]);

        // Act & Assert
        await runner.Awaiting(r => r.RunAsync<GlobalFailPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException))
            .WithMessage("*fail-pipeline*");

        // Verify global policy was called
        var globalHandler = sp.GetRequiredService<FailGlobalErrorHandler>();
        globalHandler.CallCount.Should().Be(1);
    }

    #endregion

    #region Missing Policy Tests

    [Fact]
    public async Task ErrorHandlerResolution_NoHandlerAvailable_DefaultsToFail_ThrowsException()
    {
        // Arrange
        ResetHandlerState();
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Set up test data with one item that will fail
        context.SetSourceData(["no-handler"]);

        // Act & Assert
        await runner.Awaiting(r => r.RunAsync<NoHandlerPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException))
            .WithMessage("*no-handler*");
    }

    [Fact]
    public async Task ErrorHandlerResolution_MissingHandlerInFactory_UsesDefaultBehavior()
    {
        // Arrange
        ResetHandlerState();
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<NodeLevelTestErrorHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Set up test data with one item that will fail
        context.SetSourceData(["missing-global"]);

        // Act
        await runner.RunAsync<MissingGlobalHandlerPipeline>(context);

        // Assert - The node-level policy should be used and skip failed item
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.Items.Should().BeEmpty();

        // Verify node-level policy was called
        var nodeHandler = sp.GetRequiredService<NodeLevelTestErrorHandler>();
        nodeHandler.CallCount.Should().Be(1);
    }

    #endregion

    #region Decision Propagation Tests

    [Fact]
    public async Task ErrorHandlerResolution_SkipDecision_PropagatesToSkipItem()
    {
        // Arrange
        ResetHandlerState();
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<SkipDecisionHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Set up test data with multiple items, one will fail
        context.SetSourceData(["item1", "fail-item", "item2", "item3"]);

        // Act
        await runner.RunAsync<SkipDecisionPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.Items.Should().BeEquivalentTo("item1", "item2", "item3");

        // Verify policy was called once for the failed item
        var handler = sp.GetRequiredService<SkipDecisionHandler>();
        handler.CallCount.Should().Be(1);
        handler.LastDecision.Should().Be(ResilienceDecision.Skip);
    }

    [Fact]
    public async Task ErrorHandlerResolution_FailDecision_PropagatesToFailPipeline()
    {
        // Arrange
        ResetHandlerState();
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<FailDecisionHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Set up test data with an item that will cause pipeline failure
        context.SetSourceData(["fail-pipeline-item"]);

        // Act & Assert
        await runner.Awaiting(r => r.RunAsync<FailDecisionPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException))
            .WithMessage("*fail-pipeline-item*");

        // Verify policy was called once for the failed item
        var handler = sp.GetRequiredService<FailDecisionHandler>();
        handler.CallCount.Should().Be(1);
        handler.LastDecision.Should().Be(ResilienceDecision.Fail);
    }

    #endregion

    #region Helper Classes

    // Test nodes
    private sealed class FailingTransform : TransformNode<string, string>
    {
        public override Task<string> TransformAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item.StartsWith("fail", StringComparison.Ordinal) || item == "no-handler" || item == "missing-global" ||
                item == "fail-item" || item == "fail-pipeline-item")
                throw new InvalidOperationException($"Failed on purpose: {item}");

            return Task.FromResult(item);
        }
    }

    private abstract class CountingPolicyBase : IResiliencePolicy
    {
        public virtual Task<ResilienceDecision> DecideNodeFailureAsync(
            NodeDefinition nodeDefinition,
            INode node,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public virtual Task<ResilienceDecision> DecidePipelineFailureAsync(
            string nodeId,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public abstract Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken);

        public ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
        {
            return context.GetRetryDelayStrategy().GetDelayAsync(attemptNumber, cancellationToken);
        }

        public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
        {
            return DefaultResiliencePolicy.Instance.GetCircuitBreaker(context, nodeId);
        }
    }

    private sealed class GlobalTestErrorHandler : CountingPolicyBase
    {
        public static int GlobalCallCount { get; set; }
        public int CallCount => GlobalCallCount;

        public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            GlobalCallCount++;
            return Task.FromResult(ResilienceDecision.Skip);
        }
    }

    private sealed class NodeLevelTestErrorHandler : CountingPolicyBase
    {
        public static int GlobalCallCount { get; set; }
        public int CallCount => GlobalCallCount;

        public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            GlobalCallCount++;
            return Task.FromResult(ResilienceDecision.Skip);
        }
    }

    private sealed class FailNodeLevelErrorHandler : CountingPolicyBase
    {
        public static int GlobalCallCount { get; set; }
        public int CallCount => GlobalCallCount;

        public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            GlobalCallCount++;
            return Task.FromResult(ResilienceDecision.Fail);
        }
    }

    private sealed class FailGlobalErrorHandler : CountingPolicyBase
    {
        public static int GlobalCallCount { get; set; }
        public int CallCount => GlobalCallCount;

        public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            GlobalCallCount++;
            return Task.FromResult(ResilienceDecision.Fail);
        }
    }

    private sealed class SkipDecisionHandler : CountingPolicyBase
    {
        public static int GlobalCallCount { get; set; }
        public static ResilienceDecision? LastReturnedDecision { get; set; }
        public int CallCount => GlobalCallCount;
        public ResilienceDecision LastDecision => LastReturnedDecision ?? ResilienceDecision.Fail;

        public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            GlobalCallCount++;
            LastReturnedDecision = ResilienceDecision.Skip;
            return Task.FromResult(ResilienceDecision.Skip);
        }
    }

    private sealed class FailDecisionHandler : CountingPolicyBase
    {
        public static int GlobalCallCount { get; set; }
        public static ResilienceDecision? LastReturnedDecision { get; set; }
        public int CallCount => GlobalCallCount;
        public ResilienceDecision LastDecision => LastReturnedDecision ?? ResilienceDecision.Fail;

        public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            GlobalCallCount++;
            LastReturnedDecision = ResilienceDecision.Fail;
            return Task.FromResult(ResilienceDecision.Fail);
        }
    }

    // Pipeline definitions
    private sealed class NodeLevelOverridePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var transform = builder.AddTransform<FailingTransform, string, string>("transform");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
            builder.AddResiliencePolicy<GlobalTestErrorHandler>();
            builder.SetNodeResiliencePolicy(transform, new NodeLevelTestErrorHandler());
        }
    }

    private sealed class NodeLevelFailPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var transform = builder.AddTransform<FailingTransform, string, string>("transform");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
            builder.AddResiliencePolicy<GlobalTestErrorHandler>();
            builder.SetNodeResiliencePolicy(transform, new FailNodeLevelErrorHandler());
        }
    }

    private sealed class GlobalFallbackPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var transform = builder.AddTransform<FailingTransform, string, string>("transform");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
            builder.AddResiliencePolicy<GlobalTestErrorHandler>();
        }
    }

    private sealed class GlobalFailPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var transform = builder.AddTransform<FailingTransform, string, string>("transform");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
            builder.AddResiliencePolicy<FailGlobalErrorHandler>();
        }
    }

    private sealed class NoHandlerPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var transform = builder.AddTransform<FailingTransform, string, string>("transform");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
        }
    }

    private sealed class MissingGlobalHandlerPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var transform = builder.AddTransform<FailingTransform, string, string>("transform");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
            builder.SetNodeResiliencePolicy(transform, new NodeLevelTestErrorHandler());
        }
    }

    private sealed class SkipDecisionPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var transform = builder.AddTransform<FailingTransform, string, string>("transform");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
            builder.SetNodeResiliencePolicy(transform, new SkipDecisionHandler());
        }
    }

    private sealed class FailDecisionPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var transform = builder.AddTransform<FailingTransform, string, string>("transform");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
            builder.SetNodeResiliencePolicy(transform, new FailDecisionHandler());
        }
    }

    #endregion
}
