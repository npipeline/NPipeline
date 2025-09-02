// ReSharper disable ClassNeverInstantiated.Local

using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

/// <summary>
///     Comprehensive tests for error handler resolution as specified in the production-readiness plan.
///     Tests node-level error handler override, global error handler fallback, missing error handler handling,
///     and error decision propagation.
/// </summary>
public sealed class ErrorHandlerResolutionTests
{
    #region Node-level Error Handler Override Tests

    [Fact]
    public async Task ErrorHandlerResolution_NodeLevelHandlerOverridesGlobal_SkipsFailedItem()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<GlobalTestErrorHandler>();
        services.AddSingleton<NodeLevelTestErrorHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var context = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

        // Set up test data with one item that will fail
        context.SetSourceData(["ok", "fail", "ok2"]);

        // Act
        await runner.RunAsync<NodeLevelOverridePipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.Items.Should().BeEquivalentTo("ok", "ok2");

        // Verify node-level handler was called, not global handler
        var nodeHandler = sp.GetRequiredService<NodeLevelTestErrorHandler>();
        var globalHandler = sp.GetRequiredService<GlobalTestErrorHandler>();

        nodeHandler.CallCount.Should().Be(1);
        globalHandler.CallCount.Should().Be(0);
    }

    [Fact]
    public async Task ErrorHandlerResolution_NodeLevelHandlerOverridesGlobal_FailsPipeline()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<GlobalTestErrorHandler>();
        services.AddSingleton<FailNodeLevelErrorHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var context = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

        // Set up test data with one item that will fail
        context.SetSourceData(["fail-me"]);

        // Act & Assert
        await runner.Awaiting(r => r.RunAsync<NodeLevelFailPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException))
            .WithMessage("*fail-me*");

        // Verify node-level handler was called, not global handler
        var nodeHandler = sp.GetRequiredService<FailNodeLevelErrorHandler>();
        var globalHandler = sp.GetRequiredService<GlobalTestErrorHandler>();

        nodeHandler.CallCount.Should().Be(1);
        globalHandler.CallCount.Should().Be(0);
    }

    #endregion

    #region Global Error Handler Fallback Tests

    [Fact]
    public async Task ErrorHandlerResolution_NoNodeLevelHandler_UsesGlobalFallback_SkipsFailedItem()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<GlobalTestErrorHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var context = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

        // Set up test data with one item that will fail
        context.SetSourceData(["ok", "fail", "ok2"]);

        // Act
        await runner.RunAsync<GlobalFallbackPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.Items.Should().BeEquivalentTo("ok", "ok2");

        // Verify global handler was called
        var globalHandler = sp.GetRequiredService<GlobalTestErrorHandler>();
        globalHandler.CallCount.Should().Be(1);
    }

    [Fact]
    public async Task ErrorHandlerResolution_NoNodeLevelHandler_UsesGlobalFallback_FailsPipeline()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<FailGlobalErrorHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var context = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

        // Set up test data with one item that will cause pipeline to fail
        context.SetSourceData(["fail-pipeline"]);

        // Act & Assert
        await runner.Awaiting(r => r.RunAsync<GlobalFailPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException))
            .WithMessage("*fail-pipeline*");

        // Verify global handler was called
        var globalHandler = sp.GetRequiredService<FailGlobalErrorHandler>();
        globalHandler.CallCount.Should().Be(1);
    }

    #endregion

    #region Missing Error Handler Tests

    [Fact]
    public async Task ErrorHandlerResolution_NoHandlerAvailable_DefaultsToFail_ThrowsException()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        // No error handlers registered

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var context = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

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
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<NodeLevelTestErrorHandler>();

        // But no global handler registered

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var context = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

        // Set up test data with one item that will fail
        context.SetSourceData(["missing-global"]);

        // Act
        await runner.RunAsync<MissingGlobalHandlerPipeline>(context);

        // Assert - The node-level handler should be used and skip failed item
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.Items.Should().BeEmpty();

        // Verify node-level handler was called
        var nodeHandler = sp.GetRequiredService<NodeLevelTestErrorHandler>();
        nodeHandler.CallCount.Should().Be(1);
    }

    #endregion

    #region Error Decision Propagation Tests

    [Fact]
    public async Task ErrorHandlerResolution_SkipDecision_PropagatesToSkipItem()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<SkipDecisionHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var context = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

        // Set up test data with multiple items, one will fail
        context.SetSourceData(["item1", "fail-item", "item2", "item3"]);

        // Act
        await runner.RunAsync<SkipDecisionPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.Items.Should().BeEquivalentTo("item1", "item2", "item3");

        // Verify handler was called once for the failed item
        var handler = sp.GetRequiredService<SkipDecisionHandler>();
        handler.CallCount.Should().Be(1);
        handler.LastDecision.Should().Be(NodeErrorDecision.Skip);
    }

    [Fact]
    public async Task ErrorHandlerResolution_FailDecision_PropagatesToFailPipeline()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<FailDecisionHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var context = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

        // Set up test data with an item that will cause pipeline failure
        context.SetSourceData(["fail-pipeline-item"]);

        // Act & Assert
        await runner.Awaiting(r => r.RunAsync<FailDecisionPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException))
            .WithMessage("*fail-pipeline-item*");

        // Verify handler was called once for the failed item
        var handler = sp.GetRequiredService<FailDecisionHandler>();
        handler.CallCount.Should().Be(1);
        handler.LastDecision.Should().Be(NodeErrorDecision.Fail);
    }

    #endregion

    #region Helper Classes

    // Test nodes
    private sealed class FailingTransform : TransformNode<string, string>
    {
        public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item.StartsWith("fail", StringComparison.Ordinal) || item == "no-handler" || item == "missing-global" ||
                item == "fail-item" || item == "fail-pipeline-item")
                throw new InvalidOperationException($"Failed on purpose: {item}");

            return Task.FromResult(item);
        }
    }

    // Error handlers
    private sealed class GlobalTestErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
    {
        public int CallCount { get; private set; }

        public Task<NodeErrorDecision> HandleAsync(ITransformNode<string, string> node, string failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.FromResult(NodeErrorDecision.Skip);
        }
    }

    private sealed class NodeLevelTestErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
    {
        public int CallCount { get; private set; }

        public Task<NodeErrorDecision> HandleAsync(ITransformNode<string, string> node, string failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.FromResult(NodeErrorDecision.Skip);
        }
    }

    private sealed class FailNodeLevelErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
    {
        public int CallCount { get; private set; }

        public Task<NodeErrorDecision> HandleAsync(ITransformNode<string, string> node, string failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.FromResult(NodeErrorDecision.Fail);
        }
    }

    private sealed class FailGlobalErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
    {
        public int CallCount { get; private set; }

        public Task<NodeErrorDecision> HandleAsync(ITransformNode<string, string> node, string failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.FromResult(NodeErrorDecision.Fail);
        }
    }

    private sealed class SkipDecisionHandler : INodeErrorHandler<ITransformNode<string, string>, string>
    {
        public int CallCount { get; private set; }
        public NodeErrorDecision LastDecision { get; private set; }

        public Task<NodeErrorDecision> HandleAsync(ITransformNode<string, string> node, string failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            CallCount++;
            LastDecision = NodeErrorDecision.Skip;
            return Task.FromResult(NodeErrorDecision.Skip);
        }
    }

    private sealed class FailDecisionHandler : INodeErrorHandler<ITransformNode<string, string>, string>
    {
        public int CallCount { get; private set; }
        public NodeErrorDecision LastDecision { get; private set; }

        public Task<NodeErrorDecision> HandleAsync(ITransformNode<string, string> node, string failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            CallCount++;
            LastDecision = NodeErrorDecision.Fail;
            return Task.FromResult(NodeErrorDecision.Fail);
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
            builder.WithErrorHandler<NodeLevelTestErrorHandler, string, string>(transform);
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
            builder.WithErrorHandler<FailNodeLevelErrorHandler, string, string>(transform);
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
            builder.WithErrorHandler<GlobalTestErrorHandler, string, string>(transform);
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
            builder.WithErrorHandler<FailGlobalErrorHandler, string, string>(transform);
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

            // No error handler configured
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
            builder.WithErrorHandler<NodeLevelTestErrorHandler, string, string>(transform);
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
            builder.WithErrorHandler<SkipDecisionHandler, string, string>(transform);
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
            builder.WithErrorHandler<FailDecisionHandler, string, string>(transform);
        }
    }

    #endregion
}
