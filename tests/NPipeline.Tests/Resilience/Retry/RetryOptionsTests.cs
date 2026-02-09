using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Resilience.Retry;

public sealed class RetryOptionsTests
{
    [Fact]
    public async Task Should_StopAfterConfiguredItemRetries()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<FlakyNodeErrorHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = new PipelineContext(
            PipelineContextConfiguration.Default with { ErrorHandlerFactory = new DefaultErrorHandlerFactory() });

        // Set source data on the context
        ctx.SetSourceData([1]);

        // Act & Assert
        var act = async () => await runner.RunAsync<SequentialRetryPipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .Where(ex => ex.InnerException is InvalidOperationException);
    }

    [Fact]
    public async Task Should_FailAfterMaxNodeRestartAttempts()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<NodeRestartingErrorHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = new PipelineContext(
            PipelineContextConfiguration.Default with { ErrorHandlerFactory = new DefaultErrorHandlerFactory() });

        // Set source data on the context
        ctx.SetSourceData([1]);

        var act = async () => await runner.RunAsync<ResilientPipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(RetryExhaustedException));
    }

    [Fact]
    public async Task PerNodeOverride_ShouldApplyInsteadOfGlobal()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<FlakyNodeErrorHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = new PipelineContext(
            PipelineContextConfiguration.Default with { ErrorHandlerFactory = new DefaultErrorHandlerFactory() });

        // Set source data on the context
        ctx.SetSourceData([1]);

        var act = async () => await runner.RunAsync<PerNodeOverridePipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException));
    }

    private sealed class FlakyTransform : TransformNode<int, int>
    {
        private int _attempts;

        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempts++;
            throw new InvalidOperationException($"fail-{_attempts}");
        }
    }

    private sealed class FlakyNodeErrorHandler : INodeErrorHandler<ITransformNode<int, int>, int>
    {
        public Task<NodeErrorDecision> HandleAsync(ITransformNode<int, int> node, int failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Retry);
        }
    }

    private sealed class SequentialRetryPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("s");
            var t = builder.AddTransform<FlakyTransform, int, int>("t");
            var k = builder.AddInMemorySink<int>("k");
            builder.Connect(s, t).Connect(t, k);
            builder.WithErrorHandler(t, typeof(FlakyNodeErrorHandler));
            builder.WithRetryOptions(o => o.With(2));
        }
    }

    private sealed class FailingTransform : TransformNode<int, int>
    {
        private int _attempt;

        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempt++;

            if (_attempt < 3)
                throw new InvalidOperationException("boom");

            return Task.FromResult(item);
        }
    }

    private sealed class NodeRestartingErrorHandler : IPipelineErrorHandler
    {
        private int _fails;

        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception exception, PipelineContext context,
            CancellationToken cancellationToken)
        {
            _fails++;

            return Task.FromResult(_fails < 3
                ? PipelineErrorDecision.RestartNode
                : PipelineErrorDecision.FailPipeline);
        }
    }

    private sealed class ResilientPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("s2");
            var t = builder.AddTransform<FailingTransform, int, int>("ft");
            var k = builder.AddInMemorySink<int>("k2");
            builder.Connect(s, t).Connect(t, k);
            builder.AddPipelineErrorHandler<NodeRestartingErrorHandler>();
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 2));
        }
    }

    private sealed class PerNodeOverridePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("s3");
            var t = builder.AddTransform<FlakyTransform, int, int>("ot");
            var k = builder.AddInMemorySink<int>("k3");
            builder.Connect(s, t).Connect(t, k);
            builder.WithErrorHandler(t, typeof(FlakyNodeErrorHandler));
            builder.WithRetryOptions(o => o.With(5)); // global
            builder.WithRetryOptions(t, PipelineRetryOptions.Default.With(1)); // override
        }
    }
}
