using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;
using NPipeline.Tests.Common;

namespace NPipeline.Extensions.Parallelism.Tests;

[Collection("StatefulTests")]
public class ParallelRetryTests
{
    [Fact]
    public async Task ParallelTransform_Should_StopAfterConfiguredItemRetries()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<RetryAllHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

        // Set source data on the context
        ctx.SetSourceData([1]);

        // Act & Assert
        var act = async () => await runner.RunAsync<ParallelRetryPipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException));
    }

    [Fact]
    public async Task ParallelTransform_PerNodeOverride_ShouldApply()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<RetryAllHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

        // Set source data on the context
        ctx.SetSourceData([1]);

        // Act & Assert
        var act = async () => await runner.RunAsync<OverrideParallelRetryPipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException));
    }

    private sealed class FlakyParallelTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            var attempt = SharedTestState.AttemptCounts.AddOrUpdate(item, 1, (_, i) => i + 1);
            throw new InvalidOperationException($"fail-{item}-{attempt}");
        }
    }

    private sealed class RetryAllHandler : INodeErrorHandler<ITransformNode<int, int>, int>
    {
        public Task<NodeErrorDecision> HandleAsync(ITransformNode<int, int> node, int failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Retry);
        }
    }

    private sealed class ParallelRetryPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("ps");
            var t = builder.AddTransform<FlakyParallelTransform, int, int>("pt");
            var k = builder.AddInMemorySink<int>("pk");

            builder.WithExecutionStrategy(t, new ParallelExecutionStrategy(2))
                .WithErrorHandler(t, typeof(RetryAllHandler))
                .WithRetryOptions(o => o.With(1)) // allow only 1 retry => attempt > 1 should throw
                .Connect(s, t).Connect(t, k);
        }
    }

    private sealed class OverrideParallelRetryPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("os");
            var t = builder.AddTransform<FlakyParallelTransform, int, int>("ot");
            var k = builder.AddInMemorySink<int>("ok");

            builder.WithExecutionStrategy(t, new ParallelExecutionStrategy(4))
                .WithErrorHandler(t, typeof(RetryAllHandler))
                .WithRetryOptions(o => o.With(5)) // global high
                .WithRetryOptions(t, PipelineRetryOptions.Default.With(2)) // node override lower
                .Connect(s, t).Connect(t, k);
        }
    }
}
