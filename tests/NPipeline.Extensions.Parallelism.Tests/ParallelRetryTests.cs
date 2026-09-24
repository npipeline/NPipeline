using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
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
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        SharedTestState.Reset(1, 0);
        var ctx = new PipelineContext();

        // Set source data on the context
        ctx.SetSourceData([1]);

        // Act & Assert
        var act = async () => await runner.RunAsync<ParallelRetryPipeline>(ctx);

        _ = await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(RetryExhaustedException));

        _ = SharedTestState.AttemptCounts[1].Should().Be(2); // first attempt + 1 retry
    }

    [Fact]
    public async Task ParallelTransform_PerNodeOverride_ShouldApply()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        SharedTestState.Reset(1, 0);
        var ctx = new PipelineContext();

        // Set source data on the context
        ctx.SetSourceData([1]);

        // Act & Assert
        var act = async () => await runner.RunAsync<OverrideParallelRetryPipeline>(ctx);

        _ = await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(RetryExhaustedException));

        _ = SharedTestState.AttemptCounts[1].Should().Be(3); // first attempt + 2 retries from the node override
    }

    private sealed class FlakyParallelTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            var attempt = SharedTestState.AttemptCounts.AddOrUpdate(item, 1, (_, i) => i + 1);
            throw new InvalidOperationException($"fail-{item}-{attempt}");
        }
    }

    private sealed class ParallelRetryPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("ps");
            var t = builder.AddTransform<FlakyParallelTransform, int, int>("pt");
            var k = builder.AddInMemorySink<int>("pk");

            _ = builder.WithExecutionStrategy(t, new ParallelExecutionStrategy(2))
                .AddResiliencePolicy(t, new RetryHandler())
                .WithResilience(o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 1 } }) // allow only 1 retry => attempt > 1 should throw
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
                .AddResiliencePolicy(t, new RetryHandler())
                .WithResilience(o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 5 } }) // global high
                .WithResilience(t, o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 2 } }) // node override lower
                .Connect(s, t).Connect(t, k);
        }
    }
}
