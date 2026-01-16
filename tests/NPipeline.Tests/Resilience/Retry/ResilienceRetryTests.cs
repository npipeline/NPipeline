using System.Reflection;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Resilience.Retry;

public sealed class ResilienceRetryTests
{
    [Fact]
    public async Task ResilientStrategy_Retries_Then_Succeeds_On_TransientFailure()
    {
        // Arrange DI runner
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<RestartHandler>();

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = new PipelineContext(
            PipelineContextConfiguration.Default with { ErrorHandlerFactory = new DefaultErrorHandlerFactory() });

        // Provide a single item source
        ctx.SetSourceData([42]);

        // Act
        await runner.RunAsync<TestPipeline>(ctx);

        // Assert: at least one output produced and every produced value equals the expected item.
        var sink = ctx.GetSink<InMemorySinkNode<int>>();
        sink.Items.Should().NotBeEmpty();

        foreach (var i in sink.Items)
        {
            i.Should().Be(42);
        }
    }

    private sealed class TransientFailTransform : TransformNode<int, int>
    {
        private int _attempts;

        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempts++;

            // fail first two attempts then succeed
            if (_attempts <= 2)
                throw new InvalidOperationException("transient");

            return Task.FromResult(item);
        }
    }

    private sealed class RestartHandler : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception exception, PipelineContext context,
            CancellationToken cancellationToken)
        {
            // Always request restart for transient failures
            return Task.FromResult(PipelineErrorDecision.RestartNode);
        }
    }

    private sealed class TestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("srcR");
            var t = builder.AddTransform<TransientFailTransform, int, int>("txR");
            var k = builder.AddInMemorySink<int>("snkR");
            builder.Connect(s, t).Connect(t, k);
            builder.AddPipelineErrorHandler<RestartHandler>();
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 3, maxMaterializedItems: 1000));
        }
    }
}
