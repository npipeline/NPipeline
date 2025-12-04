using System.Reflection;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Resilience.Materialization;

public sealed class ResilientMaterializationCapTests
{
    [Fact]
    public async Task Materialization_ShouldThrow_WhenCapExceeded()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<NoopHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var ctx = PipelineContext.Default;

        // Act
        var act = async () => await runner.RunAsync<ResilientPipeline>(ctx);

        // Assert
        await act.Should().ThrowAsync<NodeExecutionException>();
    }

    [Fact]
    public async Task Materialization_ShouldSucceed_WhenWithinCap()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<NoopHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var ctx = PipelineContext.Default;
        var act = async () => await runner.RunAsync<WithinCapPipeline>(ctx);
        await act.Should().NotThrowAsync();
    }

    private sealed class NoopHandler : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception error, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
        }
    }

    private sealed class StreamingSource : SourceNode<int>
    {
        public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new StreamingDataPipe<int>(Stream(cancellationToken));

            static async IAsyncEnumerable<int> Stream([EnumeratorCancellation] CancellationToken ct)
            {
                await Task.Yield();

                for (var i = 0; i < 100; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return i;
                }
            }
        }
    }

    private sealed class ResilientPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddSource<StreamingSource, int>("srcMat");
            var t = builder.AddPassThroughTransform<int, int>("txMat");
            var k = builder.AddInMemorySink<int>("snkMat");
            builder.Connect(s, t).Connect(t, k);
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 1, maxMaterializedItems: 50));
            builder.AddPipelineErrorHandler<NoopHandler>(); // ensure resilience layer activates
        }
    }

    private sealed class WithinCapPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddSource<StreamingSource, int>("srcMat2");
            var t = builder.AddPassThroughTransform<int, int>("txMat2");
            var k = builder.AddInMemorySink<int>("snkMat2");
            builder.Connect(s, t).Connect(t, k);
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 1, maxMaterializedItems: 120));
            builder.AddPipelineErrorHandler<NoopHandler>(); // ensure resilience layer activates
        }
    }
}
