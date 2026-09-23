using NPipeline.Execution;
using System.Reflection;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Resilience.Materialization;

public sealed class ResilientMaterializationCapTests
{
    [Fact]
    public async Task Materialization_ShouldThrow_WhenCapExceeded()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var ctx = PipelineContext.CreateDefault();

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
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var ctx = PipelineContext.CreateDefault();
        var act = async () => await runner.RunAsync<WithinCapPipeline>(ctx);
        await act.Should().NotThrowAsync();
    }

    private sealed class NoopResiliencePolicy : IResiliencePolicy
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

    private sealed class StreamingSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new DataStream<int>(Stream(cancellationToken));

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
            builder.WithResilience(o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 1, MaxReplayWindow = 50, Backoff = RetryBackoff.None } });
            builder.AddResiliencePolicy<NoopResiliencePolicy>();
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
            builder.WithResilience(o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 1, MaxReplayWindow = 120, Backoff = RetryBackoff.None } });
            builder.AddResiliencePolicy<NoopResiliencePolicy>();
        }
    }
}
