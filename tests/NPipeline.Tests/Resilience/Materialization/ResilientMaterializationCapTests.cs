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
using NPipeline.Resilience;

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
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var ctx = PipelineContext.Default;
        var act = async () => await runner.RunAsync<WithinCapPipeline>(ctx);
        await act.Should().NotThrowAsync();
    }

    private sealed class NoopResiliencePolicy : IResiliencePolicy
    {
        public Task<ResilienceDecision> DecideNodeFailureAsync(
            NodeDefinition nodeDefinition,
            INode node,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(
            string nodeId,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
        {
            return context.GetRetryDelayStrategy().GetDelayAsync(attemptNumber, cancellationToken);
        }

        public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
        {
            return DefaultResiliencePolicy.Instance.GetCircuitBreaker(context, nodeId);
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
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 1, maxMaterializedItems: 50));
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
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 1, maxMaterializedItems: 120));
            builder.AddResiliencePolicy<NoopResiliencePolicy>();
        }
    }
}
