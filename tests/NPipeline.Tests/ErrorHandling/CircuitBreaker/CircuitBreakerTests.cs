using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling.CircuitBreaker;

public sealed class CircuitBreakerTests
{
    [Fact]
    public async Task CircuitBreaker_Trips_AfterThreshold()
    {
        var runner = PipelineRunner.Create();

        var ctx = new PipelineContextBuilder()
            .WithErrorHandler(new AlwaysRestartHandler())
            .Build();

        var act = () => runner.RunAsync<TestPipeline>(ctx);

        var exception = await act.Should().ThrowAsync<NodeExecutionException>();
        _ = exception.WithInnerException<CircuitBreakerOpenException>();
    }

    private sealed class MultiItemSource : SourceNode<int>
    {
        public override IDataPipe<int> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            IDataPipe<int> pipe = new StreamingDataPipe<int>(Stream());
            return pipe;

            static async IAsyncEnumerable<int> Stream()
            {
                for (var i = 1; i <= 3; i++)
                {
                    yield return i;

                    await Task.CompletedTask;
                }
            }
        }
    }

    private sealed class FailingTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("Boom");
        }
    }

    private sealed class NoOpSink : SinkNode<int>
    {
        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                /* drain */
            }
        }
    }

    private sealed class AlwaysRestartHandler : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception error, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(PipelineErrorDecision.RestartNode);
        }
    }

    private sealed class TestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var s = b.AddSource<MultiItemSource, int>("src");
            var t = b.AddTransform<FailingTransform, int, int>("fail");
            var k = b.AddSink<NoOpSink, int>("sink");

            _ = b.Connect(s, t);
            _ = b.Connect(t, k);
            b.AddPipelineErrorHandler<AlwaysRestartHandler>();

            // Ensure transform runs under resilient strategy so restarts occur during streaming enumeration
            b.WithExecutionStrategy(t, new ResilientExecutionStrategy(new SequentialExecutionStrategy()));
            b.WithCircuitBreaker(2, TimeSpan.FromMinutes(1));
        }
    }
}
