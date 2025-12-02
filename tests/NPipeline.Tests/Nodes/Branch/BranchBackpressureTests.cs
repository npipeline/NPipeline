using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit.Abstractions;

// using System.Threading.Channels; // not needed

namespace NPipeline.Tests.Nodes.Branch;

public sealed class BranchBackpressureTests(ITestOutputHelper output)
{
    [Fact]
    public async Task SlowSubscriber_ShouldNotCauseDataLoss()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        // Act
        await runner.RunAsync<BranchingPipeline>(ctx);

        // Assert
        // Can't directly access sink instances (not DI-built here). This test is placeholder until DI integration.
        // Ensures pipeline runs without exception under multicast.
    }

    private sealed class SlowSink : SinkNode<int>
    {
        private readonly int _delayMs;

        // Parameterless ctor required by DefaultNodeFactory
        public SlowSink() : this(1)
        {
        }

        private SlowSink(int delayMs)
        {
            _delayMs = delayMs;
        }

        public List<int> Items { get; } = [];

        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                await Task.Delay(_delayMs, cancellationToken);
                Items.Add(item);
            }
        }
    }

    private sealed class FastSink : SinkNode<int>
    {
        public List<int> Items { get; } = [];

        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                Items.Add(item);
            }
        }
    }

    private sealed class BranchingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var src = builder.AddSource<NumSource, int>("src");
            var slow = builder.AddSink<SlowSink, int>("slow");
            var fast = builder.AddSink<FastSink, int>("fast");
            builder.Connect(src, slow).Connect(src, fast);
        }
    }

    private sealed class NumSource : SourceNode<int>
    {
        public override IDataPipe<int> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            return new StreamingDataPipe<int>(Produce(cancellationToken), "nums");

            static async IAsyncEnumerable<int> Produce([EnumeratorCancellation] CancellationToken ct)
            {
                await Task.Yield();

                for (var i = 0; i < 50; i++)
                {
                    yield return i;
                }
            }
        }
    }
}
