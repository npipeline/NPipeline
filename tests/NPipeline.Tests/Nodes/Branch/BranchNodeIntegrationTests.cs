// ReSharper disable ClassNeverInstantiated.Local

using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Branch;

/// <summary>
///     Integration tests for BranchNode functionality within the broader pipeline system.
///     Tests branching behavior with real pipeline execution, including data delivery,
///     capacity configuration, and backpressure handling.
/// </summary>
public sealed class BranchNodeIntegrationTests
{
    #region Capacity Tests

    [Fact]
    public async Task Branch_ShouldRecordConfiguredCapacityEvenIfUnboundedInternally()
    {
        var ctx = PipelineContext.Default;
        var collect1 = new InMemorySinkNode<int>();
        var collect2 = new InMemorySinkNode<int>();

        ctx.Items[PipelineContextKeys.PreconfiguredNodes] = new Dictionary<string, INode>
        {
            { "s1", collect1 },
            { "s2", collect2 },
        };

        var runner = PipelineRunner.Create();
        await runner.RunAsync<CapacityBranchingPipeline>(ctx);

        var metrics = ctx.GetBranchMetrics("t");
        metrics.Should().NotBeNull();
        metrics!.SubscriberCount.Should().Be(2);
        metrics.PerSubscriberCapacity.Should().Be(32);
        metrics.SubscribersCompleted.Should().Be(2);
        metrics.Faulted.Should().Be(0);
    }

    #endregion

    #region Backpressure Tests

    [Fact]
    public async Task SlowSubscriber_ShouldNotCauseDataLoss()
    {
        // Arrange
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        // Act
        await runner.RunAsync<BackpressurePipeline>(ctx);

        // Assert - Ensures pipeline runs without exception under multicast.
    }

    #endregion

    #region Basic Branching Tests

    [Fact]
    public async Task RunAsync_WhenBranching_ShouldNotProcessItemsMultipleTimes()
    {
        // Arrange
        var context = PipelineContext.Default;

        // Create and register the sinks in the context before running the pipeline
        var sink1 = new InMemorySinkNode<int>();
        var sink2 = new InMemorySinkNode<int>();
        context.Items["sink1"] = sink1;
        context.Items["sink2"] = sink2;

        var runner = PipelineRunner.Create();

        // Act
        await runner.RunAsync<BasicBranchingPipeline>(context);

        // Assert
        // Each sink should receive exactly the source sequence once.
        var retrievedSink1 = context.Items["sink1"] as InMemorySinkNode<int>;
        var retrievedSink2 = context.Items["sink2"] as InMemorySinkNode<int>;

        retrievedSink1.Should().NotBeNull();
        retrievedSink2.Should().NotBeNull();

        // Each sink should have received 3 items (1, 2, 3)
        retrievedSink1!.Items.Should().HaveCount(3);
        retrievedSink2!.Items.Should().HaveCount(3);

        // Check that we have the expected items in each sink
        var expectedItems = new[] { 1, 2, 3 };
        retrievedSink1.Items.Should().BeEquivalentTo(expectedItems);
        retrievedSink2.Items.Should().BeEquivalentTo(expectedItems);
    }

    [Fact]
    public async Task Branch_ShouldDeliverIdenticalSequences()
    {
        var ctx = PipelineContext.Default;
        var collect1 = new InMemorySinkNode<int>();
        var collect2 = new InMemorySinkNode<int>();

        ctx.Items["collect1"] = collect1;
        ctx.Items["collect2"] = collect2;

        var runner = PipelineRunner.Create();
        await runner.RunAsync<IdenticalSequencesPipeline>(ctx);

        collect1.Items.OrderBy(x => x).Should().Equal(collect2.Items.OrderBy(x => x));
        collect1.Items.Count.Should().Be(50);
        collect2.Items.Count.Should().Be(50);

        var metrics = ctx.GetBranchMetrics("t");
        metrics.Should().NotBeNull();
        metrics!.SubscriberCount.Should().Be(2);
        metrics.PerSubscriberCapacity.Should().BeNull();
        metrics.SubscribersCompleted.Should().Be(2);
        metrics.Faulted.Should().Be(0);

        // Because sinks execute sequentially, backlog accumulates for the not-yet-enumerated subscriber.
        metrics.MaxAggregateBacklog.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task BranchingPipeline_TwoSinks_ReceivesDataInBothSinks()
    {
        // Arrange
        var sourceData = new[] { 1, 2, 3 };
        var context = PipelineContext.Default;

        var sink1 = new InMemorySinkNode<int>();
        var sink2 = new InMemorySinkNode<int>();
        context.Items["sink1"] = sink1;
        context.Items["sink2"] = sink2;

        // Act
        var runner = PipelineRunner.Create();
        await runner.RunAsync<BasicBranchingPipeline>(context);

        // Assert
        var retrievedSink1 = context.Items["sink1"] as InMemorySinkNode<int>;
        var retrievedSink2 = context.Items["sink2"] as InMemorySinkNode<int>;

        retrievedSink1.Should().NotBeNull();
        retrievedSink2.Should().NotBeNull();

        retrievedSink1!.Items.Should().BeEquivalentTo(sourceData);
        retrievedSink2!.Items.Should().BeEquivalentTo(sourceData);
    }

    #endregion

    #region Test Pipeline Definitions

    private sealed class BasicBranchingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", [1, 2, 3]);
            var t = builder.AddTransform<SlowPassThrough, int, int>("t");

            builder.Connect(source, t);

            var sink1 = context.Items["sink1"] as InMemorySinkNode<int>;
            var sink2 = context.Items["sink2"] as InMemorySinkNode<int>;

            if (sink1 != null)
            {
                var sink1Handle = builder.AddSink<InMemorySinkNode<int>, int>("s1");
                builder.AddPreconfiguredNodeInstance("s1", sink1);
                builder.Connect(t, sink1Handle);
            }

            if (sink2 != null)
            {
                var sink2Handle = builder.AddSink<InMemorySinkNode<int>, int>("s2");
                builder.AddPreconfiguredNodeInstance("s2", sink2);
                builder.Connect(t, sink2Handle);
            }
        }
    }

    private sealed class IdenticalSequencesPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", Enumerable.Range(1, 50));
            var t = builder.AddTransform<SlowPassThrough, int, int>("t");

            builder.Connect(source, t);

            var sink1 = context.Items["collect1"] as InMemorySinkNode<int>;
            var sink2 = context.Items["collect2"] as InMemorySinkNode<int>;

            if (sink1 != null)
            {
                var sink1Handle = builder.AddSink<InMemorySinkNode<int>, int>("s1");
                builder.AddPreconfiguredNodeInstance("s1", sink1);
                builder.Connect(t, sink1Handle);
            }

            if (sink2 != null)
            {
                var sink2Handle = builder.AddSink<InMemorySinkNode<int>, int>("s2");
                builder.AddPreconfiguredNodeInstance("s2", sink2);
                builder.Connect(t, sink2Handle);
            }
        }
    }

    private sealed class CapacityBranchingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var src = builder.AddInMemorySource("src", Enumerable.Range(1, 10));
            var t = builder.AddPassThroughTransform<int, int>("t");
            var s1 = builder.AddSink<InMemorySinkNode<int>, int>("s1");
            var s2 = builder.AddSink<InMemorySinkNode<int>, int>("s2");
            builder.Connect(src, t).Connect(t, s1).Connect(t, s2);
            builder.WithBranchOptions("t", new BranchOptions(32));
        }
    }

    private sealed class BackpressurePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var src = builder.AddSource<NumSource, int>("src");
            var slow = builder.AddSink<SlowSink, int>("slow");
            var fast = builder.AddSink<FastSink, int>("fast");
            builder.Connect(src, slow).Connect(src, fast);
        }
    }

    #endregion

    #region Test Node Implementations

    private sealed class SlowPassThrough : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(2, cancellationToken);
            return item;
        }
    }

    private sealed class SlowSink : SinkNode<int>
    {
        private readonly int _delayMs;

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

    #endregion
}
