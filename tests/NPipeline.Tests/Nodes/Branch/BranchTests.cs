// ReSharper disable ClassNeverInstantiated.Local

using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit.Abstractions;

namespace NPipeline.Tests.Nodes.Branch;

public sealed class BranchTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

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
        await runner.RunAsync<BranchingPipeline>(context);

        // Assert
        // Each sink should receive exactly the source sequence once.
        // Use the GetSink extension method to retrieve the sinks
        var retrievedSink1 = context.Items["sink1"] as InMemorySinkNode<int>;
        var retrievedSink2 = context.Items["sink2"] as InMemorySinkNode<int>;

        // Both sinks should have been created
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

        // Register the sinks in the context with different keys
        ctx.Items["collect1"] = collect1;
        ctx.Items["collect2"] = collect2;

        var runner = PipelineRunner.Create();
        await runner.RunAsync<BranchingPipelineForIdenticalSequences>(ctx);

        // Since the sinks are manually created and registered in the context,
        // we can directly check their Items property
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

        // Create and register the sinks in the context before running the pipeline
        var sink1 = new InMemorySinkNode<int>();
        var sink2 = new InMemorySinkNode<int>();
        context.Items["sink1"] = sink1;
        context.Items["sink2"] = sink2;

        // Act
        var runner = PipelineRunner.Create();
        await runner.RunAsync<BranchingPipeline>(context);

        // Assert
        // Use the GetSink extension method to retrieve the sinks
        // Since both sinks are of the same type, we need to find them by name
        var retrievedSink1 = context.Items["sink1"] as InMemorySinkNode<int>;
        var retrievedSink2 = context.Items["sink2"] as InMemorySinkNode<int>;

        // Both sinks should have been created
        retrievedSink1.Should().NotBeNull();
        retrievedSink2.Should().NotBeNull();

        // Check if both sinks received the data
        retrievedSink1!.Items.Should().BeEquivalentTo(sourceData);
        retrievedSink2!.Items.Should().BeEquivalentTo(sourceData);
    }

    // Custom sink classes to differentiate between the two sinks
    private sealed class BranchTestSink1 : SinkNode<int>
    {
        private readonly List<int> _items = [];

        public IReadOnlyList<int> Items => _items;

        public override async Task ExecuteAsync(
            IDataPipe<int> input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            // Register with our own type name
            context.Items[typeof(BranchTestSink1).FullName!] = this;

            // Execute the logic
            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                _items.Add(item);
            }
        }
    }

    private sealed class BranchTestSink2 : SinkNode<int>
    {
        private readonly List<int> _items = [];

        public IReadOnlyList<int> Items => _items;

        public override async Task ExecuteAsync(
            IDataPipe<int> input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            // Register with our own type name
            context.Items[typeof(BranchTestSink2).FullName!] = this;

            // Execute the logic
            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                _items.Add(item);
            }
        }
    }

    // Test Pipeline Definition
    private sealed class BranchingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", [1, 2, 3]);
            var t = builder.AddTransform<SlowPassThrough, int, int>("t");

            // Connect the source to the transform
            builder.Connect(source, t);

            // Get the sinks that were already registered in the context
            var sink1 = context.Items["sink1"] as InMemorySinkNode<int>;
            var sink2 = context.Items["sink2"] as InMemorySinkNode<int>;

            // Register the preconfigured sinks with the builder
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

    // Test Pipeline Definition specifically for Branch_ShouldDeliverIdenticalSequences test
    private sealed class BranchingPipelineForIdenticalSequences : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", Enumerable.Range(1, 50));
            var t = builder.AddTransform<SlowPassThrough, int, int>("t");

            // Connect the source to the transform
            builder.Connect(source, t);

            // Get the sinks that were already registered in the context
            var sink1 = context.Items["collect1"] as InMemorySinkNode<int>;
            var sink2 = context.Items["collect2"] as InMemorySinkNode<int>;

            // Register the preconfigured sinks with the builder
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

            // (Temporarily) do not set a bounded per-subscriber buffer; bounded channels are disabled internally
            // to avoid deadlock while sinks execute sequentially.
        }
    }

    private sealed class SlowPassThrough : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(2, cancellationToken);
            return item;
        }
    }

    // private sealed class BranchingPipeline : IPipelineDefinition
    // {
    //     public void Define(PipelineBuilder builder, PipelineContext context)
    //     {
    //         var src = builder.AddInMemorySourceWithDataFromContext(context, "src", Enumerable.Range(1, 50));
    //         var t = builder.AddTransform<SlowPassThrough, int, int>("t");
    //
    //         // Connect the source to the transform
    //         builder.Connect(src, t);
    //
    //         // Register the preconfigured sinks with the builder
    //         var sink1 = context.Items["collect1"] as InMemorySinkNode<int>;
    //         var sink2 = context.Items["collect2"] as InMemorySinkNode<int>;
    //
    //         if (sink1 != null)
    //         {
    //             var sink1Handle = builder.AddSink<InMemorySinkNode<int>, int>("s1");
    //             builder.AddPreconfiguredNodeInstance("s1", sink1);
    //             builder.Connect(t, sink1Handle);
    //         }
    //
    //         if (sink2 != null)
    //         {
    //             var sink2Handle = builder.AddSink<InMemorySinkNode<int>, int>("s2");
    //             builder.AddPreconfiguredNodeInstance("s2", sink2);
    //             builder.Connect(t, sink2Handle);
    //         }
    //
    //         // (Temporarily) do not set a bounded per-subscriber buffer; bounded channels are disabled internally
    //         // to avoid deadlock while sinks execute sequentially.
    //     }
    // }
}
