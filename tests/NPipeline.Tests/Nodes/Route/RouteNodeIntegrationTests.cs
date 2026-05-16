using AwesomeAssertions;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.Routing;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Route;

public sealed class RouteNodeIntegrationTests
{
    [Fact]
    public async Task ConnectWhen_ShouldRouteByCondition_IndependentOfSubscriberOrder()
    {
        var context = PipelineContext.Default;
        var oddSink = new InMemorySinkNode<int>();
        var evenSink = new InMemorySinkNode<int>();
        context.Items["odd"] = oddSink;
        context.Items["even"] = evenSink;

        var runner = PipelineRunner.Create();
        await runner.RunAsync<ConditionalRoutingPipeline>(context);

        oddSink.Items.Should().BeEquivalentTo([1, 3, 5]);
        evenSink.Items.Should().BeEquivalentTo([2, 4, 6]);

        var metrics = context.GetBranchMetrics("route");
        metrics.Should().NotBeNull();
        metrics!.SubscriberCount.Should().Be(2);
        metrics.SubscribersCompleted.Should().Be(2);
        metrics.Faulted.Should().Be(0);
    }

    [Fact]
    public async Task ConnectOtherwise_ShouldReceiveUnmatchedItems()
    {
        var context = PipelineContext.Default;
        var positiveSink = new InMemorySinkNode<int>();
        var fallbackSink = new InMemorySinkNode<int>();
        context.Items["positive"] = positiveSink;
        context.Items["fallback"] = fallbackSink;

        var runner = PipelineRunner.Create();
        await runner.RunAsync<OtherwiseRoutingPipeline>(context);

        positiveSink.Items.Should().BeEquivalentTo([3, 4]);
        fallbackSink.Items.Should().BeEquivalentTo([1, 2]);
    }

    [Fact]
    public async Task AllMatches_Mode_ShouldDeliverToEveryMatchingRoute()
    {
        var context = PipelineContext.Default;
        var evenSink = new InMemorySinkNode<int>();
        var gteTwoSink = new InMemorySinkNode<int>();
        var fallbackSink = new InMemorySinkNode<int>();

        context.Items["even"] = evenSink;
        context.Items["gteTwo"] = gteTwoSink;
        context.Items["fallback"] = fallbackSink;

        var runner = PipelineRunner.Create();
        await runner.RunAsync<AllMatchesRoutingPipeline>(context);

        evenSink.Items.Should().BeEquivalentTo([2]);
        gteTwoSink.Items.Should().BeEquivalentTo([2, 3]);
        fallbackSink.Items.Should().BeEquivalentTo([1]);
    }

    [Fact]
    public async Task NoMatchBehaviorThrow_WithoutOtherwise_ShouldFailPipeline()
    {
        var context = PipelineContext.Default;
        var positiveSink = new InMemorySinkNode<int>();
        context.Items["positive"] = positiveSink;

        var runner = PipelineRunner.Create();

        var ex = await Assert.ThrowsAsync<NodeExecutionException>(
            () => runner.RunAsync<NoMatchThrowPipeline>(context));

        ex.InnerException.Should().NotBeNull();
        ex.InnerException!.Message.Should().Contain("No route rule matched an item");
    }

    [Fact]
    public void ConnectWhen_WithNonRouteNode_ShouldThrow()
    {
        var builder = new PipelineBuilder();
        var transform = builder.AddPassThroughTransform<int, int>("pass");
        var sink = builder.AddSink<InMemorySinkNode<int>, int>("sink");

        Action act = () => builder.ConnectWhen(transform, sink, _ => true);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*requires a 'Route' node*");
    }

    private sealed class ConditionalRoutingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource("src", Enumerable.Range(1, 6));
            var route = builder.AddRoute<int>("route");

            var oddSink = (InMemorySinkNode<int>)context.Items["odd"];
            var evenSink = (InMemorySinkNode<int>)context.Items["even"];

            // Add downstream nodes in this order so execution subscription order differs from edge registration order.
            var oddHandle = builder.AddSink<InMemorySinkNode<int>, int>("odd");
            var evenHandle = builder.AddSink<InMemorySinkNode<int>, int>("even");

            builder.AddPreconfiguredNodeInstance(oddHandle.Id, oddSink);
            builder.AddPreconfiguredNodeInstance(evenHandle.Id, evenSink);

            builder.Connect(source, route);

            // Connect even first, odd second to validate routing is bound to edge metadata, not subscriber index.
            builder.ConnectWhen(route, evenHandle, x => x % 2 == 0, "even");
            builder.ConnectWhen(route, oddHandle, x => x % 2 != 0, "odd");
        }
    }

    private sealed class OtherwiseRoutingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource("src", [1, 2, 3, 4]);
            var route = builder.AddRoute<int>("route");

            var positiveSink = (InMemorySinkNode<int>)context.Items["positive"];
            var fallbackSink = (InMemorySinkNode<int>)context.Items["fallback"];

            var positiveHandle = builder.AddSink<InMemorySinkNode<int>, int>("positive");
            var fallbackHandle = builder.AddSink<InMemorySinkNode<int>, int>("fallback");

            builder.AddPreconfiguredNodeInstance(positiveHandle.Id, positiveSink);
            builder.AddPreconfiguredNodeInstance(fallbackHandle.Id, fallbackSink);

            builder.Connect(source, route);
            builder.ConnectWhen(route, positiveHandle, x => x > 2, "gt2");
            builder.ConnectOtherwise(route, fallbackHandle);
        }
    }

    private sealed class AllMatchesRoutingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource("src", [1, 2, 3]);
            var route = builder.AddRoute<int>(opts => opts.WithMatchMode(RouteMatchMode.AllMatches), "route");

            var evenSink = (InMemorySinkNode<int>)context.Items["even"];
            var gteTwoSink = (InMemorySinkNode<int>)context.Items["gteTwo"];
            var fallbackSink = (InMemorySinkNode<int>)context.Items["fallback"];

            var evenHandle = builder.AddSink<InMemorySinkNode<int>, int>("even");
            var gteTwoHandle = builder.AddSink<InMemorySinkNode<int>, int>("gteTwo");
            var fallbackHandle = builder.AddSink<InMemorySinkNode<int>, int>("fallback");

            builder.AddPreconfiguredNodeInstance(evenHandle.Id, evenSink);
            builder.AddPreconfiguredNodeInstance(gteTwoHandle.Id, gteTwoSink);
            builder.AddPreconfiguredNodeInstance(fallbackHandle.Id, fallbackSink);

            builder.Connect(source, route);
            builder.ConnectWhen(route, evenHandle, x => x % 2 == 0, "even");
            builder.ConnectWhen(route, gteTwoHandle, x => x >= 2, "gte-two");
            builder.ConnectOtherwise(route, fallbackHandle, "fallback");
        }
    }

    private sealed class NoMatchThrowPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource("src", [1, -1]);
            var route = builder.AddRoute<int>("route");

            var positiveSink = (InMemorySinkNode<int>)context.Items["positive"];
            var positiveHandle = builder.AddSink<InMemorySinkNode<int>, int>("positive");
            builder.AddPreconfiguredNodeInstance(positiveHandle.Id, positiveSink);

            builder.ConfigureRoute(route, options =>
            {
                options.WithNoMatchBehavior(NoRouteMatchBehavior.Throw);
            });

            builder.Connect(source, route);
            builder.ConnectWhen(route, positiveHandle, x => x > 0, "positive");
        }
    }
}
