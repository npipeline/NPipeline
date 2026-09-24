using AwesomeAssertions;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.Routing;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Route;

public sealed class RouteNodeIntegrationTests
{
    [Fact]
    public async Task ConnectWhen_ShouldRouteByCondition_IndependentOfSubscriberOrder()
    {
        var context = PipelineContext.CreateDefault();
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
        var context = PipelineContext.CreateDefault();
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
        var context = PipelineContext.CreateDefault();
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
        var context = PipelineContext.CreateDefault();
        var positiveSink = new InMemorySinkNode<int>();
        context.Items["positive"] = positiveSink;

        var runner = PipelineRunner.Create();

        var ex = await Assert.ThrowsAsync<NodeExecutionException>(() => runner.RunAsync<NoMatchThrowPipeline>(context));

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

    [Fact]
    public async Task RouteOutput_JoinReadsTheEdgeItIsBoundTo_SinkConnectedFirst()
    {
        // The "small" output is connected to a sink first, so that edge owns subscriber channel 0.
        // A join connected to the "big" output must see only big orders, not the branch that
        // subscribed first, and must not steal the sink's channel.
        await RunRouteToJoinPipelineAsync(connectSmallSinkFirst: true);
    }

    [Fact]
    public async Task RouteOutput_JoinReadsTheEdgeItIsBoundTo_JoinConnectedFirst()
    {
        // The same graph with the join connected first, covering the other edge ordering.
        await RunRouteToJoinPipelineAsync(connectSmallSinkFirst: false);
    }

    private static async Task RunRouteToJoinPipelineAsync(bool connectSmallSinkFirst)
    {
        var context = PipelineContext.CreateDefault();
        var smallSink = new InMemorySinkNode<RoutedOrder>();
        var joinSink = new InMemorySinkNode<string>();
        context.Items["smallSink"] = smallSink;
        context.Items["joinSink"] = joinSink;

        var runner = PipelineRunner.Create();
        await runner.RunAsync(new RouteToJoinPipeline(connectSmallSinkFirst), context);

        smallSink.Items.Should().BeEquivalentTo([new RoutedOrder(10)], "the small branch must reach its own sink");
        joinSink.Items.Should().BeEquivalentTo(["100:big-customer"], "the join must see only the big branch");
    }

    private sealed class RouteToJoinPipeline(bool connectSmallSinkFirst) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource("src", new[] { new RoutedOrder(10), new RoutedOrder(100) });
            var route = builder.AddRoute<RoutedOrder>("route");

            var smallSink = (InMemorySinkNode<RoutedOrder>)context.Items["smallSink"];
            var joinSink = (InMemorySinkNode<string>)context.Items["joinSink"];

            var smallHandle = builder.AddSink<InMemorySinkNode<RoutedOrder>, RoutedOrder>("small");
            builder.AddPreconfiguredNodeInstance(smallHandle.Id, smallSink);

            var customers = builder.AddInMemorySource("customers", new[] { new Customer(100, "big-customer") });
            var join = builder.AddJoin<AmountJoin, RoutedOrder, Customer, string>("join");
            var joinSinkHandle = builder.AddSink<InMemorySinkNode<string>, string>("join-sink");
            builder.AddPreconfiguredNodeInstance(joinSinkHandle.Id, joinSink);

            builder.Connect(source, route);

            // "small" first (owns channel 0), "big" feeds the join's left input, customers feed the right input.
            builder.ConfigureRoute(route, options =>
            {
                options.When("small", x => x.Amount < 100);
                options.When("big", x => x.Amount >= 100);
            });

            if (connectSmallSinkFirst)
            {
                builder.Connect(route, smallHandle, "small");
                builder.Connect(route, join, "big");
            }
            else
            {
                builder.Connect(route, join, "big");
                builder.Connect(route, smallHandle, "small");
            }

            builder.Connect(customers, join);
            builder.Connect(join, joinSinkHandle);
        }
    }

    public sealed record RoutedOrder(int Amount);

    public sealed record Customer(int CustomerId, string Name);

    [KeySelector(typeof(RoutedOrder), nameof(RoutedOrder.Amount))]
    [KeySelector(typeof(Customer), nameof(Customer.CustomerId))]
    private sealed class AmountJoin : KeyedJoinNode<int, RoutedOrder, Customer, string>
    {
        public override string CreateOutput(RoutedOrder order, Customer customer) => $"{order.Amount}:{customer.Name}";
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

            builder.ConfigureRoute(route, options => { options.WithNoMatchBehavior(NoRouteMatchBehavior.Throw); });

            builder.Connect(source, route);
            builder.ConnectWhen(route, positiveHandle, x => x > 0, "positive");
        }
    }
}
