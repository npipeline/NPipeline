using NPipeline.DataFlow.Routing;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Decisions.Tests;

public sealed class AIRouteTests
{
    [Fact]
    public async Task Route_PreservesOriginalItemReference()
    {
        var item = new TestItem("billing");
        var selected = new InMemorySinkNode<TestItem>();
        var fallback = new InMemorySinkNode<TestItem>();
        var context = CreateContext(
            item,
            Classification(RouteLabel.Billing, 0.9, (RouteLabel.Billing, 0.9), (RouteLabel.Technical, 0.1)),
            selected,
            fallback);

        await PipelineRunner.Create().RunAsync<LabelRoutePipeline>(context);

        var routed = Assert.Single(selected.Items);
        Assert.Same(item, routed);
        Assert.Empty(fallback.Items);
    }

    [Fact]
    public async Task Route_LowConfidence_UsesOtherwise()
    {
        var item = new TestItem("ambiguous");
        var selected = new InMemorySinkNode<TestItem>();
        var fallback = new InMemorySinkNode<TestItem>();
        var context = CreateContext(
            item,
            Classification(RouteLabel.Billing, 0.4, (RouteLabel.Billing, 0.6), (RouteLabel.Technical, 0.4)),
            selected,
            fallback);

        await PipelineRunner.Create().RunAsync<LabelRoutePipeline>(context);

        Assert.Empty(selected.Items);
        Assert.Same(item, Assert.Single(fallback.Items));
    }

    [Fact]
    public async Task Route_AllMatches_CanUseNonWinningProbability()
    {
        var item = new TestItem("mixed");
        var primary = new InMemorySinkNode<TestItem>();
        var secondary = new InMemorySinkNode<TestItem>();
        var context = PipelineContext.CreateDefault();
        context.Items["item"] = item;
        context.Items["classifier"] = new FakeClassifier(
            Classification(RouteLabel.Billing, 0.55, (RouteLabel.Billing, 0.65), (RouteLabel.Technical, 0.35)));
        context.Items["selected"] = primary;
        context.Items["fallback"] = secondary;

        await PipelineRunner.Create().RunAsync<ProbabilityRoutePipeline>(context);

        Assert.Same(item, Assert.Single(primary.Items));
        Assert.Same(item, Assert.Single(secondary.Items));
    }

    [Fact]
    public async Task Route_FirstMatch_OnlyUsesFirstMatchingBranch()
    {
        var item = new TestItem("mixed");
        var first = new InMemorySinkNode<TestItem>();
        var second = new InMemorySinkNode<TestItem>();
        var context = PipelineContext.CreateDefault();
        context.Items["item"] = item;
        context.Items["classifier"] = new FakeClassifier(
            Classification(RouteLabel.Billing, 0.8, (RouteLabel.Billing, 0.8), (RouteLabel.Technical, 0.2)));
        context.Items["selected"] = first;
        context.Items["fallback"] = second;

        await PipelineRunner.Create().RunAsync<FirstMatchPipeline>(context);

        Assert.Single(first.Items);
        Assert.Empty(second.Items);
    }

    [Theory]
    [InlineData(-0.01)]
    [InlineData(1.01)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void WhenLabel_InvalidThreshold_Throws(double threshold)
    {
        var builder = new PipelineBuilder();
        var target = builder.AddSink<InMemorySinkNode<TestItem>, TestItem>("sink");
        var route = builder.AddAIRoute<TestItem, RouteLabel>(new FakeClassifier(Classification(RouteLabel.Billing, 1)));

        Assert.Throws<ArgumentOutOfRangeException>(() => route.WhenLabel(RouteLabel.Billing, target, threshold));
    }

    [Fact]
    public async Task ClassificationNode_ForwardsCancellationToken()
    {
        CancellationToken observed = default;
        var classifier = new DelegateClassifier((_, cancellationToken) =>
        {
            observed = cancellationToken;
            return ValueTask.FromResult(Classification(RouteLabel.Billing, 1));
        });
        var node = new AIClassificationNode<TestItem, RouteLabel>(classifier);
        using var cancellation = new CancellationTokenSource();

        await node.TransformAsync(new TestItem("x"), PipelineContext.CreateDefault(), cancellation.Token);

        Assert.Equal(cancellation.Token, observed);
    }

    private static PipelineContext CreateContext(
        TestItem item,
        AIClassification<RouteLabel> classification,
        InMemorySinkNode<TestItem> selected,
        InMemorySinkNode<TestItem> fallback)
    {
        var context = PipelineContext.CreateDefault();
        context.Items["item"] = item;
        context.Items["classifier"] = new FakeClassifier(classification);
        context.Items["selected"] = selected;
        context.Items["fallback"] = fallback;
        return context;
    }

    private static AIClassification<RouteLabel> Classification(
        RouteLabel label,
        double confidence,
        params (RouteLabel Label, double Probability)[] probabilities)
    {
        var distribution = probabilities.Length == 0
            ? new Dictionary<RouteLabel, double> { [label] = 1 }
            : probabilities.ToDictionary(entry => entry.Label, entry => entry.Probability);
        return new AIClassification<RouteLabel>(
            label,
            confidence,
            distribution,
            new AIInvocationMetadata("test", "fake-model"));
    }

    private static void AddCommonNodes(
        PipelineBuilder builder,
        PipelineContext context,
        out InMemorySinkNode<TestItem> selected,
        out InMemorySinkNode<TestItem> fallback,
        out NPipeline.Graph.SourceNodeHandle<TestItem> source,
        out NPipeline.Graph.SinkNodeHandle<TestItem> selectedHandle,
        out NPipeline.Graph.SinkNodeHandle<TestItem> fallbackHandle)
    {
        selected = (InMemorySinkNode<TestItem>)context.Items["selected"];
        fallback = (InMemorySinkNode<TestItem>)context.Items["fallback"];
        source = builder.AddInMemorySource("source", [(TestItem)context.Items["item"]]);
        selectedHandle = builder.AddSink<InMemorySinkNode<TestItem>, TestItem>("selected");
        fallbackHandle = builder.AddSink<InMemorySinkNode<TestItem>, TestItem>("fallback");
        builder.AddPreconfiguredNodeInstance(selectedHandle.Id, selected);
        builder.AddPreconfiguredNodeInstance(fallbackHandle.Id, fallback);
    }

    private sealed class LabelRoutePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            AddCommonNodes(builder, context, out _, out _, out var source, out var selected, out var fallback);
            var classifier = (IAIClassifier<TestItem, RouteLabel>)context.Items["classifier"];
            var route = builder.AddAIRoute(classifier, "decision")
                .WhenLabel(RouteLabel.Billing, selected, minimumConfidence: 0.75)
                .Otherwise(fallback);
            builder.Connect(source, route);
        }
    }

    private sealed class ProbabilityRoutePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            AddCommonNodes(builder, context, out _, out _, out var source, out var primary, out var secondary);
            var classifier = (IAIClassifier<TestItem, RouteLabel>)context.Items["classifier"];
            var route = builder.AddAIRoute(classifier, "decision")
                .WithMatchMode(RouteMatchMode.AllMatches)
                .WhenLabel(RouteLabel.Billing, primary, minimumConfidence: 0.5)
                .WhenProbability(RouteLabel.Technical, 0.25, secondary);
            builder.Connect(source, route);
        }
    }

    private sealed class FirstMatchPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            AddCommonNodes(builder, context, out _, out _, out var source, out var first, out var second);
            var classifier = (IAIClassifier<TestItem, RouteLabel>)context.Items["classifier"];
            var route = builder.AddAIRoute(classifier, "decision")
                .When(_ => true, first)
                .When(_ => true, second);
            builder.Connect(source, route);
        }
    }

    private sealed class FakeClassifier(AIClassification<RouteLabel> classification) : IAIClassifier<TestItem, RouteLabel>
    {
        public ValueTask<AIClassification<RouteLabel>> ClassifyAsync(TestItem input, CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(classification);
        }
    }

    private sealed class DelegateClassifier(
        Func<TestItem, CancellationToken, ValueTask<AIClassification<RouteLabel>>> classify)
        : IAIClassifier<TestItem, RouteLabel>
    {
        public ValueTask<AIClassification<RouteLabel>> ClassifyAsync(TestItem input, CancellationToken cancellationToken = default)
        {
            return classify(input, cancellationToken);
        }
    }

    private sealed record TestItem(string Text);

    private enum RouteLabel
    {
        Billing,
        Technical,
    }
}