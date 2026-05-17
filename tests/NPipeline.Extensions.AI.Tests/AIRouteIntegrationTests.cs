using Microsoft.Extensions.AI;
using NPipeline.DataFlow.Routing;
using NPipeline.Execution;
using NPipeline.Extensions.AI.Routing;
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class AIRouteIntegrationTests
{
    private const string ClientKey = "chatClient";

    private sealed class AIPositiveRoutePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var client = (IChatClient)context.Items[ClientKey];
            var source = builder.AddInMemorySource("src", [new TestDomain.Comment("love it", "alice")]);

            var positiveSink = (InMemorySinkNode<TestDomain.Comment>)context.Items["positive"];
            var negativeSink = (InMemorySinkNode<TestDomain.Comment>)context.Items["negative"];
            var fallbackSink = (InMemorySinkNode<TestDomain.Comment>)context.Items["fallback"];

            var posHandle = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("positive");
            var negHandle = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("negative");
            var fallHandle = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("fallback");
            builder.AddPreconfiguredNodeInstance(posHandle.Id, positiveSink);
            builder.AddPreconfiguredNodeInstance(negHandle.Id, negativeSink);
            builder.AddPreconfiguredNodeInstance(fallHandle.Id, fallbackSink);

            builder.Connect(source,
                builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(client, opts => opts
                        .WithSystemPrompt("Classify sentiment.")
                        .WithItemTemplate(c => $"Analyze: {c.Text}")
                        .WithResultMapper((c, r) => c with { Author = r.Label }))
                    .When(c => c.Author == "positive", posHandle)
                    .When(c => c.Author == "negative", negHandle)
                    .Otherwise(fallHandle));
        }
    }

    [Fact]
    public async Task RoutesItemToCorrectSink_BasedOnLLMClassification()
    {
        var client = FakeChatClient.ThatReturns("""{"label":"positive","score":0.9}""");
        var positiveSink = new InMemorySinkNode<TestDomain.Comment>();
        var negativeSink = new InMemorySinkNode<TestDomain.Comment>();
        var fallbackSink = new InMemorySinkNode<TestDomain.Comment>();

        var context = PipelineContext.Default;
        context.Items[ClientKey] = client;
        context.Items["positive"] = positiveSink;
        context.Items["negative"] = negativeSink;
        context.Items["fallback"] = fallbackSink;

        await PipelineRunner.Create().RunAsync<AIPositiveRoutePipeline>(context);

        Assert.Single(positiveSink.Items);
        Assert.Empty(negativeSink.Items);
        Assert.Empty(fallbackSink.Items);
        Assert.Equal("positive", positiveSink.Items[0].Author);
    }

    private sealed class AINeutralRoutePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var client = (IChatClient)context.Items[ClientKey];
            var source = builder.AddInMemorySource("src", [new TestDomain.Comment("hello", "alice")]);

            var positiveSink = (InMemorySinkNode<TestDomain.Comment>)context.Items["positive"];
            var fallbackSink = (InMemorySinkNode<TestDomain.Comment>)context.Items["fallback"];

            var posHandle = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("positive");
            var fallHandle = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("fallback");
            builder.AddPreconfiguredNodeInstance(posHandle.Id, positiveSink);
            builder.AddPreconfiguredNodeInstance(fallHandle.Id, fallbackSink);

            builder.Connect(source,
                builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(client, opts => opts
                        .WithSystemPrompt("Classify.")
                        .WithItemTemplate(c => c.Text)
                        .WithResultMapper((c, r) => c with { Author = r.Label }))
                    .When(c => c.Author == "positive", posHandle)
                    .Otherwise(fallHandle));
        }
    }

    [Fact]
    public async Task UnmatchedItem_GoesToOtherwise()
    {
        var client = FakeChatClient.ThatReturns("""{"label":"neutral","score":0.5}""");
        var positiveSink = new InMemorySinkNode<TestDomain.Comment>();
        var fallbackSink = new InMemorySinkNode<TestDomain.Comment>();

        var context = PipelineContext.Default;
        context.Items[ClientKey] = client;
        context.Items["positive"] = positiveSink;
        context.Items["fallback"] = fallbackSink;

        await PipelineRunner.Create().RunAsync<AINeutralRoutePipeline>(context);

        Assert.Empty(positiveSink.Items);
        Assert.Single(fallbackSink.Items);
    }

    private sealed class AIFirstMatchRoutePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var client = (IChatClient)context.Items[ClientKey];
            var source = builder.AddInMemorySource("src", [new TestDomain.Comment("urgent task", "alice")]);

            var allSink = (InMemorySinkNode<TestDomain.Comment>)context.Items["all"];
            var urgentSink = (InMemorySinkNode<TestDomain.Comment>)context.Items["urgent"];

            var allHandle = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("all");
            var urgHandle = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("urgent");
            builder.AddPreconfiguredNodeInstance(allHandle.Id, allSink);
            builder.AddPreconfiguredNodeInstance(urgHandle.Id, urgentSink);

            builder.Connect(source,
                builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(client, opts => opts
                        .WithSystemPrompt("Classify.")
                        .WithItemTemplate(c => c.Text)
                        .WithResultMapper((c, r) => c with { Author = r.Label }))
                    .When(_ => true, allHandle)
                    .When(c => c.Author == "urgent", urgHandle));
        }
    }

    [Fact]
    public async Task FirstMatchMode_OnlyRoutesToFirstMatchingBranch()
    {
        var client = FakeChatClient.ThatReturns("""{"label":"urgent","score":0.95}""");
        var allSink = new InMemorySinkNode<TestDomain.Comment>();
        var urgentSink = new InMemorySinkNode<TestDomain.Comment>();

        var context = PipelineContext.Default;
        context.Items[ClientKey] = client;
        context.Items["all"] = allSink;
        context.Items["urgent"] = urgentSink;

        await PipelineRunner.Create().RunAsync<AIFirstMatchRoutePipeline>(context);

        Assert.Single(allSink.Items);
        Assert.Empty(urgentSink.Items);
    }

    private sealed class AIAllMatchesRoutePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var client = (IChatClient)context.Items[ClientKey];
            var source = builder.AddInMemorySource("src", [new TestDomain.Comment("hello", "alice")]);

            var sinkA = (InMemorySinkNode<TestDomain.Comment>)context.Items["sinkA"];
            var sinkB = (InMemorySinkNode<TestDomain.Comment>)context.Items["sinkB"];

            var handleA = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("sinkA");
            var handleB = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("sinkB");
            builder.AddPreconfiguredNodeInstance(handleA.Id, sinkA);
            builder.AddPreconfiguredNodeInstance(handleB.Id, sinkB);

            builder.Connect(source,
                builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(client, opts => opts
                        .WithSystemPrompt("Classify.")
                        .WithItemTemplate(c => c.Text)
                        .WithResultMapper((c, r) => c))
                    .WithMatchMode(RouteMatchMode.AllMatches)
                    .When(_ => true, handleA)
                    .When(_ => true, handleB));
        }
    }

    [Fact]
    public async Task AllMatches_Mode_DeliversToAllMatchingBranches()
    {
        var client = FakeChatClient.ThatReturns("""{"label":"irrelevant","score":0.5}""");
        var sinkA = new InMemorySinkNode<TestDomain.Comment>();
        var sinkB = new InMemorySinkNode<TestDomain.Comment>();

        var context = PipelineContext.Default;
        context.Items[ClientKey] = client;
        context.Items["sinkA"] = sinkA;
        context.Items["sinkB"] = sinkB;

        await PipelineRunner.Create().RunAsync<AIAllMatchesRoutePipeline>(context);

        Assert.Single(sinkA.Items);
        Assert.Single(sinkB.Items);
    }

    [Fact]
    public void NullGuards()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("{}");

        Assert.Throws<ArgumentNullException>(() =>
            ((PipelineBuilder)null!).AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(client, _ => { }));
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(client, null!));
    }

    [Fact]
    public void When_PredicateNull_Throws()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("{}");
        var sink = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("s");

        var route = builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(client, opts => opts
            .WithSystemPrompt("X")
            .WithItemTemplate(c => c.Text)
            .WithResultMapper((c, r) => c));

        Assert.Throws<ArgumentNullException>(() => route.When(null!, sink));
    }

    [Fact]
    public void Otherwise_TargetNull_Throws()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("{}");

        var route = builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(client, opts => opts
            .WithSystemPrompt("X")
            .WithItemTemplate(c => c.Text)
            .WithResultMapper((c, r) => c));

        Assert.Throws<ArgumentNullException>(() => route.Otherwise(null!));
    }
}
