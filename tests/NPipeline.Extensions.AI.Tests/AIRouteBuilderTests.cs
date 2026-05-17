using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Routing;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class AIRouteBuilderTests
{
    private readonly PipelineBuilder _builder = new();
    private readonly IChatClient _client = FakeChatClient.ThatReturns("""{"label":"X","score":0.5}""");

    [Fact]
    public void When_DoesNotThrow()
    {
        var sink = _builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("sink1");

        var routeBuilder = _builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(_client, opts => opts
            .WithSystemPrompt("Classify.")
            .WithItemTemplate(c => c.Text)
            .WithResultMapper((c, r) => c));

        routeBuilder.When(c => c.Text == "hello", sink);

        Assert.NotNull(routeBuilder.RouteHandle);
    }

    [Fact]
    public void Otherwise_DoesNotThrow()
    {
        var sink = _builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("fallback");

        var routeBuilder = _builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(_client, opts => opts
            .WithSystemPrompt("Classify.")
            .WithItemTemplate(c => c.Text)
            .WithResultMapper((c, r) => c));

        routeBuilder.Otherwise(sink);

        Assert.NotNull(routeBuilder.RouteHandle);
    }

    [Fact]
    public void Chaining_ReturnsSameBuilder()
    {
        var sink1 = _builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("s1");
        var sink2 = _builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("s2");

        var routeBuilder = _builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(_client, opts => opts
            .WithSystemPrompt("Classify.")
            .WithItemTemplate(c => c.Text)
            .WithResultMapper((c, r) => c));

        var result = routeBuilder
            .When(c => c.Text == "hello", sink1)
            .Otherwise(sink2);

        Assert.Same(routeBuilder, result);
    }

    [Fact]
    public void RouteHandle_ExposesUnderlyingHandle()
    {
        var routeBuilder = _builder.AddAIRoute<TestDomain.Comment, TestDomain.SentimentResult>(_client, opts => opts
            .WithSystemPrompt("Classify.")
            .WithItemTemplate(c => c.Text)
            .WithResultMapper((c, r) => c));

        Assert.NotNull(routeBuilder.RouteHandle);
        Assert.Contains("route", routeBuilder.RouteHandle.Id, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AddAIBatchedStreamRoute_ReturnsBuilder()
    {
        var sink = _builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("sink1");

        var routeBuilder = _builder.AddAIBatchedStreamRoute<TestDomain.Comment, TestDomain.SentimentResult>(_client, opts => opts
            .WithSystemPrompt("Classify.")
            .WithBatchTemplate(batch => "classify")
            .WithResultMapper((c, r) => c)
            .WithBatchSize(5));

        routeBuilder.When(c => c.Text == "test", sink);

        Assert.NotNull(routeBuilder.RouteHandle);
    }

    [Fact]
    public void Constructor_NullBuilder_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new AIRouteBuilder<TestDomain.Comment>(null!, new TransformNodeHandle<TestDomain.Comment, TestDomain.Comment>("e"), new TransformNodeHandle<TestDomain.Comment, TestDomain.Comment>("r")));
    }

    [Fact]
    public void Constructor_NullRouteHandle_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new AIRouteBuilder<TestDomain.Comment>(new PipelineBuilder(), new TransformNodeHandle<TestDomain.Comment, TestDomain.Comment>("e"), null!));
    }
}
