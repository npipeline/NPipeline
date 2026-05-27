using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class TestDomain
{
    public record Comment(string Text, string Author);

    public record ClassificationResult(string Category, float Confidence);

    public record SentimentResult(string Label, float Score);
}

public class AITransformNodeTests
{
    [Fact]
    public async Task TransformAsync_ReturnsTypedResult_FromValidJson()
    {
        var client = FakeChatClient.ThatReturns("""{"category":"Greeting","confidence":0.95}""");
        var node = CreateNode(client);

        var result = await node.TransformAsync(
            new TestDomain.Comment("hello world", "alice"), Context(), CancellationToken.None);

        Assert.Equal("Greeting", result.Category);
        Assert.Equal(0.95f, result.Confidence);
    }

    [Fact]
    public async Task TransformAsync_QuotedNumericValue_DeserializesFloat()
    {
        var client = FakeChatClient.ThatReturns("""{"category":"Greeting","confidence":"0.95"}""");
        var node = CreateNode(client);

        var result = await node.TransformAsync(
            new TestDomain.Comment("hello world", "alice"), Context(), CancellationToken.None);

        Assert.Equal("Greeting", result.Category);
        Assert.Equal(0.95f, result.Confidence);
    }

    [Fact]
    public async Task TransformAsync_SendsSystemAndUserMessages()
    {
        string? capturedSystem = null;
        string? capturedUser = null;

        var client = new FakeChatClient((messages, _, _) =>
        {
            var msgList = messages.ToList();
            capturedSystem = msgList.First(m => m.Role == ChatRole.System).Text;
            capturedUser = msgList.First(m => m.Role == ChatRole.User).Text;

            return Task.FromResult(new ChatResponse(
                new ChatMessage(ChatRole.Assistant, """{"category":"X","confidence":0.5}""")));
        });

        var node = CreateNode(client);

        await node.TransformAsync(new TestDomain.Comment("hello", "bob"), Context(), CancellationToken.None);

        Assert.Equal("Classify: hello", capturedUser);
        Assert.Equal("You are a classifier.", capturedSystem);
    }

    [Fact]
    public async Task TransformAsync_InvalidJson_ThrowsAITransformException()
    {
        var client = FakeChatClient.ThatReturns("not json at all");
        var node = CreateNode(client);

        var ex = await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None));

        Assert.Contains("Failed to deserialize", ex.Message);
        Assert.NotNull(ex.OriginalItem);
        Assert.Equal("not json at all", ex.RawResponse);
        Assert.Equal("AI_TRANSFORM_ERROR", ex.ErrorCode);
    }

    [Fact]
    public async Task TransformAsync_JsonNullLiteral_ThrowsAITransformException()
    {
        var client = FakeChatClient.ThatReturns("null");
        var node = CreateNode(client);

        var ex = await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None));

        Assert.Contains("null value", ex.Message);
    }

    [Fact]
    public async Task TransformAsync_CapturesModelId()
    {
        var client = FakeChatClient.ThatReturns(
            """{"category":"X","confidence":0.5}""", "gpt-4o-mini");

        var node = CreateNode(client);

        var result = await node.TransformAsync(
            new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None);

        Assert.Equal("X", result.Category);
    }

    [Fact]
    public async Task TransformAsync_InfrastructureException_PropagatesUnwrapped()
    {
        var client = FakeChatClient.ThatThrows(new HttpRequestException("network error"));
        var node = CreateNode(client);

        await Assert.ThrowsAsync<HttpRequestException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None));
    }

    [Fact]
    public async Task TransformAsync_Timeout_PropagatesUnwrapped()
    {
        var client = FakeChatClient.ThatThrows(new TimeoutException("timeout"));
        var node = CreateNode(client);

        await Assert.ThrowsAsync<TimeoutException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None));
    }

    [Fact]
    public async Task TransformAsync_ItemTemplateThrows_WrapsInAITransformException()
    {
        var client = FakeChatClient.ThatReturns("""{"category":"X","confidence":0.5}""");

        var node = new AITransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AITransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "You are a classifier.",
                _ => throw new InvalidOperationException("template failed")),
        };

        var ex = await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None));

        Assert.Contains("ItemTemplate delegate failed", ex.Message);
        Assert.IsType<InvalidOperationException>(ex.InnerException, false);
    }

    [Fact]
    public async Task TransformAsync_ConfigureOptionsThrows_WrapsInAITransformException()
    {
        var client = FakeChatClient.ThatReturns("""{"category":"X","confidence":0.5}""");

        var node = new AITransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AITransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "You are a classifier.",
                c => $"Classify: {c.Text}",
                ConfigureOptions: _ => throw new InvalidOperationException("configure failed")),
        };

        var ex = await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None));

        Assert.Contains("ConfigureOptions delegate failed", ex.Message);
        Assert.Equal("Classify: hello", ex.PromptSent);
        Assert.IsType<InvalidOperationException>(ex.InnerException, false);
    }

    [Fact]
    public async Task TransformAsync_MissingRequiredOptions_ThrowsInvalidOperationException()
    {
        var client = FakeChatClient.ThatReturns("""{"category":"X","confidence":0.5}""");
        var node = new AITransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None));

        Assert.Contains("SystemPrompt", ex.Message);
    }

    [Fact]
    public async Task TransformAsync_ConstructorNullGuard()
    {
        Assert.Throws<ArgumentNullException>(() => new AITransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(null!));
    }

    private static AITransformNode<TestDomain.Comment, TestDomain.ClassificationResult> CreateNode(IChatClient client)
    {
        return new AITransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AITransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "You are a classifier.",
                c => $"Classify: {c.Text}"),
        };
    }

    private static PipelineContext Context()
    {
        return new PipelineContext();
    }
}
