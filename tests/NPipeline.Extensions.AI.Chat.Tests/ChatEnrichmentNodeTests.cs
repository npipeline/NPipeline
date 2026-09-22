using NPipeline.Extensions.AI.Chat.Configuration;
using NPipeline.Extensions.AI.Chat.Exceptions;
using NPipeline.Extensions.AI.Chat.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Chat.Tests;

public class ChatEnrichmentNodeTests
{
    [Fact]
    public async Task TransformAsync_EnrichesItemWithAIField()
    {
        var client = FakeChatClient.ThatReturns("""{"label":"Positive","score":0.92}""");

        var node = new ChatEnrichmentNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new ChatEnrichmentOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze sentiment.",
                c => $"Analyze: {c.Text}",
                (comment, result) => comment with { Author = $"{comment.Author}({result.Label})" }),
        };

        var result = await node.TransformAsync(
            new TestDomain.Comment("I love this!", "alice"), Context(), CancellationToken.None);

        Assert.Equal("I love this!", result.Text);
        Assert.Equal("alice(Positive)", result.Author);
    }

    [Fact]
    public async Task TransformAsync_BadJson_ThrowsChatTransformException()
    {
        var client = FakeChatClient.ThatReturns("bad json");

        var node = new ChatEnrichmentNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new ChatEnrichmentOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze.",
                c => c.Text,
                (item, _) => item),
        };

        await Assert.ThrowsAsync<ChatTransformException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task TransformAsync_ResultMapperThrows_WrapsInChatTransformException()
    {
        var client = FakeChatClient.ThatReturns("""{"label":"Positive","score":0.92}""");

        var node = new ChatEnrichmentNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new ChatEnrichmentOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze sentiment.",
                c => $"Analyze: {c.Text}",
                (_, _) => throw new InvalidOperationException("mapper failed")),
        };

        var ex = await Assert.ThrowsAsync<ChatTransformException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None).AsTask());

        Assert.Contains("ResultMapper delegate failed", ex.Message);
        Assert.IsType<InvalidOperationException>(ex.InnerException, false);
    }

    private static PipelineContext Context()
    {
        return new PipelineContext();
    }
}
