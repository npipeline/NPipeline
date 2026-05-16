using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class AIEnrichNodeTests
{
    [Fact]
    public async Task TransformAsync_EnrichesItemWithAIField()
    {
        var client = FakeChatClient.ThatReturns("""{"label":"Positive","score":0.92}""");

        var node = new AIEnrichNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new AIEnrichOptions<TestDomain.Comment, TestDomain.SentimentResult>(
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
    public async Task TransformAsync_BadJson_ThrowsAITransformException()
    {
        var client = FakeChatClient.ThatReturns("bad json");

        var node = new AIEnrichNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new AIEnrichOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze.",
                c => c.Text,
                (item, _) => item),
        };

        await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None));
    }

    [Fact]
    public async Task TransformAsync_ResultMapperThrows_WrapsInAITransformException()
    {
        var client = FakeChatClient.ThatReturns("""{"label":"Positive","score":0.92}""");

        var node = new AIEnrichNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new AIEnrichOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze sentiment.",
                c => $"Analyze: {c.Text}",
                (_, _) => throw new InvalidOperationException("mapper failed")),
        };

        var ex = await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), Context(), CancellationToken.None));

        Assert.Contains("ResultMapper delegate failed", ex.Message);
        Assert.IsType<InvalidOperationException>(ex.InnerException, exactMatch: false);
    }

    private static PipelineContext Context()
    {
        return new PipelineContext();
    }
}
