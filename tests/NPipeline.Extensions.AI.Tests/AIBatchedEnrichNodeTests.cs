using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class AIBatchedEnrichNodeTests
{
    [Fact]
    public async Task TransformAsync_EnrichesBatchOfItems()
    {
        var client = FakeChatClient.ThatReturns(
            """[{"label":"Positive","score":0.9},{"label":"Negative","score":0.1}]""");

        var node = new AIBatchedEnrichNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new AIBatchedEnrichOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze.",
                batch => "analyze",
                (comment, result) => comment with { Author = $"{comment.Author}({result.Label})" }),
        };

        var batch = new List<TestDomain.Comment>
        {
            new("love it", "alice"),
            new("hate it", "bob"),
        };

        var results = await node.TransformAsync(batch, Context(), CancellationToken.None);

        Assert.Equal(2, results.Count);
        var list = results.ToList();
        Assert.Equal("alice(Positive)", list[0].Author);
        Assert.Equal("bob(Negative)", list[1].Author);
    }

    [Fact]
    public async Task TransformAsync_CountMismatch_ThrowsAITransformException()
    {
        var client = FakeChatClient.ThatReturns(
            """[{"label":"Positive","score":0.9}]""");

        var node = new AIBatchedEnrichNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new AIBatchedEnrichOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze.",
                batch => "analyze",
                (comment, result) => comment with { Author = result.Label }),
        };

        var batch = new List<TestDomain.Comment>
        {
            new("love it", "alice"),
            new("hate it", "bob"),
        };

        var ex = await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(batch, Context(), CancellationToken.None));

        Assert.Contains("count mismatch", ex.Message);
        Assert.Contains("2", ex.Message);
        Assert.Contains("1", ex.Message);
    }

    private static PipelineContext Context()
    {
        return new PipelineContext();
    }
}
