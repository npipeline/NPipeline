using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class AIBatchedStreamEnrichNodeTests
{
    [Fact]
    public async Task TransformAsync_EnrichesAndFansOut()
    {
        var batchResponses = new Queue<string>(new[]
        {
            """[{"label":"Positive","score":0.9},{"label":"Negative","score":0.1}]""",
            """[{"label":"Neutral","score":0.5}]""",
        });

        var client = new FakeChatClient((_, _, _) =>
            Task.FromResult(new ChatResponse(
                new ChatMessage(ChatRole.Assistant, batchResponses.Dequeue()))));

        var node = new AIBatchedStreamEnrichNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new AIBatchedStreamEnrichOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze.",
                batch => "analyze",
                (comment, result) => comment with { Author = $"{comment.Author}({result.Label})" },
                BatchSize: 2),
        };

        var items = new List<TestDomain.Comment>
        {
            new("love it", "alice"),
            new("hate it", "bob"),
            new("wow", "charlie"),
        };

        var results = new List<TestDomain.Comment>();

        await foreach (var result in node.TransformAsync(items.ToAsyncEnumerable(), Context(), CancellationToken.None))
        {
            results.Add(result);
        }

        Assert.Equal(3, results.Count);
    }

    private static PipelineContext Context()
    {
        return new PipelineContext();
    }
}
