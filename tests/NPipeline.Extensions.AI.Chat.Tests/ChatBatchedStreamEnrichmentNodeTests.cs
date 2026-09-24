using Microsoft.Extensions.AI;
using NPipeline.Execution;
using NPipeline.Extensions.AI.Chat.Configuration;
using NPipeline.Extensions.AI.Chat.Exceptions;
using NPipeline.Extensions.AI.Chat.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Chat.Tests;

public class ChatBatchedStreamEnrichmentNodeTests
{
    [Fact]
    public void DefaultExecutionStrategy_IsStreamCapable()
    {
        var node = new ChatBatchedStreamEnrichmentNode<TestDomain.Comment, TestDomain.SentimentResult>(FakeChatClient.ThatReturns("[]"));

        _ = Assert.IsType<IStreamExecutionStrategy>(node.DefaultExecutionStrategy, false);
    }

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

        var node = new ChatBatchedStreamEnrichmentNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new ChatBatchedStreamEnrichmentOptions<TestDomain.Comment, TestDomain.SentimentResult>(
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

    [Fact]
    public async Task TransformAsync_ResultMapperThrows_WrapsInChatTransformException()
    {
        var client = FakeChatClient.ThatReturns("""[{"label":"Positive","score":0.9}]""");

        var node = new ChatBatchedStreamEnrichmentNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new ChatBatchedStreamEnrichmentOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze.",
                _ => "analyze",
                (_, _) => throw new InvalidOperationException("mapper failed"),
                BatchSize: 1),
        };

        await Assert.ThrowsAsync<ChatTransformException>(async () =>
        {
            await foreach (var _ in node.TransformAsync(new[] { new TestDomain.Comment("hello", "alice") }.ToAsyncEnumerable(), Context(),
                               CancellationToken.None))
            {
            }
        });
    }

    private static PipelineContext Context() => new();
}
