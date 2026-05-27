using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class AIBatchedEnrichNodeTests
{
    [Fact]
    public async Task TransformAsync_SingleObjectResponse_ForSingleItemBatch_IsWrappedAndDeserialized()
    {
        var client = FakeChatClient.ThatReturns(
            """{"label":"Positive","score":0.9}""");

        var node = new AIBatchedEnrichNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new AIBatchedEnrichOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze.",
                _ => "analyze",
                (comment, result) => comment with { Author = result.Label }),
        };

        var batch = new List<TestDomain.Comment>
        {
            new("love it", "alice"),
        };

        var results = await node.TransformAsync(batch, Context(), CancellationToken.None);

        Assert.Single(results);
        Assert.Equal("Positive", results.Single().Author);
    }

    [Fact]
    public async Task TransformAsync_WithNativeStructuredOutput_UsesSchemaResponseFormat()
    {
        ChatResponseFormat? capturedFormat = null;

        var client = new FakeChatClient((_, options, _) =>
        {
            capturedFormat = options?.ResponseFormat;

            return Task.FromResult(new ChatResponse(
                new ChatMessage(ChatRole.Assistant,
                    """[{"label":"Positive","score":0.9}]""")));
        });

        var node = new AIBatchedEnrichNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new AIBatchedEnrichOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze.",
                _ => "analyze",
                (comment, result) => comment with { Author = result.Label },
                UseNativeStructuredOutput: true),
        };

        var batch = new List<TestDomain.Comment>
        {
            new("love it", "alice"),
        };

        _ = await node.TransformAsync(batch, Context(), CancellationToken.None);

        Assert.NotNull(capturedFormat);
        var schemaFormat = Assert.IsType<ChatResponseFormatJson>(capturedFormat, exactMatch: false);
        Assert.Equal("BatchResponse", schemaFormat.SchemaName);
        Assert.Equal("A JSON array of objects, one per input item.", schemaFormat.SchemaDescription);
        Assert.True(schemaFormat.Schema.HasValue);
        Assert.Equal("array", schemaFormat.Schema.Value.GetProperty("type").GetString());
    }

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

    [Fact]
    public async Task TransformAsync_ResultMapperThrows_WrapsInAITransformException()
    {
        var client = FakeChatClient.ThatReturns(
            """[{"label":"Positive","score":0.9}]""");

        var node = new AIBatchedEnrichNode<TestDomain.Comment, TestDomain.SentimentResult>(client)
        {
            Options = new AIBatchedEnrichOptions<TestDomain.Comment, TestDomain.SentimentResult>(
                "Analyze.",
                _ => "analyze",
                (_, _) => throw new InvalidOperationException("mapper failed")),
        };

        var batch = new List<TestDomain.Comment>
        {
            new("love it", "alice"),
        };

        var ex = await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(batch, Context(), CancellationToken.None));

        Assert.Contains("ResultMapper delegate failed", ex.Message);
        Assert.IsType<InvalidOperationException>(ex.InnerException, false);
    }

    private static PipelineContext Context()
    {
        return new PipelineContext();
    }
}
