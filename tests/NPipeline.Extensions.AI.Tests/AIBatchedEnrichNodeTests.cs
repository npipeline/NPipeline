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
        Assert.False(string.IsNullOrWhiteSpace(schemaFormat.SchemaName));
        Assert.True(schemaFormat.Schema.HasValue);
        var schema = schemaFormat.Schema.Value;
        Assert.Equal("array", schema.GetProperty("type").GetString());

        var items = schema.GetProperty("items");
        Assert.Equal("object", items.GetProperty("type").GetString());

        var properties = items.GetProperty("properties");
        Assert.Contains(properties.EnumerateObject(), p => string.Equals(p.Name, "label", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(properties.EnumerateObject(), p => string.Equals(p.Name, "score", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task TransformAsync_CountMismatch_RetriesAndSucceeds()
    {
        var callCount = 0;
        var userMessages = new List<string>();

        var client = new FakeChatClient((messages, _, _) =>
        {
            callCount++;
            var messageList = messages.ToList();
            userMessages.Add(messageList.First(m => m.Role == ChatRole.User).Text ?? string.Empty);

            var responseText = callCount == 1
                ? """[{"label":"Positive","score":0.9}]"""
                : """[{"label":"Positive","score":0.9},{"label":"Negative","score":0.1}]""";

            return Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, responseText)));
        });

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
            new("hate it", "bob"),
        };

        var results = await node.TransformAsync(batch, Context(), CancellationToken.None);

        Assert.Equal(2, results.Count);
        Assert.Equal(2, callCount);
        Assert.Equal(2, userMessages.Count);
        Assert.Contains("EXACTLY 2", userMessages[1]);
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
