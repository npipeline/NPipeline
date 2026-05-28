using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class AIBatchedTransformNodeTests
{
    [Fact]
    public async Task TransformAsync_BareArrayResponse_ForSingleItemBatch_IsWrappedAndDeserialized()
    {
        var client = FakeChatClient.ThatReturns(
            """[{"category":"Greeting","confidence":0.9}]""");

        var node = new AIBatchedTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "Classify each item.",
                batch => $"Classify: {string.Join(", ", batch.Select(x => x.Text))}"),
        };

        var batch = new List<TestDomain.Comment>
        {
            new("hello", "alice"),
        };

        var results = await node.TransformAsync(batch, Context(), CancellationToken.None);

        Assert.Single(results);
        Assert.Equal("Greeting", results.Single().Category);
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
                    """[{"category":"Greeting","confidence":0.9}]""")));
        });

        var node = new AIBatchedTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "Classify each item.",
                batch => $"Classify: {string.Join(", ", batch.Select(x => x.Text))}",
                UseNativeStructuredOutput: true),
        };

        var batch = new List<TestDomain.Comment>
        {
            new("hello", "alice"),
        };

        _ = await node.TransformAsync(batch, Context(), CancellationToken.None);

        Assert.NotNull(capturedFormat);
        var schemaFormat = Assert.IsType<ChatResponseFormatJson>(capturedFormat, exactMatch: false);
        Assert.False(string.IsNullOrWhiteSpace(schemaFormat.SchemaName));
        Assert.True(schemaFormat.Schema.HasValue);
        var schema = schemaFormat.Schema.Value;
        Assert.Equal("object", schema.GetProperty("type").GetString());

        var properties = schema.GetProperty("properties");
        var items = properties.TryGetProperty("Items", out var pascalItems)
            ? pascalItems
            : properties.GetProperty("items");
        Assert.Equal("array", items.GetProperty("type").GetString());

        var itemSchema = items.GetProperty("items");
        Assert.Equal("object", itemSchema.GetProperty("type").GetString());

        var itemProperties = itemSchema.GetProperty("properties");
        Assert.Contains(itemProperties.EnumerateObject(), p => string.Equals(p.Name, "category", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(itemProperties.EnumerateObject(), p => string.Equals(p.Name, "confidence", StringComparison.OrdinalIgnoreCase));
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
                ? """[{"category":"Greeting","confidence":0.9}]"""
                : """[{"category":"Greeting","confidence":0.9},{"category":"Question","confidence":0.8}]""";

            return Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, responseText)));
        });

        var node = new AIBatchedTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "Classify.",
                _ => "classify"),
        };

        var batch = new List<TestDomain.Comment>
        {
            new("hello", "alice"),
            new("what time is it", "bob"),
        };

        var results = await node.TransformAsync(batch, Context(), CancellationToken.None);

        Assert.Equal(2, results.Count);
        Assert.Equal(2, callCount);
        Assert.Equal(2, userMessages.Count);
        Assert.Contains("EXACTLY 2", userMessages[1]);
    }

    [Fact]
    public async Task TransformAsync_SendsBatchAndReturnsResults()
    {
        var client = FakeChatClient.ThatReturns(
            """[{"category":"Greeting","confidence":0.9},{"category":"Question","confidence":0.8}]""");

        var node = new AIBatchedTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "Classify each item.",
                batch => $"Classify: {string.Join(", ", batch.Select(x => x.Text))}"),
        };

        var batch = new List<TestDomain.Comment>
        {
            new("hello", "alice"),
            new("what time is it", "bob"),
        };

        var results = await node.TransformAsync(batch, Context(), CancellationToken.None);

        Assert.Equal(2, results.Count);
        var list = results.ToList();
        Assert.Equal("Greeting", list[0].Category);
        Assert.Equal("Question", list[1].Category);
    }

    [Fact]
    public async Task TransformAsync_BadJson_ThrowsAITransformException()
    {
        var client = FakeChatClient.ThatReturns("not json");

        var node = new AIBatchedTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "Classify.",
                batch => "classify"),
        };

        var batch = new List<TestDomain.Comment> { new("hello", "alice") };

        await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(batch, Context(), CancellationToken.None));
    }

    [Fact]
    public async Task TransformAsync_CountMismatch_ThrowsAITransformException()
    {
        var client = FakeChatClient.ThatReturns(
            """[{"category":"Greeting","confidence":0.9}]""");

        var node = new AIBatchedTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "Classify.",
                _ => "classify"),
        };

        var batch = new List<TestDomain.Comment>
        {
            new("hello", "alice"),
            new("what time is it", "bob"),
        };

        var ex = await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(batch, Context(), CancellationToken.None));

        Assert.Contains("count mismatch", ex.Message);
    }

    private static PipelineContext Context()
    {
        return new PipelineContext();
    }
}
