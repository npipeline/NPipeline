using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class AIBatchedTransformNodeTests
{
    [Fact]
    public async Task TransformAsync_SingleObjectResponse_ForSingleItemBatch_IsWrappedAndDeserialized()
    {
        var client = FakeChatClient.ThatReturns(
            """{"category":"Greeting","confidence":0.9}""");

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
        Assert.Equal("BatchResponse", schemaFormat.SchemaName);
        Assert.Equal("A JSON array of objects, one per input item.", schemaFormat.SchemaDescription);
        Assert.True(schemaFormat.Schema.HasValue);
        Assert.Equal("array", schemaFormat.Schema.Value.GetProperty("type").GetString());
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
