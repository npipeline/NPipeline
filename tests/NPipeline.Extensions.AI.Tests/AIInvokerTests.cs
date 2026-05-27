using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class AIInvokerErrorPathTests
{
    [Fact]
    public async Task InvokeTransform_MarkdownFencedJson_DeserializesSuccessfully()
    {
        var client = FakeChatClient.ThatReturns(
            "```json\n{\"category\":\"Greeting\",\"confidence\":0.95}\n```");

        var node = CreateTransformNode(client);

        var result = await node.TransformAsync(
            new TestDomain.Comment("hello", "alice"),
            new PipelineContext(),
            CancellationToken.None);

        Assert.Equal("Greeting", result.Category);
        Assert.Equal(0.95f, result.Confidence);
    }

    [Fact]
    public async Task InvokeTransform_MarkdownFencedJson_NoNewlineAfterTag_DeserializesSuccessfully()
    {
        var client = FakeChatClient.ThatReturns(
            "```json{\"category\":\"Greeting\",\"confidence\":0.95}```");

        var node = CreateTransformNode(client);

        var result = await node.TransformAsync(
            new TestDomain.Comment("hello", "alice"),
            new PipelineContext(),
            CancellationToken.None);

        Assert.Equal("Greeting", result.Category);
        Assert.Equal(0.95f, result.Confidence);
    }

    [Fact]
    public async Task InvokeTransform_JsonNullArray_ThrowsDeserializationError()
    {
        var client = FakeChatClient.ThatReturns("[null]");
        var node = CreateTransformNode(client);

        var ex = await Assert.ThrowsAsync<AITransformException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), new PipelineContext(), CancellationToken.None));

        Assert.Contains("deserialize", ex.Message);
    }

    [Fact]
    public async Task InvokeTransform_TimeoutException_Propagates()
    {
        var client = FakeChatClient.ThatThrows(new TimeoutException("timed out"));
        var node = CreateTransformNode(client);

        await Assert.ThrowsAsync<TimeoutException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), new PipelineContext(), CancellationToken.None));
    }

    [Fact]
    public async Task InvokeTransform_OperationCanceled_Propagates()
    {
        var client = FakeChatClient.ThatThrows(new OperationCanceledException("cancelled"));
        var node = CreateTransformNode(client);

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            node.TransformAsync(new TestDomain.Comment("hello", "alice"), new PipelineContext(), CancellationToken.None));
    }

    private static AITransformNode<TestDomain.Comment, TestDomain.ClassificationResult> CreateTransformNode(IChatClient client)
    {
        return new AITransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AITransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "Classify.",
                c => $"Classify: {c.Text}"),
        };
    }
}
