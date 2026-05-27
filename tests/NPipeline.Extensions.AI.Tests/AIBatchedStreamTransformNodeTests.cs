using Microsoft.Extensions.AI;
using NPipeline.Execution;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class AIBatchedStreamTransformNodeTests
{
    [Fact]
    public void ExecutionStrategy_Default_IsStreamCapable()
    {
        var node = new AIBatchedStreamTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(FakeChatClient.ThatReturns("[]"));

        _ = Assert.IsType<IStreamExecutionStrategy>(node.ExecutionStrategy, false);
    }

    [Fact]
    public async Task TransformAsync_MultipleBatches_ReturnsAllResults()
    {
        var client = FakeChatClient.ThatReturns(
            """[{"category":"Greeting","confidence":0.9},{"category":"Greeting","confidence":0.9}]""");

        var node = new AIBatchedStreamTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedStreamTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "Classify.",
                batch => "classify",
                BatchSize: 2),
        };

        var items = new List<TestDomain.Comment>
        {
            new("a", "x"), new("b", "x"), new("c", "x"), new("d", "x"),
        };

        var results = new List<TestDomain.ClassificationResult>();

        await foreach (var result in node.TransformAsync(items.ToAsyncEnumerable(), Context(), CancellationToken.None))
        {
            results.Add(result);
        }

        Assert.Equal(4, results.Count);
        Assert.All(results, r => Assert.Equal("Greeting", r.Category));
    }

    [Fact]
    public async Task TransformAsync_IncompleteFinalBatch_IsFlushed()
    {
        var callCount = 0;

        var client = new FakeChatClient((_, _, _) =>
        {
            callCount++;

            return Task.FromResult(new ChatResponse(
                new ChatMessage(ChatRole.Assistant,
                    """[{"category":"X","confidence":0.5},{"category":"Y","confidence":0.5},{"category":"Z","confidence":0.5}]""")));
        });

        var node = new AIBatchedStreamTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedStreamTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "X",
                batch => "x",
                BatchSize: 5),
        };

        var items = new List<TestDomain.Comment> { new("a", "x"), new("b", "x"), new("c", "x") };
        var results = new List<TestDomain.ClassificationResult>();

        await foreach (var result in node.TransformAsync(items.ToAsyncEnumerable(), Context(), CancellationToken.None))
        {
            results.Add(result);
        }

        Assert.Equal(3, results.Count);
        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task TransformAsync_CountMismatch_ThrowsAITransformException()
    {
        var client = FakeChatClient.ThatReturns(
            """[{"category":"X","confidence":0.5}]""");

        var node = new AIBatchedStreamTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedStreamTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "X",
                _ => "x",
                BatchSize: 2),
        };

        var items = new List<TestDomain.Comment> { new("a", "x"), new("b", "x") };

        await Assert.ThrowsAsync<AITransformException>(async () =>
        {
            await foreach (var _ in node.TransformAsync(items.ToAsyncEnumerable(), Context(), CancellationToken.None))
            {
            }
        });
    }

    [Fact]
    public async Task TransformAsync_ErrorInBatch_ThrowsAITransformException()
    {
        var client = FakeChatClient.ThatThrows(new InvalidOperationException("something broke"));

        var node = new AIBatchedStreamTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedStreamTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "X",
                batch => "x",
                BatchSize: 2),
        };

        var items = new List<TestDomain.Comment> { new("a", "x"), new("b", "x") };

        await Assert.ThrowsAsync<AITransformException>(async () =>
        {
            await foreach (var _ in node.TransformAsync(items.ToAsyncEnumerable(), Context(), CancellationToken.None))
            {
            }
        });
    }

    [Fact]
    public async Task TransformAsync_InfrastructureException_PropagatesOnFirstBatch()
    {
        var client = FakeChatClient.ThatThrows(new HttpRequestException("network error"));

        var node = new AIBatchedStreamTransformNode<TestDomain.Comment, TestDomain.ClassificationResult>(client)
        {
            Options = new AIBatchedStreamTransformOptions<TestDomain.Comment, TestDomain.ClassificationResult>(
                "X",
                batch => "x",
                BatchSize: 2),
        };

        var items = new List<TestDomain.Comment> { new("a", "x"), new("b", "x") };

        await Assert.ThrowsAsync<HttpRequestException>(async () =>
        {
            await foreach (var _ in node.TransformAsync(items.ToAsyncEnumerable(), Context(), CancellationToken.None))
            {
            }
        });
    }

    private static PipelineContext Context()
    {
        return new PipelineContext();
    }
}
