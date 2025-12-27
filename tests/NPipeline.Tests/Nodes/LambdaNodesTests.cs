using AwesomeAssertions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes;

/// <summary>
///     Tests for Lambda-based node implementations.
///     Validates that lambda nodes execute correctly and respect cancellation.
/// </summary>
public sealed class LambdaNodesTests
{
    [Fact]
    public async Task LambdaTransformNode_ExecutesTransformation()
    {
        // Arrange
        Func<int, int> transform = x => x * 2;
        var node = new LambdaTransformNode<int, int>(transform);
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(5, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(10);
    }

    [Fact]
    public async Task AsyncLambdaTransformNode_ExecutesAsyncTransformation()
    {
        // Arrange
        Func<int, CancellationToken, ValueTask<int>> asyncTransform = async (x, ct) =>
        {
            await Task.Delay(10, ct);
            return x * 2;
        };

        var node = new AsyncLambdaTransformNode<int, int>(asyncTransform);
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(5, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(10);
    }

    [Fact]
    public async Task AsyncLambdaTransformNode_RespectsCancellationToken()
    {
        // Arrange
        var cts = new CancellationTokenSource();

        Func<int, CancellationToken, ValueTask<int>> slowTransform = async (x, ct) =>
        {
            await Task.Delay(1000, ct);
            return x;
        };

        var node = new AsyncLambdaTransformNode<int, int>(slowTransform);
        var context = PipelineContext.Default;

        // Act & Assert
        cts.CancelAfter(50);
        _ = await Assert.ThrowsAsync<TaskCanceledException>(() => node.ExecuteAsync(5, context, cts.Token));
    }

    [Fact]
    public void LambdaTransformNode_ThrowsOnNullTransform()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new LambdaTransformNode<int, int>(null!));
    }

    [Fact]
    public void AsyncLambdaTransformNode_ThrowsOnNullTransform()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new AsyncLambdaTransformNode<int, int>(null!));
    }

    [Fact]
    public void LambdaSourceNode_WithSyncFactory_ThrowsOnNull()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new LambdaSourceNode<int>((Func<IEnumerable<int>>)null!));
    }

    [Fact]
    public void LambdaSourceNode_WithAsyncFactory_ThrowsOnNull()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new LambdaSourceNode<int>((Func<CancellationToken, IAsyncEnumerable<int>>)null!));
    }

    [Fact]
    public void LambdaSinkNode_WithSyncConsumer_ThrowsOnNull()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new LambdaSinkNode<int>((Action<int>)null!));
    }

    [Fact]
    public void LambdaSinkNode_WithAsyncConsumer_ThrowsOnNull()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new LambdaSinkNode<int>(null!));
    }

    [Fact]
    public async Task LambdaTransformNode_WithStringTransformation()
    {
        // Arrange
        Func<string, string> upperTransform = s => s.ToUpperInvariant();
        var node = new LambdaTransformNode<string, string>(upperTransform);
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync("hello", context, CancellationToken.None);

        // Assert
        _ = result.Should().Be("HELLO");
    }

    [Fact]
    public async Task LambdaTransformNode_ExecuteValueTaskAsync_ReturnsSynchronouslyCompletion()
    {
        // Arrange
        Func<int, int> addOne = x => x + 1;
        var node = new LambdaTransformNode<int, int>(addOne);
        var context = PipelineContext.Default;

        // Act
        var valueTask = node.ExecuteValueTaskAsync(5, context, CancellationToken.None);

        // Assert - ValueTask should be synchronously completed
        _ = valueTask.IsCompleted.Should().BeTrue();
        var result = await valueTask;
        _ = result.Should().Be(6);
    }
}
