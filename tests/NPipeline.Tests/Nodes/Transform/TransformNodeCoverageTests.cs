// ReSharper disable ClassNeverInstantiated.Local

using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Transform;

/// <summary>
///     Comprehensive tests for DelegateTransformNode functionality.
///     Tests async transforms, sync transforms, and error handling.
/// </summary>
public sealed class DelegateTransformNodeTests
{
    #region Disposal Tests

    [Fact]
    public async Task DelegateTransformNode_DisposeAsync_CompletesSuccessfully()
    {
        // Arrange
        DelegateTransformNode<int, int> node = new(x => x);

        // Act & Assert - should not throw
        await node.DisposeAsync();
    }

    #endregion

    #region Async Delegate Transform Tests

    [Fact]
    public async Task DelegateTransformNode_WithAsyncDelegate_TransformsItem()
    {
        // Arrange
        DelegateTransformNode<int, string> node = new(async (item, _, _) =>
        {
            await Task.CompletedTask;
            return item.ToString();
        });

        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(42, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be("42");
    }

    [Fact]
    public async Task DelegateTransformNode_WithAsyncDelegate_UsesContextAndCancellation()
    {
        // Arrange
        var contextUsed = false;

        DelegateTransformNode<string, int> node = new(async (item, ctx, ct) =>
        {
            contextUsed = ctx != null;
            await Task.CompletedTask;
            return item.Length;
        });

        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync("test", context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(4);
        _ = contextUsed.Should().BeTrue();
    }

    [Fact]
    public async Task DelegateTransformNode_WithAsyncDelegate_MultipleItems_TransformsEach()
    {
        // Arrange
        DelegateTransformNode<int, int> node = new(async (item, _, _) =>
        {
            await Task.Delay(1, CancellationToken.None);
            return item * 2;
        });

        var context = PipelineContext.Default;
        int[] items = [1, 2, 3, 4, 5];

        // Act
        List<int> results = [];

        foreach (var item in items)
        {
            var result = await node.ExecuteAsync(item, context, CancellationToken.None);
            results.Add(result);
        }

        // Assert
        _ = results.Should().ContainInOrder(2, 4, 6, 8, 10);
    }

    [Fact]
    public async Task DelegateTransformNode_WithAsyncDelegate_PropagatesException()
    {
        // Arrange
        DelegateTransformNode<int, int> node = new(async (_, _, _) =>
        {
            await Task.CompletedTask;
            throw new InvalidOperationException("Transform failed");
        });

        var context = PipelineContext.Default;

        // Act & Assert
        _ = await node.Awaiting(n => n.ExecuteAsync(1, context, CancellationToken.None))
            .Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Transform failed");
    }

    #endregion

    #region Sync Delegate Transform Tests

    [Fact]
    public async Task DelegateTransformNode_WithSyncDelegate_TransformsItem()
    {
        // Arrange
        DelegateTransformNode<int, string> node = new(item => item.ToString());
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(42, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be("42");
    }

    [Fact]
    public async Task DelegateTransformNode_WithSyncDelegate_WrapsInTask()
    {
        // Arrange
        var syncCalls = 0;

        DelegateTransformNode<int, int> node = new(item =>
        {
            syncCalls++;
            return item + 10;
        });

        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(5, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(15);
        _ = syncCalls.Should().Be(1);
    }

    [Fact]
    public async Task DelegateTransformNode_WithSyncDelegate_MultipleItems()
    {
        // Arrange
        DelegateTransformNode<string, int> node = new(s => s.Length);
        var context = PipelineContext.Default;

        // Act
        var result1 = await node.ExecuteAsync("hello", context, CancellationToken.None);
        var result2 = await node.ExecuteAsync("world", context, CancellationToken.None);
        var result3 = await node.ExecuteAsync("test", context, CancellationToken.None);

        // Assert
        _ = result1.Should().Be(5);
        _ = result2.Should().Be(5);
        _ = result3.Should().Be(4);
    }

    [Fact]
    public async Task DelegateTransformNode_WithSyncDelegate_PropagatesException()
    {
        // Arrange
        DelegateTransformNode<int, int> node = new(item => { throw new ArgumentException("Invalid input"); });
        var context = PipelineContext.Default;

        // Act & Assert
        _ = await node.Awaiting(n => n.ExecuteAsync(1, context, CancellationToken.None))
            .Should().ThrowAsync<ArgumentException>()
            .WithMessage("Invalid input");
    }

    #endregion

    #region Null Handling Tests

    [Fact]
    public void DelegateTransformNode_WithNullAsyncDelegate_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() =>
        {
            _ = new DelegateTransformNode<int, int>(
                (Func<int, PipelineContext, CancellationToken, Task<int>>)null!);
        });
    }

    [Fact]
    public void DelegateTransformNode_WithNullSyncDelegate_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() =>
        {
            _ = new DelegateTransformNode<int, int>(
                (Func<int, int>)null!);
        });
    }

    #endregion
}

/// <summary>
///     Comprehensive tests for DefaultTypeConversionErrorHandler functionality.
///     Tests error decision logic for type conversion failures.
/// </summary>
public sealed class DefaultTypeConversionErrorHandlerTests
{
    #region Format Exception Handling

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithFormatException_ReturnsSkip()
    {
        // Arrange
        DefaultTypeConversionErrorHandler<string, int> handler = new(NodeErrorDecision.Skip);
        TestTransformNode node = new();
        var context = PipelineContext.Default;
        FormatException error = new("Invalid format");

        // Act
        var decision = await handler.HandleAsync(node, "invalid", error, context, CancellationToken.None);

        // Assert
        _ = decision.Should().Be(NodeErrorDecision.Skip);
    }

    #endregion

    #region InvalidCast Exception Handling

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithInvalidCastException_ReturnsSkip()
    {
        // Arrange
        DefaultTypeConversionErrorHandler<object, int> handler = new(NodeErrorDecision.Skip);
        TestTransformNodeObject node = new();
        var context = PipelineContext.Default;
        InvalidCastException error = new("Cannot cast");

        // Act
        var decision = await handler.HandleAsync(node, new object(), error, context, CancellationToken.None);

        // Assert
        _ = decision.Should().Be(NodeErrorDecision.Skip);
    }

    #endregion

    #region Overflow Exception Handling

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithOverflowException_ReturnsSkip()
    {
        // Arrange
        DefaultTypeConversionErrorHandler<string, long> handler = new(NodeErrorDecision.Skip);
        TestTransformNodeLong node = new();
        var context = PipelineContext.Default;
        OverflowException error = new("Number too large");

        // Act
        var decision = await handler.HandleAsync(node, "999999999999999999999", error, context, CancellationToken.None);

        // Assert
        _ = decision.Should().Be(NodeErrorDecision.Skip);
    }

    #endregion

    #region Other Exception Handling

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithOtherException_ReturnsFail()
    {
        // Arrange
        DefaultTypeConversionErrorHandler<int, int> handler = new(NodeErrorDecision.Skip);
        TestTransformNodeInt node = new();
        var context = PipelineContext.Default;
        NotSupportedException error = new("Operation not supported");

        // Act
        var decision = await handler.HandleAsync(node, 1, error, context, CancellationToken.None);

        // Assert
        _ = decision.Should().Be(NodeErrorDecision.Fail);
    }

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithGenericException_ReturnsFail()
    {
        // Arrange
        DefaultTypeConversionErrorHandler<int, int> handler = new(NodeErrorDecision.Skip);
        TestTransformNodeInt node = new();
        var context = PipelineContext.Default;
        Exception error = new("Generic error");

        // Act
        var decision = await handler.HandleAsync(node, 1, error, context, CancellationToken.None);

        // Assert
        _ = decision.Should().Be(NodeErrorDecision.Fail);
    }

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithArgumentException_ReturnsFail()
    {
        // Arrange
        DefaultTypeConversionErrorHandler<string, int> handler = new(NodeErrorDecision.Skip);
        TestTransformNode node = new();
        var context = PipelineContext.Default;
        ArgumentException error = new("Invalid argument");

        // Act
        var decision = await handler.HandleAsync(node, "invalid", error, context, CancellationToken.None);

        // Assert
        _ = decision.Should().Be(NodeErrorDecision.Fail);
    }

    #endregion

    #region Decision Configuration Tests

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithFailDecision_UsesFailForConversionErrors()
    {
        // Arrange
        DefaultTypeConversionErrorHandler<string, int> handler = new(NodeErrorDecision.Fail);
        TestTransformNode node = new();
        var context = PipelineContext.Default;
        FormatException error = new("Invalid");

        // Act
        var decision = await handler.HandleAsync(node, "invalid", error, context, CancellationToken.None);

        // Assert
        _ = decision.Should().Be(NodeErrorDecision.Fail);
    }

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_AllConversionExceptionTypes_AreRecognized()
    {
        // Arrange
        DefaultTypeConversionErrorHandler<string, int> handler = new(NodeErrorDecision.Skip);
        TestTransformNode node = new();
        var context = PipelineContext.Default;

        Exception[] exceptions =
        [
            new FormatException(),
            new InvalidCastException(),
            new OverflowException(),
        ];

        // Act & Assert
        foreach (var ex in exceptions)
        {
            var decision = await handler.HandleAsync(node, "test", ex, context, CancellationToken.None);
            _ = decision.Should().Be(NodeErrorDecision.Skip);
        }
    }

    #endregion

    #region Helper Classes

    private sealed class TestTransformNode : TransformNode<string, int>
    {
        public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(int.Parse(item));
        }
    }

    private sealed class TestTransformNodeObject : TransformNode<object, int>
    {
        public override Task<int> ExecuteAsync(object item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult((int)item);
        }
    }

    private sealed class TestTransformNodeLong : TransformNode<string, long>
    {
        public override Task<long> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(long.Parse(item));
        }
    }

    private sealed class TestTransformNodeInt : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }

    #endregion
}
