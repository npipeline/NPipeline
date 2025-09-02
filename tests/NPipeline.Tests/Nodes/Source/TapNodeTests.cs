using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Source;

/// <summary>
///     Tests for TapNode&lt;T&gt; diagnostic node.
///     Validates that TapNode sends copies to sink while passing through original items unchanged.
///     Covers 19 statements in TapNode&lt;T&gt;.
/// </summary>
public sealed class TapNodeTests
{
    #region Core Functionality Tests

    [Fact]
    public async Task TapNode_PassesItemThroughUnchanged()
    {
        // Arrange
        const int testItem = 42;
        DummySink<int> sink = new();
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.Default;

        // Act
        var result = await tapNode.ExecuteAsync(testItem, context, CancellationToken.None);

        // Assert - item should pass through unchanged
        _ = result.Should().Be(testItem);
    }

    [Fact]
    public async Task TapNode_SendsCopyToSink()
    {
        // Arrange
        const string testItem = "test_data";
        DummySink<string> sink = new();
        TapNode<string> tapNode = new(sink);
        var context = PipelineContext.Default;

        // Act
        _ = await tapNode.ExecuteAsync(testItem, context, CancellationToken.None);

        // Assert - sink should have received the item
        _ = sink.ReceivedItems.Should().HaveCount(1);
        _ = sink.ReceivedItems[0].Should().Be(testItem);
    }

    [Fact]
    public async Task TapNode_WithMultipleItems_AllPassThroughUnchanged()
    {
        // Arrange
        int[] testItems = [1, 2, 3, 4, 5];
        DummySink<int> sink = new();
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.Default;

        // Act
        List<int> results = [];

        foreach (var item in testItems)
        {
            results.Add(await tapNode.ExecuteAsync(item, context, CancellationToken.None));
        }

        // Assert - all items pass through unchanged
        _ = results.Should().Equal(testItems);
        _ = sink.ReceivedItems.Should().HaveCount(5);
    }

    [Fact]
    public async Task TapNode_WithReferenceType_PassesByReference()
    {
        // Arrange
        CustomData originalData = new(1, "original");
        DummySink<CustomData> sink = new();
        TapNode<CustomData> tapNode = new(sink);
        var context = PipelineContext.Default;

        // Act
        var result = await tapNode.ExecuteAsync(originalData, context, CancellationToken.None);

        // Assert - should return same reference
        _ = result.Should().BeSameAs(originalData);
        _ = sink.ReceivedItems.Should().HaveCount(1);
        _ = sink.ReceivedItems[0].Should().BeSameAs(originalData);
    }

    [Fact]
    public async Task TapNode_SinkExceptionDoesNotAffectItemReturn()
    {
        // Arrange
        const int testItem = 100;
        FailingSink<int> failingSink = new();
        TapNode<int> tapNode = new(failingSink);
        var context = PipelineContext.Default;

        // Act & Assert
        // Even if sink fails, TapNode should propagate the exception
        _ = await tapNode.Invoking(tn => tn.ExecuteAsync(testItem, context, CancellationToken.None))
            .Should().ThrowAsync<InvalidOperationException>();
    }

    #endregion

    #region Data Type Variation Tests

    [Fact]
    public async Task TapNode_WithInt_Works()
    {
        // Arrange
        DummySink<int> sink = new();
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.Default;

        // Act
        var result = await tapNode.ExecuteAsync(999, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(999);
        _ = sink.ReceivedItems.Should().ContainSingle(i => i == 999);
    }

    [Fact]
    public async Task TapNode_WithDouble_Works()
    {
        // Arrange
        DummySink<double> sink = new();
        TapNode<double> tapNode = new(sink);
        var context = PipelineContext.Default;

        // Act
        var result = await tapNode.ExecuteAsync(3.14, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(3.14);
        _ = sink.ReceivedItems.Should().ContainSingle(d => d.Equals(3.14));
    }

    [Fact]
    public async Task TapNode_WithComplexType_Works()
    {
        // Arrange
        CustomData complexData = new(42, "complex");
        DummySink<CustomData> sink = new();
        TapNode<CustomData> tapNode = new(sink);
        var context = PipelineContext.Default;

        // Act
        var result = await tapNode.ExecuteAsync(complexData, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(complexData);
        _ = sink.ReceivedItems.Should().ContainSingle(cd => cd.Id == 42 && cd.Name == "complex");
    }

    [Fact]
    public async Task TapNode_WithNullableValue_Works()
    {
        // Arrange
        DummySink<int?> sink = new();
        TapNode<int?> tapNode = new(sink);
        var context = PipelineContext.Default;
        int? nullableValue = 42;

        // Act
        var result = await tapNode.ExecuteAsync(nullableValue, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(42);
        _ = sink.ReceivedItems.Should().ContainSingle(v => v == 42);
    }

    [Fact]
    public async Task TapNode_WithNull_Works()
    {
        // Arrange
        DummySink<string?> sink = new();
        TapNode<string?> tapNode = new(sink);
        var context = PipelineContext.Default;

        // Act
        var result = await tapNode.ExecuteAsync(null, context, CancellationToken.None);

        // Assert
        _ = result.Should().BeNull();
        _ = sink.ReceivedItems.Should().ContainSingle(s => s == null);
    }

    #endregion

    #region Cancellation Tests

    [Fact]
    public async Task TapNode_WithCancellationToken_PassesTokenToSink()
    {
        // Arrange
        DummySink<int> sink = new();
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.Default;
        using CancellationTokenSource cts = new();

        // Act
        var result = await tapNode.ExecuteAsync(123, context, cts.Token);

        // Assert
        _ = result.Should().Be(123);
        _ = sink.ReceivedItems.Should().HaveCount(1);
    }

    [Fact]
    public async Task TapNode_WithCancelledToken_PropagatesException()
    {
        // Arrange
        DummySink<int> sink = new();
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.Default;
        using CancellationTokenSource cts = new();
        cts.Cancel();

        // Act & Assert - should throw OperationCanceledException
        _ = await tapNode.Invoking(tn => tn.ExecuteAsync(123, context, cts.Token))
            .Should().ThrowAsync<OperationCanceledException>();
    }

    #endregion

    #region Disposal Tests

    [Fact]
    public async Task TapNode_DisposeAsyncDisposesSink()
    {
        // Arrange
        DisposableSink<int> disposableSink = new();
        TapNode<int> tapNode = new(disposableSink);

        // Act
        await tapNode.DisposeAsync();

        // Assert - sink should be disposed
        _ = disposableSink.IsDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task TapNode_DisposalDoesNotThrow()
    {
        // Arrange
        DummySink<int> sink = new();
        TapNode<int> tapNode = new(sink);

        // Act & Assert - should not throw
        _ = await tapNode.Invoking(tn => tn.DisposeAsync().AsTask()).Should().NotThrowAsync();
    }

    #endregion

    #region Test Fixtures

    private sealed class DummySink<T> : SinkNode<T>
    {
        public List<T> ReceivedItems { get; } = [];

        public override async Task ExecuteAsync(
            IDataPipe<T> input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                ReceivedItems.Add(item);
            }
        }
    }

    private sealed class FailingSink<T> : SinkNode<T>
    {
        public override Task ExecuteAsync(
            IDataPipe<T> input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("Sink failed intentionally");
        }
    }

    private sealed class DisposableSink<T> : SinkNode<T>
    {
        public bool IsDisposed { get; private set; }

        public override async Task ExecuteAsync(
            IDataPipe<T> input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // No-op
            }
        }

        public override async ValueTask DisposeAsync()
        {
            IsDisposed = true;
            await base.DisposeAsync();
        }
    }

    private sealed record CustomData(int Id, string Name);

    #endregion
}
