using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tests.Reliability.Behavior;

namespace NPipeline.Tests.Nodes.Source;

/// <summary>
///     Tests for TapNode&lt;T&gt; diagnostic node.
///     Validates that TapNode sends copies to sink while passing through original items unchanged.
/// </summary>
public sealed class TapNodeTests
{
    #region Core Functionality Tests

    [Fact]
    public async Task TransformAsync_PassesItemsThroughUnchanged()
    {
        // Arrange
        int[] testItems = [1, 2, 3, 4, 5];
        DummySink<int> sink = new();
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.CreateDefault();

        // Act
        var results = await tapNode.TransformAsync(testItems.ToAsyncEnumerable(), context, CancellationToken.None).ToListAsync();

        // Assert - all items pass through unchanged
        _ = results.Should().Equal(testItems);
        _ = sink.ReceivedItems.Should().Equal(testItems);
    }

    [Fact]
    public async Task TransformAsync_WithinPipeline_CallsSinkOnceForTheWholeStream()
    {
        // Arrange
        var tapSink = new DummySink<int>();
        var mainSink = new DummySink<int>();

        // Act
        await BehaviorPipeline.RunAsync(b =>
        {
            var source = b.AddSource<StreamingSource<int>, int>("source");
            _ = b.AddPreconfiguredNodeInstance(source.Id, StreamingSource<int>.Of([1, 2, 3]));
            var tap = b.AddTap<int>(tapSink, "tap");
            var sink = b.AddSink<DummySink<int>, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(sink.Id, mainSink);
            _ = b.Connect(source, tap).Connect(tap, sink);
        });

        // Assert
        tapSink.Calls.Should().Be(1);
        tapSink.ReceivedItems.Should().Equal(1, 2, 3);
        mainSink.ReceivedItems.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task TransformAsync_WithinPipeline_SinkExceptionPropagates()
    {
        // Arrange
        var failingSink = new FailingSink<int>();

        // Act
        var act = async () => await BehaviorPipeline.RunAsync(b =>
        {
            var source = b.AddSource<StreamingSource<int>, int>("source");
            _ = b.AddPreconfiguredNodeInstance(source.Id, StreamingSource<int>.Of([1, 2, 3]));
            var tap = b.AddTap<int>(failingSink, "tap");
            var sink = b.AddSink<DummySink<int>, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(sink.Id, new DummySink<int>());
            _ = b.Connect(source, tap).Connect(tap, sink);
        });

        // Assert
        await act.Should().ThrowAsync<Exception>().WithInnerException(typeof(InvalidOperationException));
    }

    [Fact]
    public async Task TransformAsync_WithinPipeline_IgnoringSinkDoesNotBlockMainPath()
    {
        // Arrange
        var mainSink = new DummySink<int>();

        // Act
        await BehaviorPipeline.RunAsync(b =>
        {
            var source = b.AddSource<StreamingSource<int>, int>("source");
            _ = b.AddPreconfiguredNodeInstance(source.Id, StreamingSource<int>.Of(Enumerable.Range(0, 1_000)));
            var tap = b.AddTap<int>(new IgnoringSink<int>(), "tap");
            var sink = b.AddSink<DummySink<int>, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(sink.Id, mainSink);
            _ = b.Connect(source, tap).Connect(tap, sink);
        });

        // Assert
        mainSink.ReceivedItems.Should().Equal(Enumerable.Range(0, 1_000), "more items than the tap buffer holds must still flow");
    }

    [Fact]
    public async Task TransformAsync_WithinPipeline_CancellationIsPropagated()
    {
        // Arrange
        var tapSink = new DummySink<int>();
        using var cts = new CancellationTokenSource();

        // Act
        var run = BehaviorPipeline.RunAsync(b =>
        {
            var source = b.AddSource<StreamingSource<int>, int>("source");
            _ = b.AddPreconfiguredNodeInstance(source.Id, StreamingSource<int>.Unbounded([1]));
            var tap = b.AddTap<int>(tapSink, "tap");
            var sink = b.AddSink<DummySink<int>, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(sink.Id, new DummySink<int>());
            _ = b.Connect(source, tap).Connect(tap, sink);
        }, cancellationToken: cts.Token);

        await tapSink.FirstItemReceived.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();

        // Assert
        var act = async () => await run;
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task TransformAsync_CallsSinkOnceForTheWholeStream()
    {
        // Arrange
        DummySink<int> sink = new();
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.CreateDefault();

        // Act
        _ = await tapNode.TransformAsync(Enumerable.Range(0, 3).ToAsyncEnumerable(), context, CancellationToken.None).ToListAsync();

        // Assert
        _ = sink.Calls.Should().Be(1);
        _ = sink.ReceivedItems.Should().HaveCount(3);
    }

    [Fact]
    public async Task TransformAsync_ReturnsSameReferenceForReferenceTypes()
    {
        // Arrange
        CustomData originalData = new(1, "original");
        DummySink<CustomData> sink = new();
        TapNode<CustomData> tapNode = new(sink);
        var context = PipelineContext.CreateDefault();

        // Act
        var results = await tapNode.TransformAsync(new[] { originalData }.ToAsyncEnumerable(), context, CancellationToken.None).ToListAsync();

        // Assert - should return same reference
        _ = results.Should().ContainSingle().Which.Should().BeSameAs(originalData);
        _ = sink.ReceivedItems.Should().ContainSingle().Which.Should().BeSameAs(originalData);
    }

    [Fact]
    public async Task TransformAsync_SinkExceptionPropagates()
    {
        // Arrange
        FailingSink<int> failingSink = new();
        TapNode<int> tapNode = new(failingSink);
        var context = PipelineContext.CreateDefault();

        // Act & Assert
        var act = async () =>
            await tapNode.TransformAsync(Enumerable.Range(0, 5).ToAsyncEnumerable(), context, CancellationToken.None).ToListAsync();

        _ = await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task TransformAsync_SinkThatReturnsWithoutReading_DoesNotBlockMainPath()
    {
        // Arrange
        IgnoringSink<int> sink = new();
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.CreateDefault();

        // Act
        var results = await tapNode.TransformAsync(Enumerable.Range(0, 1_000).ToAsyncEnumerable(), context, CancellationToken.None).ToListAsync();

        // Assert - all items still pass through, including more than the tap buffer holds
        _ = results.Should().Equal(Enumerable.Range(0, 1_000));
    }

    [Fact]
    public async Task TransformAsync_SinkFailingAfterSomeItemsWhileTheBufferIsFull_Propagates()
    {
        // Arrange: the sink reads a few items, then stalls long enough for the writer to block on a full buffer.
        ThrowAfterSink<int> sink = new(3);
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.CreateDefault();

        // Act
        async Task DrainAsync()
        {
            await foreach (var _ in tapNode.TransformAsync(Enumerable.Range(0, 1_000).ToAsyncEnumerable(), context, CancellationToken.None))
            {
            }
        }

        var act = () => DrainAsync().WaitAsync(TimeSpan.FromSeconds(10));

        // Assert
        _ = await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("sink failed after reading");
    }

    [Fact]
    public async Task TransformAsync_InputFails_CancelsTheSinkInsteadOfEndingItsInput()
    {
        // Arrange
        ObservingSink<int> sink = new();
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.CreateDefault();

        async IAsyncEnumerable<int> FailingInput()
        {
            yield return 1;
            await Task.Yield();
            throw new InvalidOperationException("upstream failed");
        }

        // Act
        var act = async () => await tapNode.TransformAsync(FailingInput(), context, CancellationToken.None).ToListAsync();
        _ = await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("upstream failed");
        await tapNode.DisposeAsync();

        // Assert
        _ = sink.Outcome.Should().Be("cancelled", "a sink must not treat an aborted stream as a complete one");
    }

    #endregion

    #region Cancellation Tests

    [Fact]
    public async Task TransformAsync_WithCancelledToken_PropagatesException()
    {
        // Arrange
        DummySink<int> sink = new();
        TapNode<int> tapNode = new(sink);
        var context = PipelineContext.CreateDefault();
        using CancellationTokenSource cts = new();
        cts.Cancel();

        // Act & Assert - should throw OperationCanceledException
        var act = async () =>
            await tapNode.TransformAsync(Enumerable.Range(0, 5).ToAsyncEnumerable(), context, cts.Token).ToListAsync();

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
    }

    #endregion

    #region Disposal Tests

    [Fact]
    public async Task DisposeAsync_DisposesSink()
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
    public async Task DisposeAsync_DoesNotThrow()
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
        private readonly TaskCompletionSource _firstItem = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Calls { get; private set; }

        public List<T> ReceivedItems { get; } = [];

        public Task FirstItemReceived => _firstItem.Task;

        public override async Task ConsumeAsync(
            IDataStream<T> input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            Calls++;

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                ReceivedItems.Add(item);
                _ = _firstItem.TrySetResult();
            }
        }
    }

    private sealed class FailingSink<T> : SinkNode<T>
    {
        public override Task ConsumeAsync(
            IDataStream<T> input,
            PipelineContext context,
            CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Sink failed intentionally");
    }

    private sealed class IgnoringSink<T> : SinkNode<T>
    {
        public override Task ConsumeAsync(
            IDataStream<T> input,
            PipelineContext context,
            CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class DisposableSink<T> : SinkNode<T>, IAsyncDisposable
    {
        public bool IsDisposed { get; private set; }

        public async ValueTask DisposeAsync()
        {
            IsDisposed = true;
        }

        public override async Task ConsumeAsync(
            IDataStream<T> input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // No-op
            }
        }
    }

    private sealed class ThrowAfterSink<T>(int readBeforeFailing) : SinkNode<T>
    {
        public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
        {
            var read = 0;

            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                if (++read == readBeforeFailing)
                {
                    await Task.Delay(100, CancellationToken.None);
                    throw new InvalidOperationException("sink failed after reading");
                }
            }
        }
    }

    private sealed class ObservingSink<T> : SinkNode<T>
    {
        public string? Outcome { get; private set; }

        public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var _ in input.WithCancellation(cancellationToken))
                {
                }

                Outcome = "completed";
            }
            catch (OperationCanceledException)
            {
                Outcome = "cancelled";
            }
        }
    }

    private sealed record CustomData(int Id, string Name);

    #endregion
}
