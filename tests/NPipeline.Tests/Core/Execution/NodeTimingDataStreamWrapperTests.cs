using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Observability;

namespace NPipeline.Tests.Core.Execution;

public sealed class NodeTimingDataStreamWrapperTests
{
    [Fact]
    public async Task WrapInputWait_TypedInput_PreservesStreamNameAndData()
    {
        // Arrange
        var input = new DataStream<int>(new[] { 1, 2, 3 }.ToAsyncEnumerable(), "source-stream");
        var scope = new RecordingScope();

        // Act
        var wrapped = NodeTimingDataStreamWrapper.WrapInputWait(input, scope);
        var output = new List<int>();

        await foreach (var item in wrapped.WithCancellation(CancellationToken.None))
        {
            output.Add(item);
        }

        // Assert
        wrapped.StreamName.Should().Be("source-stream");
        output.Should().Equal(1, 2, 3);
        scope.InputWaitSamples.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task WrapInputWait_UntypedInput_PreservesStreamNameAndData()
    {
        // Arrange
        IDataStream input = new DataStream<int>(new[] { 4, 5 }.ToAsyncEnumerable(), "source-stream-untyped");
        var scope = new RecordingScope();

        // Act
        var wrapped = NodeTimingDataStreamWrapper.WrapInputWait(input, scope);
        var typedWrapped = (IDataStream<int>)wrapped;
        var output = new List<int>();

        await foreach (var item in typedWrapped.WithCancellation(CancellationToken.None))
        {
            output.Add(item);
        }

        // Assert
        wrapped.StreamName.Should().Be("source-stream-untyped");
        output.Should().Equal(4, 5);
        scope.InputWaitSamples.Should().BeGreaterThan(0);
    }

    private sealed class RecordingScope : IAutoObservabilityScope
    {
        public int InputWaitSamples { get; private set; }

        public void RecordItemCount(long processed, long emitted)
        {
        }

        public void IncrementProcessed()
        {
        }

        public void IncrementEmitted()
        {
        }

        public void RecordFailure(Exception exception)
        {
        }

        public Exception? GetFailureException()
        {
            return null;
        }

        public void AddInputWait(TimeSpan duration)
        {
            InputWaitSamples++;
        }

        public void Dispose()
        {
        }
    }
}
