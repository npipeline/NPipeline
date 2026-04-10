using AwesomeAssertions;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Sampling;

namespace NPipeline.Tests.Execution.Strategies;

public sealed class SequentialExecutionStrategyValueTaskTests
{
    [Fact]
    public async Task Should_PreferValueTaskPath_WhenTransformOverrides()
    {
        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>(new[] { 1, 2, 3 }, "input");
        var transform = new ValueTaskFriendlyTransform();
        var strategy = new SequentialExecutionStrategy();
        var context = new PipelineContext();

        await using var output = await strategy.ExecuteAsync(input, transform, context, CancellationToken.None);

        var results = new List<int>();

        await foreach (var value in output.WithCancellation(CancellationToken.None))
        {
            results.Add(value);
        }

        _ = results.Should().BeEquivalentTo([2, 3, 4]);
        _ = transform.ExecuteAsyncCallCount.Should().Be(0);
        _ = transform.ExecuteValueTaskCallCount.Should().Be(3);
    }

    [Fact]
    public async Task Should_RecordErrorWithCorrelation_WhenTransformThrows()
    {
        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>(new[] { 7 }, "input");
        var transform = new ThrowingTransform();
        var strategy = new SequentialExecutionStrategy();
        var context = new PipelineContext();
        var recorder = new RecordingSampleRecorder();
        var correlationId = Guid.NewGuid();

        context.Properties[PipelineContextKeys.SampleRecorder] = recorder;
        LineageExecutionItemContext.SetCurrentInputContext(0, correlationId, [2, 4]);

        try
        {
            using (context.ScopedNode("transform"))
            {
                await using var output = await strategy.ExecuteAsync(input, transform, context, CancellationToken.None);

                var act = async () =>
                {
                    await foreach (var _ in output.WithCancellation(CancellationToken.None))
                    {
                    }
                };

                _ = await act.Should().ThrowAsync<InvalidOperationException>();
            }
        }
        finally
        {
            LineageExecutionItemContext.ClearCurrentInputIndex();
        }

        _ = recorder.Errors.Should().HaveCount(1);
        _ = recorder.Errors[0].CorrelationId.Should().Be(correlationId);
        _ = recorder.Errors[0].AncestryInputIndices.Should().BeEquivalentTo([2, 4]);
        _ = recorder.Errors[0].RetryCount.Should().Be(0);
        _ = recorder.Errors[0].ErrorMessage.Should().Contain("sequential boom");
    }

    private sealed class ValueTaskFriendlyTransform : TransformNode<int, int>
    {
        public int ExecuteAsyncCallCount { get; private set; }

        public int ExecuteValueTaskCallCount { get; private set; }

        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            ExecuteAsyncCallCount++;
            return FromValueTask(ExecuteValueTaskAsync(item, context, cancellationToken));
        }

        protected internal override ValueTask<int> ExecuteValueTaskAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            ExecuteValueTaskCallCount++;
            return ValueTask.FromResult(item + 1);
        }
    }

    private sealed class ThrowingTransform : TransformNode<int, int>
    {
        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("sequential boom");
        }
    }

    private sealed class RecordingSampleRecorder : IPipelineSampleRecorder
    {
        public List<RecordedError> Errors { get; } = [];

        public void RecordSample(string nodeId, string direction, Guid correlationId, int[]? ancestryInputIndices, object? serializedRecord,
            DateTimeOffset timestamp, string? pipelineName = null, Guid? runId = null, SampleOutcome outcome = SampleOutcome.Success,
            int retryCount = 0)
        {
        }

        public void RecordError(string nodeId, Guid correlationId, int[]? ancestryInputIndices, object? serializedRecord, string errorMessage,
            string? exceptionType, string? stackTrace, int retryCount = 0, string? pipelineName = null, Guid? runId = null,
            DateTimeOffset timestamp = default)
        {
            Errors.Add(new RecordedError(correlationId, ancestryInputIndices, errorMessage, retryCount));
        }
    }

    private sealed record RecordedError(Guid CorrelationId, int[]? AncestryInputIndices, string ErrorMessage, int RetryCount);
}
