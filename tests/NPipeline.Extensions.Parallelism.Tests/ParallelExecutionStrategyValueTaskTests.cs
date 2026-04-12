using AwesomeAssertions;
using NPipeline.Execution.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Sampling;
using NPipeline.Tests.Common;

namespace NPipeline.Extensions.Parallelism.Tests;

public sealed class ParallelExecutionStrategyValueTaskTests
{
    [Fact]
    public async Task Should_PreferValueTaskPath_WhenTransformOverrides()
    {
        await using InMemoryDataStream<int> input = new([1, 2, 3], "input");
        ValueTaskFriendlyTransform transform = new();
        ParallelExecutionStrategy strategy = new(1);
        var context = new PipelineContext();
        List<int> results = [];

        using (context.ScopedNode("transform"))
        {
            await using var output = await strategy.ExecuteAsync(input, transform, context, CancellationToken.None);

            await foreach (var value in output.WithCancellation(CancellationToken.None))
            {
                results.Add(value);
            }
        }

        _ = results.Should().BeEquivalentTo([2, 3, 4]);
        _ = transform.ExecuteAsyncCallCount.Should().Be(0);
        _ = transform.ExecuteValueTaskCallCount.Should().Be(3);
    }

    [Fact]
    public async Task DropOldest_Should_PreferValueTaskPath_WhenTransformOverrides()
    {
        await using InMemoryDataStream<int> input = new([10, 20, 30], "input");
        ValueTaskFriendlyTransform transform = new();
        DropOldestParallelStrategy strategy = new(1);
        var context = new PipelineContext();
        List<int> results = [];

        using (context.ScopedNode("transform"))
        {
            context.NodeExecutionAnnotations["transform"] = new ParallelOptions
            {
                MaxDegreeOfParallelism = 1,
                MaxQueueLength = 4,
                QueuePolicy = BoundedQueuePolicy.DropOldest,
            };

            await using var output = await strategy.ExecuteAsync(input, transform, context, CancellationToken.None);

            await foreach (var value in output.WithCancellation(CancellationToken.None))
            {
                results.Add(value);
            }
        }

        int[] expected = [11, 21, 31];
        _ = results.Should().BeEquivalentTo(expected);
        _ = transform.ExecuteAsyncCallCount.Should().Be(0);
        _ = transform.ExecuteValueTaskCallCount.Should().Be(3);
    }

    [Fact]
    public async Task DropNewest_Should_PreferValueTaskPath_WhenTransformOverrides()
    {
        await using InMemoryDataStream<int> input = new([7, 8, 9], "input");
        ValueTaskFriendlyTransform transform = new();
        DropNewestParallelStrategy strategy = new(1);
        var context = new PipelineContext();
        List<int> results = [];

        using (context.ScopedNode("transform"))
        {
            context.NodeExecutionAnnotations["transform"] = new ParallelOptions
            {
                MaxDegreeOfParallelism = 1,
                MaxQueueLength = 4,
                QueuePolicy = BoundedQueuePolicy.DropNewest,
            };

            await using var output = await strategy.ExecuteAsync(input, transform, context, CancellationToken.None);

            await foreach (var value in output.WithCancellation(CancellationToken.None))
            {
                results.Add(value);
            }
        }

        int[] expected = [8, 9, 10];
        _ = results.Should().BeEquivalentTo(expected);
        _ = transform.ExecuteAsyncCallCount.Should().Be(0);
        _ = transform.ExecuteValueTaskCallCount.Should().Be(3);
    }

    [Fact]
    public async Task Should_RecordErrorWithCorrelation_WhenParallelTransformThrows()
    {
        await using InMemoryDataStream<int> input = new([9], "input");
        ThrowingTransform transform = new();
        ParallelExecutionStrategy strategy = new(1);
        var context = new PipelineContext();
        var recorder = new RecordingSampleRecorder();
        var correlationId = Guid.NewGuid();

        context.Properties[PipelineContextKeys.SampleRecorder] = recorder;
        LineageExecutionItemContext.SetCurrentInputContext(0, correlationId, [1]);

        try
        {
            using (context.ScopedNode("transform"))
            {
                await using var output = await strategy.ExecuteAsync(input, transform, context, CancellationToken.None);

                var threw = false;

                try
                {
                    await foreach (var _ in output.WithCancellation(CancellationToken.None))
                    {
                    }
                }
                catch
                {
                    threw = true;
                }

                _ = threw.Should().BeTrue();
            }
        }
        finally
        {
            LineageExecutionItemContext.ClearCurrentInputIndex();
        }

        _ = recorder.Errors.Should().HaveCount(1);
        _ = recorder.Errors[0].CorrelationId.Should().Be(correlationId);
        _ = recorder.Errors[0].AncestryInputIndices.Should().BeEquivalentTo([1]);
        _ = recorder.Errors[0].RetryCount.Should().Be(0);
        _ = recorder.Errors[0].ErrorMessage.Should().Contain("parallel boom");
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
            throw new InvalidOperationException("parallel boom");
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

        public void RecordError(string nodeId, string originNodeId, Guid correlationId, int[]? ancestryInputIndices, object? serializedRecord, string errorMessage,
            string? exceptionType, string? stackTrace, int retryCount = 0, string? pipelineName = null, Guid? runId = null,
            DateTimeOffset timestamp = default)
        {
            Errors.Add(new RecordedError(correlationId, ancestryInputIndices, errorMessage, retryCount));
        }
    }

    private sealed record RecordedError(Guid CorrelationId, int[]? AncestryInputIndices, string ErrorMessage, int RetryCount);
}
