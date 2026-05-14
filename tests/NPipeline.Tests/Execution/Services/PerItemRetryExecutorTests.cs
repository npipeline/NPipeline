using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Resilience;
using NPipeline.Sampling;

namespace NPipeline.Tests.Execution.Services;

public sealed class PerItemRetryExecutorTests
{
    private const string NodeId = "transform";

    [Fact]
    public async Task ExecuteWithRetryAsync_SkipDecision_ReturnsSkippedAndRecordsFilteredOutcome()
    {
        var executor = PerItemRetryExecutor.Instance;
        var transformException = new InvalidOperationException("skip-me");
        var transform = new ScriptedTransform(transformException);
        var resiliencePolicy = new SequenceDecisionPolicy(ResilienceDecision.Skip);
        var deadLetterSink = new RecordingDeadLetterSink();

        var (context, pipelineId) = CreateTrackedContext();
        context.ResiliencePolicy = resiliencePolicy;
        context.DeadLetterSink = deadLetterSink;
        var activity = new RecordingPipelineActivity();

        try
        {
            var result = await executor.ExecuteWithRetryAsync(
                item: 7,
                node: transform,
                valueTaskTransform: null,
                context,
                NodeId,
                maxItemRetries: 3,
                hasLineageIndex: true,
                lineageInputIndex: 0,
                itemActivity: activity,
                CancellationToken.None);

            _ = result.Outcome.Should().Be(ItemExecutionOutcome.Skipped);
            _ = result.Produced.Should().BeFalse();
            _ = result.RetryCount.Should().Be(0);
            _ = resiliencePolicy.CallCount.Should().Be(1);
            _ = deadLetterSink.Envelopes.Should().BeEmpty();
            _ = activity.Exceptions.Should().HaveCount(1);

            _ = LineageNodeOutcomeRegistry.TryGet(pipelineId, NodeId, 0, out var outcome).Should().BeTrue();
            _ = outcome.OutcomeReason.Should().Be(LineageOutcomeReason.FilteredOut);
            _ = outcome.RetryCount.Should().Be(0);
        }
        finally
        {
            LineageNodeOutcomeRegistry.ClearNode(pipelineId, NodeId);
            LineageExecutionItemContext.ClearCurrentInputIndex();
        }
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_DeadLetterDecision_SendsEnvelopeAndRecordsDeadLetteredOutcome()
    {
        var executor = PerItemRetryExecutor.Instance;
        var transformException = new InvalidOperationException("dead-letter-me");
        var transform = new ScriptedTransform(transformException);
        var resiliencePolicy = new SequenceDecisionPolicy(ResilienceDecision.DeadLetter);
        var deadLetterSink = new RecordingDeadLetterSink();

        var (context, pipelineId) = CreateTrackedContext();
        context.ResiliencePolicy = resiliencePolicy;
        context.DeadLetterSink = deadLetterSink;

        try
        {
            var result = await executor.ExecuteWithRetryAsync(
                item: 42,
                node: transform,
                valueTaskTransform: null,
                context,
                NodeId,
                maxItemRetries: 2,
                hasLineageIndex: true,
                lineageInputIndex: 0,
                itemActivity: null,
                CancellationToken.None);

            _ = result.Outcome.Should().Be(ItemExecutionOutcome.DeadLettered);
            _ = result.Produced.Should().BeFalse();
            _ = deadLetterSink.Envelopes.Should().HaveCount(1);
            _ = deadLetterSink.Envelopes[0].Item.Should().Be(42);
            _ = deadLetterSink.Envelopes[0].Error.Should().BeSameAs(transformException);
            _ = deadLetterSink.Envelopes[0].Attribution.DecisionNodeId.Should().Be(NodeId);

            _ = LineageNodeOutcomeRegistry.TryGet(pipelineId, NodeId, 0, out var outcome).Should().BeTrue();
            _ = outcome.OutcomeReason.Should().Be(LineageOutcomeReason.DeadLettered);
            _ = outcome.RetryCount.Should().Be(0);
        }
        finally
        {
            LineageNodeOutcomeRegistry.ClearNode(pipelineId, NodeId);
            LineageExecutionItemContext.ClearCurrentInputIndex();
        }
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_RetryThenSuccess_ReturnsEmittedAndTracksRetryCount()
    {
        var executor = PerItemRetryExecutor.Instance;
        var transform = new ScriptedTransform(new InvalidOperationException("transient"), 99);
        var resiliencePolicy = new SequenceDecisionPolicy(ResilienceDecision.Retry);
        var activity = new RecordingPipelineActivity();

        var (context, pipelineId) = CreateTrackedContext();
        context.ResiliencePolicy = resiliencePolicy;

        try
        {
            var result = await executor.ExecuteWithRetryAsync(
                item: 10,
                node: transform,
                valueTaskTransform: null,
                context,
                NodeId,
                maxItemRetries: 3,
                hasLineageIndex: true,
                lineageInputIndex: 0,
                itemActivity: activity,
                CancellationToken.None);

            _ = result.Outcome.Should().Be(ItemExecutionOutcome.Emitted);
            _ = result.Produced.Should().BeTrue();
            _ = result.Output.Should().Be(99);
            _ = result.RetryCount.Should().Be(1);
            _ = transform.InvocationCount.Should().Be(2);
            _ = resiliencePolicy.CallCount.Should().Be(1);
            _ = activity.Exceptions.Should().HaveCount(1);
            _ = activity.Tags.Should().ContainKey("retry.attempt");
            _ = activity.Tags["retry.attempt"].Should().Be("1");

            _ = LineageNodeOutcomeRegistry.TryGet(pipelineId, NodeId, 0, out var outcome).Should().BeTrue();
            _ = outcome.OutcomeReason.Should().Be(LineageOutcomeReason.Emitted);
            _ = outcome.RetryCount.Should().Be(1);
        }
        finally
        {
            LineageNodeOutcomeRegistry.ClearNode(pipelineId, NodeId);
            LineageExecutionItemContext.ClearCurrentInputIndex();
        }
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_FailDecision_ThrowsAndRecordsSampleError()
    {
        var executor = PerItemRetryExecutor.Instance;
        var transformException = new InvalidOperationException("terminal");
        var transform = new ScriptedTransform(transformException);
        var resiliencePolicy = new SequenceDecisionPolicy(ResilienceDecision.Fail);
        var recorder = new RecordingSampleRecorder();

        var (context, pipelineId) = CreateTrackedContext();
        context.ResiliencePolicy = resiliencePolicy;
        context.Properties[PipelineContextKeys.SampleRecorder] = recorder;
        LineageExecutionItemContext.SetCurrentInputContext(0, Guid.NewGuid(), [1, 2]);

        try
        {
            var act = async () => await executor.ExecuteWithRetryAsync(
                item: 11,
                node: transform,
                valueTaskTransform: null,
                context,
                NodeId,
                maxItemRetries: 0,
                hasLineageIndex: true,
                lineageInputIndex: 0,
                itemActivity: null,
                CancellationToken.None);

            var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
            _ = thrown.Which.Should().BeSameAs(transformException);

            _ = recorder.Errors.Should().HaveCount(1);
            _ = recorder.Errors[0].RetryCount.Should().Be(0);
            _ = recorder.Errors[0].ErrorMessage.Should().Contain("terminal");

            _ = LineageNodeOutcomeRegistry.TryGet(pipelineId, NodeId, 0, out var outcome).Should().BeTrue();
            _ = outcome.OutcomeReason.Should().Be(LineageOutcomeReason.Error);
            _ = outcome.RetryCount.Should().Be(0);
        }
        finally
        {
            LineageNodeOutcomeRegistry.ClearNode(pipelineId, NodeId);
            LineageExecutionItemContext.ClearCurrentInputIndex();
        }
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_RetryExhausted_ThrowsAndRecordsErrorWithMaxRetryCount()
    {
        var executor = PerItemRetryExecutor.Instance;
        var transform = new ScriptedTransform(new InvalidOperationException("first"), new InvalidOperationException("second"));
        var resiliencePolicy = new SequenceDecisionPolicy(ResilienceDecision.Retry, ResilienceDecision.Retry);
        var recorder = new RecordingSampleRecorder();

        var (context, pipelineId) = CreateTrackedContext();
        context.ResiliencePolicy = resiliencePolicy;
        context.Properties[PipelineContextKeys.SampleRecorder] = recorder;
        LineageExecutionItemContext.SetCurrentInputContext(0, Guid.NewGuid(), [4]);

        try
        {
            var act = async () => await executor.ExecuteWithRetryAsync(
                item: 18,
                node: transform,
                valueTaskTransform: null,
                context,
                NodeId,
                maxItemRetries: 1,
                hasLineageIndex: true,
                lineageInputIndex: 0,
                itemActivity: null,
                CancellationToken.None);

            var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
            _ = thrown.Which.Message.Should().Contain("after 2 attempts");
            _ = thrown.Which.InnerException.Should().NotBeNull();
            _ = thrown.Which.InnerException!.Message.Should().Be("second");

            _ = recorder.Errors.Should().HaveCount(1);
            _ = recorder.Errors[0].RetryCount.Should().Be(1);

            _ = LineageNodeOutcomeRegistry.TryGet(pipelineId, NodeId, 0, out var outcome).Should().BeTrue();
            _ = outcome.OutcomeReason.Should().Be(LineageOutcomeReason.Error);
            _ = outcome.RetryCount.Should().Be(1);
        }
        finally
        {
            LineageNodeOutcomeRegistry.ClearNode(pipelineId, NodeId);
            LineageExecutionItemContext.ClearCurrentInputIndex();
        }
    }

    private static (PipelineContext Context, Guid PipelineId) CreateTrackedContext()
    {
        var context = new PipelineContext();
        var pipelineId = Guid.NewGuid();

        context.PipelineId = pipelineId;
        context.RunId = Guid.NewGuid();
        LineageNodeOutcomeRegistry.BeginNode(pipelineId, NodeId);

        return (context, pipelineId);
    }

    private sealed class ScriptedTransform(params object[] outcomes) : TransformNode<int, int>
    {
        private readonly Queue<object> _outcomes = new(outcomes);

        public int InvocationCount { get; private set; }

        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            InvocationCount++;

            if (_outcomes.Count == 0)
                return Task.FromResult(item);

            var outcome = _outcomes.Dequeue();

            if (outcome is Exception exception)
                throw exception;

            return Task.FromResult((int)outcome);
        }
    }

    private sealed class SequenceDecisionPolicy(params ResilienceDecision[] decisions)
        : IResiliencePolicy
    {
        private readonly Queue<ResilienceDecision> _decisions = new(decisions);

        public int CallCount { get; private set; }

        public Task<ResilienceDecision> DecideNodeFailureAsync(
            NodeDefinition nodeDefinition,
            INode node,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(
            string nodeId,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            CallCount++;

            var decision = _decisions.Count > 0
                ? _decisions.Dequeue()
                : ResilienceDecision.Fail;

            return Task.FromResult(decision);
        }

        public ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
        {
            return context.GetRetryDelayStrategy().GetDelayAsync(attemptNumber, cancellationToken);
        }

        public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
        {
            return DefaultResiliencePolicy.Instance.GetCircuitBreaker(context, nodeId);
        }
    }

    private sealed class RecordingDeadLetterSink : IDeadLetterSink
    {
        public List<DeadLetterEnvelope> Envelopes { get; } = [];

        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
        {
            Envelopes.Add(envelope);
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingPipelineActivity : IPipelineActivity
    {
        public List<Exception> Exceptions { get; } = [];

        public Dictionary<string, object> Tags { get; } = [];

        public void SetTag(string key, object value)
        {
            Tags[key] = value;
        }

        public void RecordException(Exception exception)
        {
            Exceptions.Add(exception);
        }

        public void Dispose()
        {
        }
    }

    private sealed class RecordingSampleRecorder : IPipelineSampleRecorder
    {
        public List<RecordedError> Errors { get; } = [];

        public void RecordSample(
            string nodeId,
            string direction,
            Guid correlationId,
            int[]? ancestryInputIndices,
            object? serializedRecord,
            DateTimeOffset timestamp,
            string? pipelineName = null,
            Guid? runId = null,
            SampleOutcome outcome = SampleOutcome.Success,
            int retryCount = 0)
        {
        }

        public void RecordError(
            string nodeId,
            string originNodeId,
            Guid correlationId,
            int[]? ancestryInputIndices,
            object? serializedRecord,
            string errorMessage,
            string? exceptionType,
            string? stackTrace,
            int retryCount = 0,
            string? pipelineName = null,
            Guid? runId = null,
            DateTimeOffset timestamp = default)
        {
            Errors.Add(new RecordedError(correlationId, ancestryInputIndices, errorMessage, retryCount));
        }
    }

    private sealed record RecordedError(Guid CorrelationId, int[]? AncestryInputIndices, string ErrorMessage, int RetryCount);
}
