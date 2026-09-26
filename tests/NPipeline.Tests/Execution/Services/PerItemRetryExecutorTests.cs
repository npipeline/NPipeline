using AwesomeAssertions;
using Microsoft.Extensions.Time.Testing;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Services;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Reliability;
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
        context.ExecutionConfiguration.ResiliencePolicy = resiliencePolicy;
        context.DeadLetterSink = deadLetterSink;
        var activity = new RecordingPipelineActivity();

        try
        {
            var result = await executor.ExecuteWithRetryAsync(
                7,
                transform,
                context,
                NodeId,
                Options(3),
                true,
                0,
                context.Lineage.Outcomes.GetWriter(NodeId),
                activity,
                CancellationToken.None);

            _ = result.Outcome.Should().Be(ItemExecutionOutcome.Skipped);
            _ = result.Produced.Should().BeFalse();
            _ = result.RetryCount.Should().Be(0);
            _ = resiliencePolicy.CallCount.Should().Be(1);
            _ = deadLetterSink.Envelopes.Should().BeEmpty();
            _ = activity.Exceptions.Should().HaveCount(1);

            _ = context.Lineage.Outcomes.GetWriter(NodeId).TryGetOutcome(0, out var outcome).Should().BeTrue();
            _ = outcome.OutcomeReason.Should().Be(LineageOutcomeReason.FilteredOut);
            _ = outcome.RetryCount.Should().Be(0);
        }
        finally
        {
            context.Lineage.Outcomes.ClearNode(NodeId);
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
        context.ExecutionConfiguration.ResiliencePolicy = resiliencePolicy;
        context.DeadLetterSink = deadLetterSink;

        try
        {
            var result = await executor.ExecuteWithRetryAsync(
                42,
                transform,
                context,
                NodeId,
                Options(2),
                true,
                0,
                context.Lineage.Outcomes.GetWriter(NodeId),
                null,
                CancellationToken.None);

            _ = result.Outcome.Should().Be(ItemExecutionOutcome.DeadLettered);
            _ = result.Produced.Should().BeFalse();
            _ = deadLetterSink.Envelopes.Should().HaveCount(1);
            _ = deadLetterSink.Envelopes[0].Item.Should().Be(42);
            _ = deadLetterSink.Envelopes[0].Error.Should().BeSameAs(transformException);
            _ = deadLetterSink.Envelopes[0].Attribution.DecisionNodeId.Should().Be(NodeId);

            _ = context.Lineage.Outcomes.GetWriter(NodeId).TryGetOutcome(0, out var outcome).Should().BeTrue();
            _ = outcome.OutcomeReason.Should().Be(LineageOutcomeReason.DeadLettered);
            _ = outcome.RetryCount.Should().Be(0);
        }
        finally
        {
            context.Lineage.Outcomes.ClearNode(NodeId);
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
        context.ExecutionConfiguration.ResiliencePolicy = resiliencePolicy;

        try
        {
            var result = await executor.ExecuteWithRetryAsync(
                10,
                transform,
                context,
                NodeId,
                Options(3),
                true,
                0,
                context.Lineage.Outcomes.GetWriter(NodeId),
                activity,
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

            _ = context.Lineage.Outcomes.GetWriter(NodeId).TryGetOutcome(0, out var outcome).Should().BeTrue();
            _ = outcome.OutcomeReason.Should().Be(LineageOutcomeReason.Emitted);
            _ = outcome.RetryCount.Should().Be(1);
        }
        finally
        {
            context.Lineage.Outcomes.ClearNode(NodeId);
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
        context.ExecutionConfiguration.ResiliencePolicy = resiliencePolicy;
        context.Properties[PipelineContextKeys.SampleRecorder] = recorder;
        var correlationId = Guid.NewGuid();
        context.Lineage.Outcomes.GetWriter(NodeId).RegisterInput(0, correlationId, [1, 2]);

        try
        {
            var act = async () => await executor.ExecuteWithRetryAsync(
                11,
                transform,
                context,
                NodeId,
                Options(0),
                true,
                0,
                context.Lineage.Outcomes.GetWriter(NodeId),
                null,
                CancellationToken.None);

            var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
            _ = thrown.Which.Should().BeSameAs(transformException);

            _ = recorder.Errors.Should().HaveCount(1);
            _ = recorder.Errors[0].CorrelationId.Should().Be(correlationId);
            _ = recorder.Errors[0].AncestryInputIndices.Should().BeEquivalentTo([1, 2]);
            _ = recorder.Errors[0].RetryCount.Should().Be(0);
            _ = recorder.Errors[0].ErrorMessage.Should().Contain("terminal");

            _ = context.Lineage.Outcomes.GetWriter(NodeId).TryGetOutcome(0, out var outcome).Should().BeTrue();
            _ = outcome.OutcomeReason.Should().Be(LineageOutcomeReason.Error);
            _ = outcome.RetryCount.Should().Be(0);
        }
        finally
        {
            context.Lineage.Outcomes.ClearNode(NodeId);
        }
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_RetryExhausted_ThrowsAndRecordsErrorWithMaxRetryCount()
    {
        var executor = PerItemRetryExecutor.Instance;
        var transform = new ScriptedTransform(new InvalidOperationException("first"), new InvalidOperationException("second"));

        // One retry, then Fail: the policy, not a cap in the executor, ends the retries.
        var resiliencePolicy = new SequenceDecisionPolicy(ResilienceDecision.Retry);
        var recorder = new RecordingSampleRecorder();

        var (context, pipelineId) = CreateTrackedContext();
        context.ExecutionConfiguration.ResiliencePolicy = resiliencePolicy;
        context.Properties[PipelineContextKeys.SampleRecorder] = recorder;
        context.Lineage.Outcomes.GetWriter(NodeId).RegisterInput(0, Guid.NewGuid(), [4]);

        try
        {
            var act = async () => await executor.ExecuteWithRetryAsync(
                18,
                transform,
                context,
                NodeId,
                Options(1),
                true,
                0,
                context.Lineage.Outcomes.GetWriter(NodeId),
                null,
                CancellationToken.None);

            // C4: exhaustion is a RetryExhaustedException, as at the other two layers.
            var thrown = await act.Should().ThrowAsync<RetryExhaustedException>();
            _ = thrown.Which.NodeId.Should().Be(NodeId);
            _ = thrown.Which.Message.Should().Contain("after 2 attempts");
            _ = thrown.Which.InnerException.Should().NotBeNull();
            _ = thrown.Which.InnerException!.Message.Should().Be("second");

            _ = recorder.Errors.Should().HaveCount(1);
            _ = recorder.Errors[0].RetryCount.Should().Be(1);

            _ = context.Lineage.Outcomes.GetWriter(NodeId).TryGetOutcome(0, out var outcome).Should().BeTrue();
            _ = outcome.OutcomeReason.Should().Be(LineageOutcomeReason.Error);
            _ = outcome.RetryCount.Should().Be(1);
        }
        finally
        {
            context.Lineage.Outcomes.ClearNode(NodeId);
        }
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_ClassifierThatThrows_FreesTheBreakerPermit()
    {
        var time = new FakeTimeProvider();
        var breaker = new CircuitBreaker(
            NodeId,
            new CircuitBreakerOptions { ConsecutiveFailures = 1, OpenDuration = TimeSpan.FromSeconds(30) },
            time);

        // Trip the breaker, then let it half-open so the next attempt takes its only probe slot.
        breaker.TryAcquire(out var trip, out _).Should().BeTrue();
        _ = breaker.RecordFailure(trip);
        time.Advance(TimeSpan.FromSeconds(30));

        var classifier = RetryClassifier.Default.Transient<TimeoutException>(_ => throw new InvalidOperationException("classifier boom"));

        var options = PipelineResilienceOptions.None with
        {
            ItemRetry = new ItemRetryOptions { MaxRetries = 0, Classifier = classifier },
        };

        var executor = PerItemRetryExecutor.Instance;
        var transform = new ScriptedTransform(new TimeoutException("transient"));
        var (context, pipelineId) = CreateTrackedContext();

        try
        {
            var act = async () => await executor.ExecuteWithRetryAsync(
                1,
                transform,
                context,
                NodeId,
                options,
                false,
                0,
                default,
                null,
                CancellationToken.None,
                circuitBreaker: breaker);

            _ = await act.Should().ThrowAsync<InvalidOperationException>("the classifier's own failure surfaces");

            // Without the release, the probe slot stays in use and every later attempt is refused.
            breaker.TryAcquire(out var next, out _).Should().BeTrue("a throwing classifier must not wedge the breaker half-open");
            next.IsProbe.Should().BeTrue();
        }
        finally
        {
            context.Lineage.Outcomes.ClearNode(NodeId);
        }
    }

    private static PipelineResilienceOptions Options(int maxRetries) =>
        PipelineResilienceOptions.None with { ItemRetry = new ItemRetryOptions { MaxRetries = maxRetries } };

    private static (PipelineContext Context, Guid PipelineId) CreateTrackedContext()
    {
        var context = new PipelineContext();
        var pipelineId = Guid.NewGuid();

        context.RunIdentity.PipelineId = pipelineId;
        context.RunIdentity.RunId = Guid.NewGuid();
        context.Lineage.Outcomes.BeginNode(NodeId);

        return (context, pipelineId);
    }

    private sealed class ScriptedTransform(params object[] outcomes) : TransformNode<int, int>
    {
        private readonly Queue<object> _outcomes = new(outcomes);

        public int InvocationCount { get; private set; }

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            InvocationCount++;

            if (_outcomes.Count == 0)
                return ValueTask.FromResult(item);

            var outcome = _outcomes.Dequeue();

            if (outcome is Exception exception)
                throw exception;

            return ValueTask.FromResult((int)outcome);
        }
    }

    private sealed class SequenceDecisionPolicy(params ResilienceDecision[] decisions)
        : IResiliencePolicy
    {
        private readonly Queue<ResilienceDecision> _decisions = new(decisions);

        public int CallCount { get; private set; }

        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            CallCount++;

            var decision = _decisions.Count > 0
                ? _decisions.Dequeue()
                : ResilienceDecision.Fail;

            return ValueTask.FromResult(decision);
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
