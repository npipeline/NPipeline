using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Lineage;
using NPipeline.Sampling;

namespace NPipeline.Tests.DataFlow;

public sealed class SamplingDataStreamTests
{
    [Fact]
    public async Task Enumerate_WithLineageItemWithoutRecords_RecordsSuccessWithZeroRetries()
    {
        var recorder = new TestSampleRecorder();
        var packet = new LineagePacket<int>(42, Guid.NewGuid(), ImmutableList<string>.Empty);
        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>([packet], "input");
        await using var sampled = new SamplingDataStream<LineagePacket<int>>(input, "node-a", "output", recorder, sampleRate: 1);

        await DrainAsync(sampled);

        _ = recorder.Calls.Should().HaveCount(1);
        _ = recorder.Calls[0].Outcome.Should().Be(SampleOutcome.Success);
        _ = recorder.Calls[0].RetryCount.Should().Be(0);
    }

    [Theory]
    [InlineData(LineageOutcomeReason.Error, SampleOutcome.Error)]
    [InlineData(LineageOutcomeReason.FilteredOut, SampleOutcome.Skipped)]
    [InlineData(LineageOutcomeReason.Emitted, SampleOutcome.Success)]
    public async Task Enumerate_WithLineageRecord_MapsOutcomeFromLatestRecord(LineageOutcomeReason reason, SampleOutcome expectedOutcome)
    {
        var recorder = new TestSampleRecorder();
        var packet = BuildPacket("node-a", reason, retryCount: 1, contributorInputIndices: [3, 5]);
        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>([packet], "input");
        await using var sampled = new SamplingDataStream<LineagePacket<int>>(input, "node-a", "output", recorder, sampleRate: 1);

        await DrainAsync(sampled);

        _ = recorder.Calls.Should().HaveCount(1);
        _ = recorder.Calls[0].Outcome.Should().Be(expectedOutcome);
        _ = recorder.Calls[0].AncestryInputIndices.Should().BeEquivalentTo([3, 5]);
        _ = recorder.Calls[0].RetryCount.Should().Be(1);
    }

    [Fact]
    public async Task Enumerate_WhenLatestRecordIsDeadLettered_RecordsDeadLetterOutcome()
    {
        var recorder = new TestSampleRecorder();
        var packet = BuildPacket("node-a", LineageOutcomeReason.DeadLettered);
        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>([packet], "input");
        await using var sampled = new SamplingDataStream<LineagePacket<int>>(input, "node-a", "output", recorder, sampleRate: 1);

        await DrainAsync(sampled);

        _ = recorder.Calls.Should().HaveCount(1);
        _ = recorder.Calls[0].Outcome.Should().Be(SampleOutcome.DeadLetter);
    }

    [Fact]
    public async Task Enumerate_WithConsecutiveRecords_UsesLatestRetryCount()
    {
        var recorder = new TestSampleRecorder();
        var correlationId = Guid.NewGuid();
        var records = ImmutableList.Create(
            BuildRecord(correlationId, "upstream", LineageOutcomeReason.Emitted, retryCount: null),
            BuildRecord(correlationId, "node-a", LineageOutcomeReason.Emitted, retryCount: 1),
            BuildRecord(correlationId, "node-a", LineageOutcomeReason.Emitted, retryCount: 2));

        var packet = new LineagePacket<int>(42, correlationId, ImmutableList<string>.Empty)
        {
            LineageRecords = records,
        };

        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>([packet], "input");
        await using var sampled = new SamplingDataStream<LineagePacket<int>>(input, "node-a", "output", recorder, sampleRate: 1);

        await DrainAsync(sampled);

        _ = recorder.Calls.Should().HaveCount(1);
        _ = recorder.Calls[0].RetryCount.Should().Be(2);
        _ = recorder.Calls[0].Outcome.Should().Be(SampleOutcome.Success);
    }

    [Fact]
    public async Task Enumerate_WithExplicitRecordRetryCount_UsesRecordRetryCount()
    {
        var recorder = new TestSampleRecorder();
        var correlationId = Guid.NewGuid();
        var packet = new LineagePacket<int>(42, correlationId, ImmutableList<string>.Empty)
        {
            LineageRecords = ImmutableList.Create(BuildRecord(correlationId, "node-a", LineageOutcomeReason.Emitted, retryCount: 4)),
        };

        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>([packet], "input");
        await using var sampled = new SamplingDataStream<LineagePacket<int>>(input, "node-a", "output", recorder, sampleRate: 1);

        await DrainAsync(sampled);

        _ = recorder.Calls.Should().HaveCount(1);
        _ = recorder.Calls[0].RetryCount.Should().Be(4);
    }

    private static LineagePacket<int> BuildPacket(string nodeId, LineageOutcomeReason reason, int? retryCount = null,
        int[]? contributorInputIndices = null)
    {
        var correlationId = Guid.NewGuid();
        return new LineagePacket<int>(42, correlationId, ImmutableList<string>.Empty)
        {
            LineageRecords = ImmutableList.Create(BuildRecord(correlationId, nodeId, reason, retryCount, contributorInputIndices)),
        };
    }

    private static LineageRecord BuildRecord(Guid correlationId, string nodeId, LineageOutcomeReason reason, int? retryCount = null,
        int[]? contributorInputIndices = null)
    {
        return new LineageRecord(
            correlationId,
            nodeId,
            Guid.NewGuid(),
            reason,
            false,
            ImmutableList<string>.Empty,
            RetryCount: retryCount,
            ContributorInputIndices: contributorInputIndices,
            Cardinality: ObservedCardinality.One);
    }

    private static async Task DrainAsync<T>(IAsyncEnumerable<T> stream)
    {
        await foreach (var _ in stream.ConfigureAwait(false))
        {
        }
    }

    private sealed class TestSampleRecorder : IPipelineSampleRecorder
    {
        public List<SampleCall> Calls { get; } = [];

        public void RecordSample(string nodeId, string direction, Guid correlationId, int[]? ancestryInputIndices, object? serializedRecord,
            DateTimeOffset timestamp, string? pipelineName = null, Guid? runId = null, SampleOutcome outcome = SampleOutcome.Success,
            int retryCount = 0)
        {
            Calls.Add(new SampleCall(nodeId, direction, correlationId, ancestryInputIndices, serializedRecord, timestamp, pipelineName, runId, outcome,
                retryCount));
        }

        public void RecordError(string nodeId, string originNodeId, Guid correlationId, int[]? ancestryInputIndices, object? serializedRecord, string errorMessage,
            string? exceptionType, string? stackTrace, int retryCount = 0, string? pipelineName = null, Guid? runId = null,
            DateTimeOffset timestamp = default)
        {
        }
    }

    private sealed record SampleCall(
        string NodeId,
        string Direction,
        Guid CorrelationId,
        int[]? AncestryInputIndices,
        object? SerializedRecord,
        DateTimeOffset Timestamp,
        string? PipelineName,
        Guid? RunId,
        SampleOutcome Outcome,
        int RetryCount);
}
