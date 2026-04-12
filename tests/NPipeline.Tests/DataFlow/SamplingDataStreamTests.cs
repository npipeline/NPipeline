using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Lineage;
using NPipeline.Sampling;

namespace NPipeline.Tests.DataFlow;

public sealed class SamplingDataStreamTests
{
    [Fact]
    public async Task Enumerate_WithLineageItemWithoutHops_RecordsSuccessWithZeroRetries()
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
    [InlineData(HopDecisionFlags.Error, SampleOutcome.Error)]
    [InlineData(HopDecisionFlags.FilteredOut, SampleOutcome.Skipped)]
    [InlineData(HopDecisionFlags.Retried, SampleOutcome.Success)]
    public async Task Enumerate_WithLineageHop_MapsOutcomeFromLatestHop(HopDecisionFlags flags, SampleOutcome expectedOutcome)
    {
        var recorder = new TestSampleRecorder();
        var packet = BuildPacket("node-a", flags, ancestryInputIndices: [3, 5]);
        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>([packet], "input");
        await using var sampled = new SamplingDataStream<LineagePacket<int>>(input, "node-a", "output", recorder, sampleRate: 1);

        await DrainAsync(sampled);

        _ = recorder.Calls.Should().HaveCount(1);
        _ = recorder.Calls[0].Outcome.Should().Be(expectedOutcome);
        _ = recorder.Calls[0].AncestryInputIndices.Should().BeEquivalentTo([3, 5]);
    }

    [Fact]
    public async Task Enumerate_WhenLatestHopIncludesDeadLetterAndError_PrefersDeadLetter()
    {
        var recorder = new TestSampleRecorder();
        var packet = BuildPacket("node-a", HopDecisionFlags.DeadLettered | HopDecisionFlags.Error | HopDecisionFlags.FilteredOut);
        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>([packet], "input");
        await using var sampled = new SamplingDataStream<LineagePacket<int>>(input, "node-a", "output", recorder, sampleRate: 1);

        await DrainAsync(sampled);

        _ = recorder.Calls.Should().HaveCount(1);
        _ = recorder.Calls[0].Outcome.Should().Be(SampleOutcome.DeadLetter);
    }

    [Fact]
    public async Task Enumerate_WithConsecutiveRetriedHopsForSameNode_CountsRetries()
    {
        var recorder = new TestSampleRecorder();
        var correlationId = Guid.NewGuid();
        var hops = ImmutableList.Create(
            new LineageHop("upstream", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, Guid.NewGuid()),
            new LineageHop("node-a", HopDecisionFlags.Retried, ObservedCardinality.One, 1, 1, null, false, Guid.NewGuid()),
            new LineageHop("node-a", HopDecisionFlags.Emitted | HopDecisionFlags.Retried, ObservedCardinality.One, 1, 1, null, false, Guid.NewGuid()));

        var packet = new LineagePacket<int>(42, correlationId, ImmutableList<string>.Empty)
        {
            LineageHops = hops,
        };

        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>([packet], "input");
        await using var sampled = new SamplingDataStream<LineagePacket<int>>(input, "node-a", "output", recorder, sampleRate: 1);

        await DrainAsync(sampled);

        _ = recorder.Calls.Should().HaveCount(1);
        _ = recorder.Calls[0].RetryCount.Should().Be(2);
        _ = recorder.Calls[0].Outcome.Should().Be(SampleOutcome.Success);
    }

    [Fact]
    public async Task Enumerate_WithExplicitHopRetryCount_UsesHopRetryCount()
    {
        var recorder = new TestSampleRecorder();
        var hop = new LineageHop("node-a", HopDecisionFlags.Emitted | HopDecisionFlags.Retried, ObservedCardinality.One, 1, 1, null, false,
            Guid.NewGuid(), RetryCount: 4);
        var packet = new LineagePacket<int>(42, Guid.NewGuid(), ImmutableList<string>.Empty)
        {
            LineageHops = ImmutableList.Create(hop),
        };

        await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>([packet], "input");
        await using var sampled = new SamplingDataStream<LineagePacket<int>>(input, "node-a", "output", recorder, sampleRate: 1);

        await DrainAsync(sampled);

        _ = recorder.Calls.Should().HaveCount(1);
        _ = recorder.Calls[0].RetryCount.Should().Be(4);
    }

    private static LineagePacket<int> BuildPacket(string nodeId, HopDecisionFlags flags, int[]? ancestryInputIndices = null)
    {
        var hop = new LineageHop(nodeId, flags, ObservedCardinality.One, 1, 1, ancestryInputIndices, false, Guid.NewGuid());
        return new LineagePacket<int>(42, Guid.NewGuid(), ImmutableList<string>.Empty)
        {
            LineageHops = ImmutableList.Create(hop),
        };
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
