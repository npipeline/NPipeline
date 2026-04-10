// ReSharper disable ClassNeverInstantiated.Local

using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Lineage.Strategies;
using NPipeline.Lineage;

namespace NPipeline.Tests.Lineage;

/// <summary>
///     Comprehensive tests for lineage mapping strategies.
///     Tests StreamingOneToOneStrategy, PositionalStreamingStrategy, MaterializingStrategy,
///     CapAwareMaterializingStrategy, and LineageMappingHelpers.
/// </summary>
public sealed class LineageMappingStrategiesTests
{
    private static readonly Guid s_pipelineId = Guid.Parse("11111111-1111-1111-1111-111111111111");

    #region Helper Methods

    private static async IAsyncEnumerable<LineagePacket<T>> CreatePacketStream<T>(params T[] items)
    {
        var index = 0;

        foreach (var item in items)
        {
            await Task.Yield();

            yield return new LineagePacket<T>(
                item,
                Guid.NewGuid(),
                ImmutableList.Create($"node_{index}"))
            {
                Collect = true,
                LineageHops = ImmutableList<LineageHop>.Empty,
            };

            index++;
        }
    }

    private static IAsyncEnumerable<T> CreateDataStream<T>(params T[] items)
    {
        return items.ToAsyncEnumerable();
    }

    private static LineageOptions CreateOptions(
        bool strict = false,
        bool warnOnMismatch = false,
        int? maxHopRecords = null,
        int? materializationCap = null,
        LineageOverflowPolicy overflowPolicy = LineageOverflowPolicy.Degrade)
    {
        return new LineageOptions(
            strict, // Strict
            warnOnMismatch, // WarnOnMismatch
            null, // OnMismatch
            materializationCap, // MaterializationCap
            overflowPolicy, // OverflowPolicy
            true, // CaptureHopTimestamps
            true, // CaptureDecisions
            true, // CaptureObservedCardinality
            true, // CaptureAncestryMapping
            false, // CaptureHopSnapshots
            100, // SampleEvery
            true, // DeterministicSampling
            true, // RedactData
            maxHopRecords ?? 100); // MaxHopRecordsPerItem
    }

    private sealed class OneToThreeMapper : ILineageMapper
    {
        public LineageMappingResult MapInputToOutputs(IReadOnlyList<object> inputPackets, IReadOnlyList<object> outputs, LineageMappingContext context)
        {
            return new LineageMappingResult(
            [
                new LineageMappingRecord(0, [0]),
                new LineageMappingRecord(1, [0]),
                new LineageMappingRecord(2, [0]),
            ]);
        }
    }

    private sealed class ConflictingFanOutMapper : ILineageMapper
    {
        public LineageMappingResult MapInputToOutputs(IReadOnlyList<object> inputPackets, IReadOnlyList<object> outputs, LineageMappingContext context)
        {
            return new LineageMappingResult(
            [
                new LineageMappingRecord(0, [0]),
                new LineageMappingRecord(1, [0, 1]),
                new LineageMappingRecord(2, [1]),
                new LineageMappingRecord(3, [1]),
            ]);
        }
    }

    #endregion

    #region StreamingOneToOneStrategy Tests

    [Fact]
    public async Task StreamingOneToOneStrategy_OneToOne_Match_EmitsCorrectLineagePackets()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2, 3);
        var outputData = CreateDataStream("a", "b", "c");
        ILineageMappingStrategy<int, string> strategy = StreamingOneToOneStrategy<int, string>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(3);
        _ = results[0].Data.Should().Be("a");
        _ = results[1].Data.Should().Be("b");
        _ = results[2].Data.Should().Be("c");
        _ = results[0].CorrelationId.Should().NotBe(Guid.Empty);
        _ = results[0].TraversalPath.Should().Contain($"{s_pipelineId:N}::test_node");
    }

    [Fact]
    public async Task StreamingOneToOneStrategy_InputExceedsOutput_OneToOneMode_Throws()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2, 3, 4);
        var outputData = CreateDataStream("a", "b", "c");
        ILineageMappingStrategy<int, string> strategy = StreamingOneToOneStrategy<int, string>.Instance;
        var options = CreateOptions(true);

        // Act & Assert
        var act = async () =>
        {
            await foreach (var _ in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                               CancellationToken.None))
            {
                // Enumerate to trigger mismatch
            }
        };

        _ = await act.Should().ThrowAsync<InvalidOperationException>()
            .Where(ex => ex.Message.Contains("cardinality mismatch"));
    }

    [Fact]
    public async Task StreamingOneToOneStrategy_OutputExceedsInput_OneToOneMode_Throws()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2);
        var outputData = CreateDataStream("a", "b", "c");
        ILineageMappingStrategy<int, string> strategy = StreamingOneToOneStrategy<int, string>.Instance;
        var options = CreateOptions(true);

        // Act & Assert
        var act = async () =>
        {
            await foreach (var _ in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                               CancellationToken.None))
            {
                // Enumerate to trigger mismatch
            }
        };

        _ = await act.Should().ThrowAsync<InvalidOperationException>()
            .Where(ex => ex.Message.Contains("cardinality mismatch"));
    }

    [Fact]
    public async Task StreamingOneToOneStrategy_CardinalityNotOneToOne_AllowsMismatch()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2);
        var outputData = CreateDataStream("a", "b", "c");
        ILineageMappingStrategy<int, string> strategy = StreamingOneToOneStrategy<int, string>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToMany, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(2);
    }

    [Fact]
    public async Task StreamingOneToOneStrategy_EmptyStreams_EmitsNothing()
    {
        // Arrange
        var inputPackets = CreatePacketStream<int>();
        var outputData = CreateDataStream<string>();
        ILineageMappingStrategy<int, string> strategy = StreamingOneToOneStrategy<int, string>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().BeEmpty();
    }

    [Fact]
    public async Task StreamingOneToOneStrategy_PreservesLineageMetadata()
    {
        // Arrange
        var correlationId = Guid.NewGuid();

        LineagePacket<int>[] packets =
        [
            new(42, correlationId, ImmutableList.Create("input_node"))
            {
                Collect = true,
                LineageHops = ImmutableList<LineageHop>.Empty,
            },
        ];

        var inputPackets = packets.ToAsyncEnumerable();
        var outputData = CreateDataStream("answer");
        ILineageMappingStrategy<int, string> strategy = StreamingOneToOneStrategy<int, string>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "transform_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(1);
        _ = results[0].CorrelationId.Should().Be(correlationId);
        _ = results[0].Collect.Should().BeTrue();
    }

    [Fact]
    public async Task StreamingOneToOneStrategy_MaxHopRecordsCap_TruncatesLineageHops()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1);
        var outputData = CreateDataStream("a");
        ILineageMappingStrategy<int, string> strategy = StreamingOneToOneStrategy<int, string>.Instance;
        var options = CreateOptions(maxHopRecords: 1);

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(1);
        _ = results[0].LineageHops.Should().HaveCount(1);
    }

    [Fact]
    public async Task StreamingOneToOneStrategy_HopsSetOutputEmissionCountToOne()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2, 3);
        var outputData = CreateDataStream("a", "b", "c");
        ILineageMappingStrategy<int, string> strategy = StreamingOneToOneStrategy<int, string>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(3);

        foreach (var packet in results)
        {
            _ = packet.LineageHops.Should().HaveCount(1);
            _ = packet.LineageHops[0].OutputEmissionCount.Should().Be(1);
        }
    }

    [Fact]
    public async Task StreamingOneToOneStrategy_ShouldPropagateRecordedRetryOutcome()
    {
        // Arrange
        LineageNodeOutcomeRegistry.BeginNode(s_pipelineId, "test_node");
        LineageNodeOutcomeRegistry.Record(s_pipelineId, "test_node", 0, HopDecisionFlags.Emitted | HopDecisionFlags.Retried, 3);

        var inputPackets = CreatePacketStream(1);
        var outputData = CreateDataStream("a");
        ILineageMappingStrategy<int, string> strategy = StreamingOneToOneStrategy<int, string>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options,
                           null, null, CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(1);
        _ = results[0].LineageHops.Should().HaveCount(1);
        _ = results[0].LineageHops[0].Outcome.HasFlag(HopDecisionFlags.Retried).Should().BeTrue();
        _ = results[0].LineageHops[0].RetryCount.Should().Be(3);

        // Cleanup for isolation
        LineageNodeOutcomeRegistry.ClearNode(s_pipelineId, "test_node");
    }

    #endregion

    #region PositionalStreamingStrategy Tests

    [Fact]
    public async Task PositionalStreamingStrategy_OneToOne_Match_EmitsCorrectPackets()
    {
        // Arrange
        var inputPackets = CreatePacketStream(10, 20, 30);
        var outputData = CreateDataStream(100, 200, 300);
        ILineageMappingStrategy<int, int> strategy = PositionalStreamingStrategy<int, int>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<int>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(3);
        _ = results.Select(p => p.Data).Should().ContainInOrder(100, 200, 300);
    }

    [Fact]
    public async Task PositionalStreamingStrategy_MismatchCount_WithOneToOne_Throws()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2, 3);
        var outputData = CreateDataStream(10, 20);
        ILineageMappingStrategy<int, int> strategy = PositionalStreamingStrategy<int, int>.Instance;
        var options = CreateOptions(true);

        // Act & Assert
        var act = async () =>
        {
            await foreach (var _ in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                               CancellationToken.None))
            {
                // Enumerate to trigger
            }
        };

        _ = await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task PositionalStreamingStrategy_EmptyInputs_NoOutput()
    {
        // Arrange
        var inputPackets = CreatePacketStream<int>();
        var outputData = CreateDataStream<int>();
        ILineageMappingStrategy<int, int> strategy = PositionalStreamingStrategy<int, int>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<int>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().BeEmpty();
    }

    #endregion

    #region MaterializingStrategy Tests

    [Fact]
    public async Task MaterializingStrategy_BuffersAllInputsAndOutputs()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2, 3);
        var outputData = CreateDataStream("a", "b", "c");
        ILineageMappingStrategy<int, string> strategy = MaterializingStrategy<int, string>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(3);
        _ = results.Select(p => p.Data).Should().ContainInOrder("a", "b", "c");
    }

    [Fact]
    public async Task MaterializingStrategy_EmptyStreams_NoResults()
    {
        // Arrange
        var inputPackets = CreatePacketStream<int>();
        var outputData = CreateDataStream<string>();
        ILineageMappingStrategy<int, string> strategy = MaterializingStrategy<int, string>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().BeEmpty();
    }

    [Fact]
    public async Task MaterializingStrategy_LargeDataSet_ProcessesCorrectly()
    {
        // Arrange
        var inputs = Enumerable.Range(1, 1000).ToArray();
        var inputPackets = CreatePacketStream(inputs);
        var outputs = Enumerable.Range(1000, 1000).ToArray();
        var outputData = CreateDataStream(outputs);
        ILineageMappingStrategy<int, int> strategy = MaterializingStrategy<int, int>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<int>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(1000);
        _ = results[0].Data.Should().Be(1000);
        _ = results[999].Data.Should().Be(1999);
    }

    [Fact]
    public async Task MaterializingStrategy_CardinalityMismatch_OneToOne_Strict()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2, 3);
        var outputData = CreateDataStream("a", "b");
        ILineageMappingStrategy<int, string> strategy = MaterializingStrategy<int, string>.Instance;
        var options = CreateOptions(true);

        // Act & Assert
        var act = async () =>
        {
            await foreach (var _ in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                               CancellationToken.None))
            {
                // Enumerate to trigger
            }
        };

        _ = await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task MaterializingStrategy_MapperFanOut_SetsOutputEmissionCountOnEachSiblingOutput()
    {
        // Arrange
        var inputPackets = CreatePacketStream("input-0");
        var outputData = CreateDataStream("out-0", "out-1", "out-2");
        ILineageMappingStrategy<string, string> strategy = MaterializingStrategy<string, string>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "fanout_node", s_pipelineId, null, TransformCardinality.OneToMany, options,
                           typeof(OneToThreeMapper), new OneToThreeMapper(), CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(3);

        foreach (var packet in results)
        {
            _ = packet.LineageHops.Should().HaveCount(1);
            _ = packet.LineageHops[0].AncestryInputIndices.Should().BeEquivalentTo([0]);
            _ = packet.LineageHops[0].InputContributorCount.Should().Be(1);
            _ = packet.LineageHops[0].OutputEmissionCount.Should().Be(3);
        }
    }

    [Fact]
    public async Task MaterializingStrategy_ConflictingContributorFanOut_UsesNullOutputEmissionCountForAmbiguousOutputs()
    {
        // Arrange
        var inputPackets = CreatePacketStream("left", "right");
        var outputData = CreateDataStream("out-0", "out-1", "out-2", "out-3");
        ILineageMappingStrategy<string, string> strategy = MaterializingStrategy<string, string>.Instance;
        var options = CreateOptions();

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "conflict_node", s_pipelineId, null, TransformCardinality.OneToMany, options,
                           typeof(ConflictingFanOutMapper), new ConflictingFanOutMapper(), CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(4);
        var outputEmissionByOutputData = results.ToDictionary(r => r.Data, r => r.LineageHops[0].OutputEmissionCount);

        _ = outputEmissionByOutputData["out-0"].Should().Be(2);
        _ = outputEmissionByOutputData["out-1"].Should().BeNull();
        _ = outputEmissionByOutputData["out-2"].Should().Be(3);
        _ = outputEmissionByOutputData["out-3"].Should().Be(3);
    }

    #endregion

    #region CapAwareMaterializingStrategy Tests

    [Fact]
    public async Task CapAwareMaterializingStrategy_NoCap_UsesMaterializingStrategy()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2, 3);
        var outputData = CreateDataStream("a", "b", "c");
        ILineageMappingStrategy<int, string> strategy = CapAwareMaterializingStrategy<int, string>.Instance;
        var options = CreateOptions(materializationCap: null);

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(3);
    }

    [Fact]
    public async Task CapAwareMaterializingStrategy_WithinCap_BuffersCompletely()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2, 3);
        var outputData = CreateDataStream("a", "b", "c");
        ILineageMappingStrategy<int, string> strategy = CapAwareMaterializingStrategy<int, string>.Instance;
        var options = CreateOptions(materializationCap: 10);

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(3);
    }

    [Fact]
    public async Task CapAwareMaterializingStrategy_ExceedsCap_Degrade_FallsBackToStreaming()
    {
        // Arrange
        var inputs = Enumerable.Range(1, 100).ToArray();
        var inputPackets = CreatePacketStream(inputs);
        var outputs = Enumerable.Range(100, 100).ToArray();
        var outputData = CreateDataStream(outputs);
        ILineageMappingStrategy<int, int> strategy = CapAwareMaterializingStrategy<int, int>.Instance;

        var options = CreateOptions(
            materializationCap: 10,
            overflowPolicy: LineageOverflowPolicy.Degrade
        );

        // Act
        List<LineagePacket<int>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(100);
    }

    [Fact]
    public async Task CapAwareMaterializingStrategy_ExceedsCap_Strict_Throws()
    {
        // Arrange
        var inputs = Enumerable.Range(1, 100).ToArray();
        var inputPackets = CreatePacketStream(inputs);
        var outputs = Enumerable.Range(100, 100).ToArray();
        var outputData = CreateDataStream(outputs);
        ILineageMappingStrategy<int, int> strategy = CapAwareMaterializingStrategy<int, int>.Instance;

        var options = CreateOptions(
            materializationCap: 10,
            overflowPolicy: LineageOverflowPolicy.Strict
        );

        // Act & Assert
        var act = async () =>
        {
            await foreach (var _ in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                               CancellationToken.None))
            {
                // Enumerate to trigger
            }
        };

        _ = await act.Should().ThrowAsync<InvalidOperationException>()
            .Where(ex => ex.Message.Contains("cap exceeded"));
    }

    [Fact]
    public async Task CapAwareMaterializingStrategy_ExceedsCap_WarnContinue_BuffersAll()
    {
        // Arrange
        var inputs = Enumerable.Range(1, 100).ToArray();
        var inputPackets = CreatePacketStream(inputs);
        var outputs = Enumerable.Range(100, 100).ToArray();
        var outputData = CreateDataStream(outputs);
        ILineageMappingStrategy<int, int> strategy = CapAwareMaterializingStrategy<int, int>.Instance;

        var options = CreateOptions(
            materializationCap: 10,
            overflowPolicy: LineageOverflowPolicy.WarnContinue
        );

        // Act
        List<LineagePacket<int>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        // When WarnContinue is used, the strategy should buffer all data and process it
        _ = results.Should().HaveCountGreaterThanOrEqualTo(99);
    }

    [Fact]
    public async Task CapAwareMaterializingStrategy_ExceedsCapWithMismatch_Degrade()
    {
        // Arrange
        var inputs = Enumerable.Range(1, 100).ToArray();
        var inputPackets = CreatePacketStream(inputs);
        var outputs = Enumerable.Range(100, 50).ToArray();
        var outputData = CreateDataStream(outputs);
        ILineageMappingStrategy<int, int> strategy = CapAwareMaterializingStrategy<int, int>.Instance;

        var options = CreateOptions(
            materializationCap: 10,
            overflowPolicy: LineageOverflowPolicy.Degrade
        );

        // Act
        List<LineagePacket<int>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = results.Should().HaveCount(50);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task MultiStrategy_ConsistentBehavior_OneToOneMatch()
    {
        // Test that all strategies produce consistent results for matching 1:1 data
        var options = CreateOptions();

        ILineageMappingStrategy<int, string>[] strategies =
        [
            StreamingOneToOneStrategy<int, string>.Instance,
            PositionalStreamingStrategy<int, string>.Instance,
            MaterializingStrategy<int, string>.Instance,
            CapAwareMaterializingStrategy<int, string>.Instance,
        ];

        List<List<LineagePacket<string>>> results = [];

        foreach (var strategy in strategies)
        {
            var packets = CreatePacketStream(1, 2, 3);
            var data = CreateDataStream("a", "b", "c");
            List<LineagePacket<string>> strategyResults = [];

            await foreach (var packet in strategy.MapAsync(packets, data, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                               CancellationToken.None))
            {
                strategyResults.Add(packet);
            }

            results.Add(strategyResults);
        }

        // All strategies should produce same count
        foreach (var strategyResult in results)
        {
            _ = strategyResult.Should().HaveCount(3);
        }

        // All should have same output data
        foreach (var strategyResult in results)
        {
            _ = strategyResult.Select(p => p.Data).Should().ContainInOrder("a", "b", "c");
        }
    }

    [Fact]
    public async Task LineageOptions_OnMismatchCallback_InvokedCorrectly()
    {
        // Arrange
        var inputPackets = CreatePacketStream(1, 2);
        var outputData = CreateDataStream("a", "b", "c");
        List<LineageMismatchContext> mismatchContexts = [];

        var options = new LineageOptions(
            false, // Strict
            false, // WarnOnMismatch
            mismatchContexts.Add, // OnMismatch
            null, // MaterializationCap
            LineageOverflowPolicy.Degrade, // OverflowPolicy
            true, // CaptureHopTimestamps
            true, // CaptureDecisions
            true, // CaptureObservedCardinality
            false, // CaptureAncestryMapping
            false, // CaptureHopSnapshots
            100, // SampleEvery
            true, // DeterministicSampling
            true, // RedactData
            100); // MaxHopRecordsPerItem

        ILineageMappingStrategy<int, string> strategy = MaterializingStrategy<int, string>.Instance;

        // Act
        List<LineagePacket<string>> results = [];

        await foreach (var packet in strategy.MapAsync(inputPackets, outputData, "test_node", s_pipelineId, null, TransformCardinality.OneToOne, options, null, null,
                           CancellationToken.None))
        {
            results.Add(packet);
        }

        // Assert
        _ = mismatchContexts.Should().HaveCount(1);
        _ = mismatchContexts[0].NodeId.Should().Be("test_node");
        _ = mismatchContexts[0].InputCount.Should().Be(2);
        _ = mismatchContexts[0].OutputCount.Should().Be(3);
    }

    #endregion
}
