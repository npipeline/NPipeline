using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.Lineage;

namespace NPipeline.Tests.Lineage;

/// <summary>
///     The lineage envelope used to carry <c>ImmutableList&lt;T&gt;</c> — an AVL tree that allocates a spine of nodes
///     per append and an enumerator per walk — on the per-item path. It now carries <see cref="ImmutableArray{T}" />,
///     which is a struct over a flat array. These tests pin the behaviour that switch has to preserve: a default
///     (uninitialised) array must not escape as one, appends must stay append-only, and <see cref="LineageRecord" />
///     contributor normalisation must still produce a sorted, deduplicated list.
/// </summary>
public sealed class LineageEnvelopeAllocationTests
{
    [Fact]
    public void LineagePacket_NormalizesDefaultCollections_ToEmpty()
    {
        var packet = new LineagePacket<int>(1, Guid.NewGuid(), default)
        {
            LineageRecords = default,
        };

        packet.TraversalPath.IsDefault.Should().BeFalse();
        packet.TraversalPath.Should().BeEmpty();
        packet.LineageRecords.IsDefault.Should().BeFalse();
        packet.LineageRecords.Should().BeEmpty();
    }

    [Fact]
    public void LineagePacket_Append_LeavesTheOriginalPathUntouched()
    {
        var packet = new LineagePacket<int>(1, Guid.NewGuid(), ["node-a"]);

        var appended = packet with { TraversalPath = packet.TraversalPath.Add("node-b") };

        packet.TraversalPath.Should().Equal("node-a");
        appended.TraversalPath.Should().Equal("node-a", "node-b");
    }

    [Fact]
    public void Normalize_SortsAndDeduplicatesContributors()
    {
        var first = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var second = Guid.Parse("22222222-2222-2222-2222-222222222222");

        var normalized = BuildRecord([second, first, second], [4, 1, 4, 1]).Normalize();

        normalized.ContributorCorrelationIds.Should().Equal(first, second);
        normalized.ContributorInputIndices.Should().Equal(1, 4);
    }

    [Fact]
    public void Normalize_KeepsASingleContributor()
    {
        var only = Guid.NewGuid();

        var normalized = BuildRecord([only], [7]).Normalize();

        normalized.ContributorCorrelationIds.Should().Equal(only);
        normalized.ContributorInputIndices.Should().Equal(7);
    }

    [Fact]
    public void Normalize_TreatsEmptyContributorsAsAbsent()
    {
        var normalized = BuildRecord([], []).Normalize();

        normalized.ContributorCorrelationIds.Should().BeNull();
        normalized.ContributorInputIndices.Should().BeNull();
    }

    [Fact]
    public void Normalize_DoesNotMutateTheSourceLists()
    {
        List<int> indices = [3, 1, 2];

        _ = BuildRecord([], indices).Normalize();

        indices.Should().Equal(3, 1, 2);
    }

    [Fact]
    public void Normalize_StampsATimestamp_WhenNoneWasSupplied()
    {
        var normalized = BuildRecord([], []).Normalize();

        normalized.TimestampUtc.Should().NotBe(default);
    }

    private static LineageRecord BuildRecord(IReadOnlyList<Guid> correlationIds, IReadOnlyList<int> inputIndices) =>
        new(
            Guid.NewGuid(),
            "node-a",
            Guid.NewGuid(),
            LineageOutcomeReason.Emitted,
            false,
            ["node-a"],
            ContributorCorrelationIds: correlationIds,
            ContributorInputIndices: inputIndices);
}
