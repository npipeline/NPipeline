using AwesomeAssertions;
using NPipeline.Attributes.Nodes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Join;

public sealed class BaseJoinNodeFallbackTests
{
    [Fact]
    public async Task BaseJoinNode_DefaultFallbackProjectsSourceMembers()
    {
        var node = new AutoProjectionJoinNode { JoinType = JoinType.FullOuter };
        var context = PipelineContext.Default;

        var inputs = new object?[]
        {
            new User(1, "Alice"),
            new UserProfile(1, "Alice's Profile"),
            new User(2, "Bob"), // unmatched left
            new UserProfile(3, "Charlie Profile"), // unmatched right
        }.ToAsyncEnumerable();

        var stream = await node.ExecuteAsync(inputs, context);
        var results = new List<UserProjection>();

        await foreach (var item in stream)
        {
            item.Should().BeOfType<UserProjection>();
            results.Add((UserProjection)item!);
        }

        results.Should().HaveCount(3);
        results.Should().ContainEquivalentOf(new UserProjection(1, "Alice", "Alice's Profile"));
        results.Should().ContainEquivalentOf(new UserProjection(2, "Bob", null));
        results.Should().ContainEquivalentOf(new UserProjection(3, null, "Charlie Profile"));
    }

    [KeySelector(typeof(User), nameof(User.Id))]
    [KeySelector(typeof(UserProfile), nameof(UserProfile.Id))]
    private sealed class AutoProjectionJoinNode : KeyedJoinNode<int, User, UserProfile, UserProjection>
    {
        public AutoProjectionJoinNode()
        {
            JoinType = JoinType.Inner;
        }

        public override UserProjection CreateOutput(User item1, UserProfile item2)
        {
            return new UserProjection(item1.Id, item1.Name, item2.ProfileInfo);
        }
    }

    private sealed record User(int Id, string Name);

    private sealed record UserProfile(int Id, string ProfileInfo);

    private sealed record UserProjection(int Id, string? Name, string? ProfileInfo);
}
