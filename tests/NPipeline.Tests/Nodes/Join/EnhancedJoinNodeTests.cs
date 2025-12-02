using System.Collections.Concurrent;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Join;

public sealed class EnhancedJoinNodeTests
{
    [Fact]
    public async Task KeyedJoinNode_WithLeftOuterJoin_ShouldIncludeUnmatchedLeftItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<EnrichedUser>>();
        services.AddNPipeline(typeof(EnhancedJoinNodeTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<LeftOuterJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<EnrichedUser>>();
        resultStore.Should().HaveCount(3);

        resultStore.Should().BeEquivalentTo([
            new EnrichedUser(1, "Alice", "Alice's Profile"),
            new EnrichedUser(2, "Bob", "Bob's Profile"),
            new EnrichedUser(3, "Charlie", null), // Left outer join should include unmatched left item
        ]);
    }

    [Fact]
    public async Task KeyedJoinNode_WithRightOuterJoin_ShouldIncludeUnmatchedRightItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<EnrichedUser>>();
        services.AddNPipeline(typeof(EnhancedJoinNodeTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<RightOuterJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<EnrichedUser>>();
        resultStore.Should().HaveCount(3);

        resultStore.Should().BeEquivalentTo([
            new EnrichedUser(1, "Alice", "Alice's Profile"),
            new EnrichedUser(2, "Bob", "Bob's Profile"),
            new EnrichedUser(4, null, "Extra Profile"), // Right outer join should include unmatched right item with original ID
        ]);
    }

    [Fact]
    public async Task KeyedJoinNode_WithFullOuterJoin_ShouldIncludeAllUnmatchedItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<EnrichedUser>>();
        services.AddNPipeline(typeof(EnhancedJoinNodeTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<FullOuterJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<EnrichedUser>>();
        resultStore.Should().HaveCount(4);

        resultStore.Should().BeEquivalentTo([
            new EnrichedUser(1, "Alice", "Alice's Profile"),
            new EnrichedUser(2, "Bob", "Bob's Profile"),
            new EnrichedUser(3, "Charlie", null), // Full outer join should include unmatched left item with original ID
            new EnrichedUser(4, null, "Extra Profile"), // Full outer join should include unmatched right item with original ID
        ]);
    }

    // Test Data Models
    private sealed record User(int Id, string Name);

    private sealed record UserProfile(int Id, string ProfileInfo);

    private sealed record EnrichedUser(int Id, string? Name, string? ProfileInfo);

    // Test Node Implementations

    private sealed class UserSourceWithExtra : SourceNode<User>
    {
        public override IDataPipe<User> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            var users = new[] { new User(1, "Alice"), new User(2, "Bob"), new User(3, "Charlie") };
            return new StreamingDataPipe<User>(users.ToAsyncEnumerable(), "UserStream");
        }
    }

    private sealed class UserProfileSourceWithExtra : SourceNode<UserProfile>
    {
        public override IDataPipe<UserProfile> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            var profiles = new[] { new UserProfile(2, "Bob's Profile"), new UserProfile(1, "Alice's Profile"), new UserProfile(4, "Extra Profile") };
            return new StreamingDataPipe<UserProfile>(profiles.ToAsyncEnumerable(), "ProfileStream");
        }
    }

    [KeySelector(typeof(User), nameof(User.Id))]
    [KeySelector(typeof(UserProfile), nameof(UserProfile.Id))]
    private sealed class UserEnrichmentNodeWithOuterSupport : KeyedJoinNode<int, User, UserProfile, EnrichedUser>
    {
        public UserEnrichmentNodeWithOuterSupport()
        {
            JoinType = JoinType.Inner; // Default, will be overridden by test pipelines
        }

        public override EnrichedUser CreateOutput(User item1, UserProfile item2)
        {
            return new EnrichedUser(item1.Id, item1.Name, item2.ProfileInfo);
        }

        public override EnrichedUser CreateOutputFromLeft(User item1)
        {
            return new EnrichedUser(item1.Id, item1.Name, null);
        }

        public override EnrichedUser CreateOutputFromRight(UserProfile item2)
        {
            return new EnrichedUser(item2.Id, null, item2.ProfileInfo);
        }
    }

    private sealed class EnrichedUserSink(ConcurrentQueue<EnrichedUser> store) : SinkNode<EnrichedUser>
    {
        public override async Task ExecuteAsync(IDataPipe<EnrichedUser> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    // Test Definitions

    private sealed class LeftOuterJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var userSource = builder.AddSource<UserSourceWithExtra, User>("user_source");
            var profileSource = builder.AddSource<UserProfileSourceWithExtra, UserProfile>("profile_source");
            var enrichmentNode = builder.AddJoin<UserEnrichmentNodeWithOuterSupport, User, UserProfile, EnrichedUser>("enrichment_node");
            var sink = builder.AddSink<EnrichedUserSink, EnrichedUser>("sink");

            var enrichmentNodeInstance = new UserEnrichmentNodeWithOuterSupport { JoinType = JoinType.LeftOuter };
            builder.AddPreconfiguredNodeInstance("enrichment-node", enrichmentNodeInstance);

            builder.Connect(userSource, enrichmentNode);
            builder.Connect(profileSource, enrichmentNode);
            builder.Connect(enrichmentNode, sink);
        }
    }

    private sealed class RightOuterJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var userSource = builder.AddSource<UserSourceWithExtra, User>("user_source");
            var profileSource = builder.AddSource<UserProfileSourceWithExtra, UserProfile>("profile_source");
            var enrichmentNode = builder.AddJoin<UserEnrichmentNodeWithOuterSupport, User, UserProfile, EnrichedUser>("enrichment_node");
            var sink = builder.AddSink<EnrichedUserSink, EnrichedUser>("sink");

            var enrichmentNodeInstance = new UserEnrichmentNodeWithOuterSupport { JoinType = JoinType.RightOuter };
            builder.AddPreconfiguredNodeInstance("enrichment-node", enrichmentNodeInstance);

            builder.Connect(userSource, enrichmentNode);
            builder.Connect(profileSource, enrichmentNode);
            builder.Connect(enrichmentNode, sink);
        }
    }

    private sealed class FullOuterJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var userSource = builder.AddSource<UserSourceWithExtra, User>("user_source");
            var profileSource = builder.AddSource<UserProfileSourceWithExtra, UserProfile>("profile_source");
            var enrichmentNode = builder.AddJoin<UserEnrichmentNodeWithOuterSupport, User, UserProfile, EnrichedUser>("enrichment_node");
            var sink = builder.AddSink<EnrichedUserSink, EnrichedUser>("sink");

            var enrichmentNodeInstance = new UserEnrichmentNodeWithOuterSupport { JoinType = JoinType.FullOuter };
            builder.AddPreconfiguredNodeInstance("enrichment-node", enrichmentNodeInstance);

            builder.Connect(userSource, enrichmentNode);
            builder.Connect(profileSource, enrichmentNode);
            builder.Connect(enrichmentNode, sink);
        }
    }
}
