using System.Collections.Immutable;
using AwesomeAssertions;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Join;

public sealed class LookupNodeTests
{
    [Fact]
    public async Task AddInMemoryLookup_CorrectlyEnrichesData()
    {
        // Arrange
        var users = new[]
        {
            new User(1, "Alice", "US"),
            new User(2, "Bob", "CA"),
            new User(3, "Charlie", "XX"), // Non-existent country code
        };

        var source = new InMemorySourceNode<User>(users);
        var sink = new InMemorySinkNode<EnrichedUser>();
        var context = PipelineContext.Default;
        var services = new ServiceCollection();
        services.AddNPipeline(typeof(LookupNodeTests).Assembly);
        services.AddSingleton(source);
        services.AddSingleton(sink);
        var provider = services.BuildServiceProvider();
        var runner = new TestPipelineRunner(provider.GetRequiredService<IPipelineRunner>());
        context.Items[typeof(InMemorySinkNode<EnrichedUser>).FullName!] = sink;

        // Act
        var result = await runner.RunAndGetResultAsync<InMemoryLookupPipeline, EnrichedUser>(context);

        // Assert
        result.Should().HaveCount(3);
        result.Should().Contain(u => u.Id == 1 && u.CountryName == "United States");
        result.Should().Contain(u => u.Id == 2 && u.CountryName == "Canada");
        result.Should().Contain(u => u.Id == 3 && u.CountryName == null);
    }

    [Fact]
    public async Task CustomLookupNode_CorrectlyEnrichesData()
    {
        // Arrange
        var users = new[]
        {
            new EnrichedUser(1, "Alice", "United States"),
            new EnrichedUser(2, "Bob", "Canada"),
        };

        var source = new InMemorySourceNode<EnrichedUser>(users);
        var sink = new InMemorySinkNode<FullyEnrichedUser>();
        var context = PipelineContext.Default;

        var profileService = A.Fake<IUserProfileService>();

        A.CallTo(() => profileService.GetProfileAsync(1, A<CancellationToken>._))
            .Returns(Task.FromResult<UserProfile?>(new UserProfile(1, "Bio for Alice")));

        A.CallTo(() => profileService.GetProfileAsync(2, A<CancellationToken>._))
            .Returns(Task.FromResult<UserProfile?>(null)); // Bob has no profile

        var services = new ServiceCollection();
        services.AddNPipeline(typeof(LookupNodeTests).Assembly);
        services.AddSingleton(source);
        services.AddSingleton(sink);
        services.AddSingleton(profileService);
        var provider = services.BuildServiceProvider();
        var runner = new TestPipelineRunner(provider.GetRequiredService<IPipelineRunner>());
        context.Items[typeof(InMemorySinkNode<FullyEnrichedUser>).FullName!] = sink;

        // Act
        var result = await runner.RunAndGetResultAsync<ComplexLookupPipeline, FullyEnrichedUser>(context);

        // Assert
        result.Should().HaveCount(2);
        result.Should().Contain(u => u.Id == 1 && u.Bio == "Bio for Alice");
        result.Should().Contain(u => u.Id == 2 && u.Bio == null);
    }

    // Test Data
    public record User(int Id, string Name, string CountryCode);

    public record EnrichedUser(int Id, string Name, string? CountryName);

    public record Country(string Code, string Name);

    public record UserProfile(int UserId, string Bio);

    public record FullyEnrichedUser(int Id, string Name, string? CountryName, string? Bio);

    // Test Service
    public interface IUserProfileService
    {
        Task<UserProfile?> GetProfileAsync(int userId, CancellationToken cancellationToken);
    }

    // Test Node for complex lookups
    public class ProfileLookupNode(IUserProfileService profileService) : LookupNode<EnrichedUser, int, UserProfile, FullyEnrichedUser>
    {
        protected override int ExtractKey(EnrichedUser input, PipelineContext context)
        {
            return input.Id;
        }

        protected override async Task<UserProfile?> LookupAsync(int key, PipelineContext context, CancellationToken cancellationToken)
        {
            return await profileService.GetProfileAsync(key, cancellationToken);
        }

        protected override FullyEnrichedUser CreateOutput(EnrichedUser input, UserProfile? lookupValue, PipelineContext context)
        {
            return new FullyEnrichedUser(input.Id, input.Name, input.CountryName, lookupValue?.Bio);
        }
    }

    // Test Pipeline Definitions
    public class InMemoryLookupPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var countryData = new Dictionary<string, Country>
            {
                { "US", new Country("US", "United States") },
                { "CA", new Country("CA", "Canada") },
            }.ToImmutableDictionary();

            var source = builder.AddSource<InMemorySourceNode<User>, User>("user_source");
            var sink = builder.AddSink<InMemorySinkNode<EnrichedUser>, EnrichedUser>("sink");

            var countryLookup = builder.AddInMemoryLookup<User, string, Country, EnrichedUser>(
                "country_lookup",
                countryData,
                user => user.CountryCode,
                (user, country) => new EnrichedUser(user.Id, user.Name, country?.Name)
            );

            builder.Connect(source, countryLookup)
                .Connect(countryLookup, sink);
        }
    }

    public class ComplexLookupPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<InMemorySourceNode<EnrichedUser>, EnrichedUser>("user_source");
            var sink = builder.AddSink<InMemorySinkNode<FullyEnrichedUser>, FullyEnrichedUser>("sink");
            var profileLookup = builder.AddTransform<ProfileLookupNode, EnrichedUser, FullyEnrichedUser>("profile_lookup");

            builder.Connect(source, profileLookup)
                .Connect(profileLookup, sink);
        }
    }
}
