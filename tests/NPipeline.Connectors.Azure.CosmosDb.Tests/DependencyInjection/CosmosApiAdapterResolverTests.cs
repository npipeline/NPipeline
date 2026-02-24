using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.DependencyInjection;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.DependencyInjection;

public sealed class CosmosApiAdapterResolverTests
{
    [Fact]
    public void GetAdapter_ByApiType_ShouldReturnMatchingAdapter()
    {
        var mongoAdapter = CreateAdapter(CosmosApiType.Mongo, "cosmos-mongo", "mongo-alt");
        var cassandraAdapter = CreateAdapter(CosmosApiType.Cassandra, "cosmos-cassandra");
        var resolver = new CosmosApiAdapterResolver([mongoAdapter, cassandraAdapter]);

        var result = resolver.GetAdapter(CosmosApiType.Mongo);

        _ = result.Should().BeSameAs(mongoAdapter);
    }

    [Fact]
    public void GetAdapter_ByScheme_ShouldBeCaseInsensitive()
    {
        var mongoAdapter = CreateAdapter(CosmosApiType.Mongo, "cosmos-mongo");
        var resolver = new CosmosApiAdapterResolver([mongoAdapter]);

        var result = resolver.GetAdapter("COSMOS-MONGO");

        _ = result.Should().BeSameAs(mongoAdapter);
    }

    [Fact]
    public void GetAdapter_ByApiType_WhenMissing_ShouldThrowInvalidOperationException()
    {
        var resolver = new CosmosApiAdapterResolver([CreateAdapter(CosmosApiType.Mongo, "cosmos-mongo")]);

        var act = () => resolver.GetAdapter(CosmosApiType.Cassandra);

        var exception = act.Should().Throw<InvalidOperationException>();
        _ = exception.Which.Message.Should().Contain("No adapter registered for API type");
    }

    [Fact]
    public void GetAdapter_ByScheme_WithWhitespace_ShouldThrowArgumentNullException()
    {
        var resolver = new CosmosApiAdapterResolver([CreateAdapter(CosmosApiType.Mongo, "cosmos-mongo")]);

        var act = () => resolver.GetAdapter(" ");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetAdapter_ByScheme_WhenMissing_ShouldThrowInvalidOperationException()
    {
        var resolver = new CosmosApiAdapterResolver([CreateAdapter(CosmosApiType.Mongo, "cosmos-mongo")]);

        var act = () => resolver.GetAdapter("cosmos-unknown");

        var exception = act.Should().Throw<InvalidOperationException>();
        _ = exception.Which.Message.Should().Contain("No adapter registered for URI scheme");
    }

    private static ICosmosApiAdapter CreateAdapter(CosmosApiType apiType, params string[] schemes)
    {
        var adapter = A.Fake<ICosmosApiAdapter>();
        A.CallTo(() => adapter.ApiType).Returns(apiType);
        A.CallTo(() => adapter.SupportedSchemes).Returns(schemes);
        return adapter;
    }
}
