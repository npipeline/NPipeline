using AwesomeAssertions;
using Cassandra;
using FakeItEasy;
using MongoDB.Driver;
using NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;
using NPipeline.Connectors.Azure.CosmosDb.Api.Mongo;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Api;

public sealed class CosmosApiAdaptersTests
{
    [Fact]
    public void MongoAdapter_ShouldExposeExpectedApiMetadata()
    {
        var adapter = new CosmosMongoApiAdapter();

        _ = adapter.ApiType.Should().Be(CosmosApiType.Mongo);
        _ = adapter.SupportedSchemes.Should().ContainSingle().Which.Should().Be("cosmos-mongo");
    }

    [Fact]
    public async Task MongoAdapter_CreateClientAsync_WithCanceledToken_ShouldThrowOperationCanceledException()
    {
        var adapter = new CosmosMongoApiAdapter();
        var configuration = new CosmosConfiguration { ConnectionString = "mongodb://localhost:27017", DatabaseId = "db" };
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = async () => await adapter.CreateClientAsync(configuration, cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task MongoAdapter_CreateClientAsync_WithoutConnectionString_ShouldThrowInvalidOperationException()
    {
        var adapter = new CosmosMongoApiAdapter();
        var configuration = new CosmosConfiguration { DatabaseId = "db" };

        var act = async () => await adapter.CreateClientAsync(configuration, CancellationToken.None);

        var exception = await act.Should().ThrowAsync<InvalidOperationException>();
        _ = exception.Which.Message.Should().Contain("MongoConnectionString or ConnectionString");
    }

    [Fact]
    public void MongoAdapter_CreateSourceExecutor_WithMongoClient_ShouldCreateExecutor()
    {
        var adapter = new CosmosMongoApiAdapter();

        var configuration = new CosmosConfiguration
        {
            DatabaseId = "db",
            ContainerId = "container",
        };

        var client = new MongoClient("mongodb://localhost:27017");

        var executor = adapter.CreateSourceExecutor(client, configuration);

        _ = executor.Should().NotBeNull();
    }

    [Fact]
    public void MongoAdapter_CreateSourceExecutor_WithWrongClientType_ShouldThrowInvalidCastException()
    {
        var adapter = new CosmosMongoApiAdapter();

        var act = () => adapter.CreateSourceExecutor(new object(), new CosmosConfiguration());

        _ = act.Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void MongoAdapter_CreateSinkExecutor_WithWrongClientType_ShouldThrowInvalidCastException()
    {
        var adapter = new CosmosMongoApiAdapter();

        var act = () => adapter.CreateSinkExecutor<object>(new object(), new CosmosConfiguration());

        _ = act.Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void CassandraAdapter_ShouldExposeExpectedApiMetadata()
    {
        var adapter = new CosmosCassandraApiAdapter();

        _ = adapter.ApiType.Should().Be(CosmosApiType.Cassandra);
        _ = adapter.SupportedSchemes.Should().ContainSingle().Which.Should().Be("cosmos-cassandra");
    }

    [Fact]
    public async Task CassandraAdapter_CreateClientAsync_WithCanceledToken_ShouldThrowOperationCanceledException()
    {
        var adapter = new CosmosCassandraApiAdapter();

        var configuration = new CosmosConfiguration
        {
            CassandraContactPoint = "localhost",
            DatabaseId = "keyspace",
        };

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = async () => await adapter.CreateClientAsync(configuration, cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task CassandraAdapter_CreateClientAsync_WithoutContactPointOrEndpoint_ShouldThrowInvalidOperationException()
    {
        var adapter = new CosmosCassandraApiAdapter();

        var configuration = new CosmosConfiguration
        {
            DatabaseId = "keyspace",
            CassandraContactPoint = "",
            AccountEndpoint = "",
        };

        var act = async () => await adapter.CreateClientAsync(configuration, CancellationToken.None);

        var exception = await act.Should().ThrowAsync<InvalidOperationException>();
        _ = exception.Which.Message.Should().Contain("CassandraContactPoint or AccountEndpoint");
    }

    [Fact]
    public async Task CassandraAdapter_CreateClientAsync_WithoutDatabaseId_ShouldThrowInvalidOperationException()
    {
        var adapter = new CosmosCassandraApiAdapter();

        var configuration = new CosmosConfiguration
        {
            CassandraContactPoint = "localhost",
            DatabaseId = "",
        };

        var act = async () => await adapter.CreateClientAsync(configuration, CancellationToken.None);

        var exception = await act.Should().ThrowAsync<InvalidOperationException>();
        _ = exception.Which.Message.Should().Contain("DatabaseId");
    }

    [Fact]
    public void CassandraAdapter_CreateSourceExecutor_WithContext_ShouldCreateExecutor()
    {
        var adapter = new CosmosCassandraApiAdapter();
        var cluster = A.Fake<ICluster>();
        var session = A.Fake<ISession>();
        var clientContext = new CassandraClientContext(cluster, session);

        var executor = adapter.CreateSourceExecutor(clientContext, new CosmosConfiguration());

        _ = executor.Should().NotBeNull();
    }

    [Fact]
    public void CassandraAdapter_CreateSourceExecutor_WithWrongClientType_ShouldThrowInvalidCastException()
    {
        var adapter = new CosmosCassandraApiAdapter();

        var act = () => adapter.CreateSourceExecutor(new object(), new CosmosConfiguration());

        _ = act.Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void CassandraAdapter_CreateSinkExecutor_WithWrongClientType_ShouldThrowInvalidCastException()
    {
        var adapter = new CosmosCassandraApiAdapter();

        var act = () => adapter.CreateSinkExecutor<object>(new object(), new CosmosConfiguration());

        _ = act.Should().Throw<InvalidCastException>();
    }
}
