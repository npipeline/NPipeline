using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.MongoDB.Connection;
using NPipeline.Connectors.MongoDB.DependencyInjection;
using NPipeline.Connectors.MongoDB.Tests.Fixtures;

namespace NPipeline.Connectors.MongoDB.Tests.Integration;

[Collection(MongoTestCollection.Name)]
public class MongoDependencyInjectionIntegrationTests(MongoTestContainerFixture fixture)
{
    // ── AddMongoConnector ─────────────────────────────────────────────────────

    [Fact]
    public void AddMongoConnector_RegistersRequiredServices()
    {
        var services = new ServiceCollection();

        services.AddMongoConnector(opts =>
            opts.DefaultConnectionString = fixture.ConnectionString);

        using var sp = services.BuildServiceProvider();

        sp.GetService<MongoConnectorOptions>().Should().NotBeNull();
        sp.GetService<IMongoConnectionPool>().Should().NotBeNull();
        sp.GetService<MongoSourceNodeFactory>().Should().NotBeNull();
        sp.GetService<MongoSinkNodeFactory>().Should().NotBeNull();
    }

    [Fact]
    public void AddMongoConnector_NoConfiguration_RegistersServicesWithDefaults()
    {
        var services = new ServiceCollection();
        services.AddMongoConnector();

        using var sp = services.BuildServiceProvider();

        sp.GetService<MongoConnectorOptions>().Should().NotBeNull();
        sp.GetService<IMongoConnectionPool>().Should().NotBeNull();
    }

    [Fact]
    public void AddMongoConnector_CalledTwice_DoesNotDuplicate()
    {
        var services = new ServiceCollection();
        services.AddMongoConnector(opts => opts.DefaultConnectionString = fixture.ConnectionString);
        services.AddMongoConnector(opts => opts.DefaultConnectionString = fixture.ConnectionString);

        using var sp = services.BuildServiceProvider();

        // TryAddSingleton means only one registration — should resolve without error
        sp.GetService<MongoConnectorOptions>().Should().NotBeNull();
    }

    // ── AddMongoConnection ────────────────────────────────────────────────────

    [Fact]
    public void AddMongoConnection_AfterAddMongoConnector_AddsNamedConnection()
    {
        var services = new ServiceCollection();
        services.AddMongoConnector();
        services.AddMongoConnection("secondary", fixture.ConnectionString);

        using var sp = services.BuildServiceProvider();

        var opts = sp.GetRequiredService<MongoConnectorOptions>();
        opts.GetConnectionString("secondary").Should().Be(fixture.ConnectionString);
    }

    [Fact]
    public void AddMongoConnection_WithoutPriorAddMongoConnector_StillWorks()
    {
        var services = new ServiceCollection();
        services.AddMongoConnection("main", fixture.ConnectionString);

        using var sp = services.BuildServiceProvider();

        var opts = sp.GetService<MongoConnectorOptions>();
        opts.Should().NotBeNull();
        opts!.GetConnectionString("main").Should().Be(fixture.ConnectionString);
    }

    // ── ConnectionPool ────────────────────────────────────────────────────────

    [Fact]
    public void ConnectionPool_GetClient_ReturnsSameInstanceForSameConnectionString()
    {
        var services = new ServiceCollection();

        services.AddMongoConnector(opts =>
            opts.DefaultConnectionString = fixture.ConnectionString);

        using var sp = services.BuildServiceProvider();
        var pool = sp.GetRequiredService<IMongoConnectionPool>();

        var c1 = pool.GetClientForUri(fixture.ConnectionString);
        var c2 = pool.GetClientForUri(fixture.ConnectionString);

        // Pool should return the same MongoClient instance for the same connection string
        c1.Should().BeSameAs(c2);
    }

    [Fact]
    public void ConnectionPool_GetClient_ReturnsDifferentInstancesForDifferentConnectionStrings()
    {
        var services = new ServiceCollection();
        services.AddMongoConnector();

        using var sp = services.BuildServiceProvider();
        var pool = sp.GetRequiredService<IMongoConnectionPool>();

        var c1 = pool.GetClientForUri(fixture.ConnectionString);
        var c2 = pool.GetClientForUri("mongodb://localhost:37017"); // different port

        c1.Should().NotBeSameAs(c2);
    }

    // ── Factories ─────────────────────────────────────────────────────────────

    [Fact]
    public void MongoSourceNodeFactory_IsSingleton()
    {
        var services = new ServiceCollection();

        services.AddMongoConnector(opts =>
            opts.DefaultConnectionString = fixture.ConnectionString);

        using var sp = services.BuildServiceProvider();

        var f1 = sp.GetRequiredService<MongoSourceNodeFactory>();
        var f2 = sp.GetRequiredService<MongoSourceNodeFactory>();

        f1.Should().BeSameAs(f2);
    }

    [Fact]
    public void MongoSinkNodeFactory_IsSingleton()
    {
        var services = new ServiceCollection();

        services.AddMongoConnector(opts =>
            opts.DefaultConnectionString = fixture.ConnectionString);

        using var sp = services.BuildServiceProvider();

        var f1 = sp.GetRequiredService<MongoSinkNodeFactory>();
        var f2 = sp.GetRequiredService<MongoSinkNodeFactory>();

        f1.Should().BeSameAs(f2);
    }
}
