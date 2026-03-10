using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Attributes;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.Connectors.MongoDB.Tests.Fixtures;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MongoDB.Tests.Integration;

[Collection(MongoTestCollection.Name)]
public class MongoStorageUriIntegrationTests(MongoTestContainerFixture fixture)
{
    private static string UniqueCollection()
    {
        return $"col_{Guid.NewGuid():N}";
    }

    private static PipelineContext DefaultContext()
    {
        return new PipelineContext();
    }

    // ── MongoDatabaseStorageProvider ──────────────────────────────────────────

    [Fact]
    public void StorageProvider_CanHandle_MongodbScheme()
    {
        var provider = new MongoDatabaseStorageProvider();
        var uri = StorageUri.Parse("mongodb://localhost:27017/mydb");
        provider.CanHandle(uri).Should().BeTrue();
    }

    [Fact]
    public void StorageProvider_CanHandle_MongodbSrvScheme()
    {
        var provider = new MongoDatabaseStorageProvider();
        var uri = StorageUri.Parse("mongodb+srv://cluster0.example.com/mydb");
        provider.CanHandle(uri).Should().BeTrue();
    }

    [Fact]
    public void StorageProvider_CannotHandle_OtherSchemes()
    {
        var provider = new MongoDatabaseStorageProvider();

        var postgres = StorageUri.Parse("postgresql://localhost:5432/mydb");
        var sql = StorageUri.Parse("sqlserver://localhost/mydb");

        provider.CanHandle(postgres).Should().BeFalse();
        provider.CanHandle(sql).Should().BeFalse();
    }

    [Fact]
    public void StorageProvider_GetConnectionString_ReconstructsUri()
    {
        var provider = new MongoDatabaseStorageProvider();
        var uri = StorageUri.Parse("mongodb://localhost:27017/mydb");

        var cs = provider.GetConnectionString(uri);

        cs.Should().StartWith("mongodb://");
        cs.Should().Contain("localhost");
        cs.Should().Contain("mydb");
    }

    [Fact]
    public void StorageProvider_GetConnectionString_ThrowsForUnsupportedScheme()
    {
        var provider = new MongoDatabaseStorageProvider();
        var uri = StorageUri.Parse("postgresql://localhost/mydb");
        var act = () => provider.GetConnectionString(uri);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void StorageProvider_GetConnectionString_NullUri_Throws()
    {
        var provider = new MongoDatabaseStorageProvider();
        var act = () => provider.GetConnectionString(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    // ── MongoStorageResolverFactory ───────────────────────────────────────────

    [Fact]
    public void StorageResolverFactory_CreateResolver_ReturnsNonNull()
    {
        var resolver = MongoStorageResolverFactory.CreateResolver();
        resolver.Should().NotBeNull();
    }

    [Fact]
    public void StorageResolverFactory_CreateResolver_CanResolveMongoUri()
    {
        var resolver = MongoStorageResolverFactory.CreateResolver();
        var uri = StorageUri.Parse("mongodb://localhost:27017/mydb");
        var act = () => resolver.ResolveProvider(uri);

        // Should not throw — provider is registered
        act.Should().NotThrow();
    }

    // ── SourceNode via StorageUri ─────────────────────────────────────────────

    [Fact]
    public async Task SourceNode_FromStorageUri_ReadsDocuments()
    {
        var colName = UniqueCollection();

        // Seed directly via connection string
        using var seedClient = new MongoClient(fixture.ConnectionString);
        var seedDb = seedClient.GetDatabase("uri_integration");

        await seedDb.GetCollection<BsonDocument>(colName)
            .InsertManyAsync(Enumerable.Range(1, 5).Select(i =>
                new BsonDocument { ["name"] = $"item{i}" }));

        // Build a StorageUri that preserves required connection options from Testcontainers.
        // In replica-set mode, dropping options like replicaSet/directConnection can cause
        // server discovery to target container-internal endpoints (e.g. 127.0.0.1:27017),
        // which are unreachable from the host test process.
        var mongoUrl = new MongoUrl(fixture.ConnectionString);
        var mongoUrlBuilder = new MongoUrlBuilder(fixture.ConnectionString);

        var userInfo = mongoUrl.Username != null
            ? $"{Uri.EscapeDataString(mongoUrl.Username)}:{Uri.EscapeDataString(mongoUrl.Password ?? "")}@"
            : "";

        var server = mongoUrl.Server;

        var queryParts = new List<string>();

        if (!string.IsNullOrWhiteSpace(mongoUrlBuilder.ReplicaSetName))
            queryParts.Add($"replicaSet={Uri.EscapeDataString(mongoUrlBuilder.ReplicaSetName)}");

        queryParts.Add($"directConnection={mongoUrlBuilder.DirectConnection.ToString().ToLowerInvariant()}");

        if (!string.IsNullOrWhiteSpace(mongoUrlBuilder.AuthenticationSource))
            queryParts.Add($"authSource={Uri.EscapeDataString(mongoUrlBuilder.AuthenticationSource)}");

        queryParts.Add($"collection={Uri.EscapeDataString(colName)}");
        var query = string.Join("&", queryParts);

        var storageUri = StorageUri.Parse(
            $"mongodb://{userInfo}{server.Host}:{server.Port}?{query}");

        var provider = new MongoDatabaseStorageProvider();

        var config = new MongoConfiguration
        {
            DatabaseName = "uri_integration",
            CollectionName = colName,
        };

        await using var source = new MongoSourceNode<NameRecord>(provider, storageUri, config);

        var results = new List<NameRecord>();

        await foreach (var item in source.OpenStream(DefaultContext(), CancellationToken.None))
        {
            results.Add(item);
        }

        results.Should().HaveCount(5);
    }

    // ── model ─────────────────────────────────────────────────────────────────

    private sealed class NameRecord
    {
        [MongoField("name")]
        public string Name { get; set; } = string.Empty;
    }
}
