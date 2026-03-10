using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Attributes;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.Connectors.MongoDB.Tests.Fixtures;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.MongoDB.Tests.Integration;

[Collection(MongoTestCollection.Name)]
public class MongoConnectorEndToEndTests(MongoTestContainerFixture fixture)
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private MongoClient CreateClient()
    {
        return new MongoClient(fixture.ConnectionString);
    }

    private static string UniqueCollection()
    {
        return $"col_{Guid.NewGuid():N}";
    }

    private static PipelineContext DefaultContext()
    {
        return new PipelineContext();
    }

    private static async Task SeedAsync(
        IMongoDatabase db, string collection, IEnumerable<BsonDocument> docs)
    {
        var col = db.GetCollection<BsonDocument>(collection);
        await col.InsertManyAsync(docs);
    }

    private static async Task<long> CountAsync(IMongoDatabase db, string collection)
    {
        var col = db.GetCollection<BsonDocument>(collection);
        return await col.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
    }

    private static async Task<List<T>> DrainAsync<T>(
        MongoSourceNode<T> source, PipelineContext? ctx = null) where T : class, new()
    {
        var results = new List<T>();

        await foreach (var item in source.OpenStream(ctx ?? new PipelineContext(), CancellationToken.None))
        {
            results.Add(item);
        }

        return results;
    }

    // ── source ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task SourceNode_ReadsAllDocuments()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("integration");
        var colName = UniqueCollection();

        await SeedAsync(db, colName, Enumerable.Range(1, 10).Select(i =>
            new BsonDocument { ["name"] = $"item{i}", ["value"] = i }));

        var config = new MongoConfiguration { DatabaseName = "integration", CollectionName = colName };
        await using var source = new MongoSourceNode<ProductRecord>(client, config);

        var results = await DrainAsync(source);

        results.Should().HaveCount(10);

        results.Select(r => r.Name).Should()
            .BeEquivalentTo(Enumerable.Range(1, 10).Select(i => $"item{i}"));
    }

    [Fact]
    public async Task SourceNode_EmptyCollection_ReturnsEmpty()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("integration");
        var colName = UniqueCollection();
        await db.CreateCollectionAsync(colName);

        var config = new MongoConfiguration { DatabaseName = "integration", CollectionName = colName };
        await using var source = new MongoSourceNode<ProductRecord>(client, config);

        (await DrainAsync(source)).Should().BeEmpty();
    }

    [Fact]
    public async Task SourceNode_WithFilter_ReturnsMatchingDocumentsOnly()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("integration");
        var colName = UniqueCollection();

        await SeedAsync(db, colName, Enumerable.Range(1, 20).Select(i =>
            new BsonDocument { ["name"] = $"item{i}", ["value"] = i }));

        var filter = Builders<BsonDocument>.Filter.Gt("value", 15);
        var config = new MongoConfiguration { DatabaseName = "integration", CollectionName = colName };
        await using var source = new MongoSourceNode<ProductRecord>(client, config, filter);

        var results = await DrainAsync(source);

        results.Should().HaveCount(5);
        results.All(r => r.Value > 15).Should().BeTrue();
    }

    [Fact]
    public async Task SourceNode_WithSort_ReturnsDocumentsInOrder()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("integration");
        var colName = UniqueCollection();

        await SeedAsync(db, colName,
        [
            new BsonDocument { ["name"] = "charlie", ["value"] = 3 },
            new BsonDocument { ["name"] = "alpha", ["value"] = 1 },
            new BsonDocument { ["name"] = "bravo", ["value"] = 2 },
        ]);

        var sort = Builders<BsonDocument>.Sort.Ascending("value");
        var config = new MongoConfiguration { DatabaseName = "integration", CollectionName = colName };
        await using var source = new MongoSourceNode<ProductRecord>(client, config, sort: sort);

        var results = await DrainAsync(source);

        results.Select(r => r.Name).Should().ContainInOrder("alpha", "bravo", "charlie");
    }

    // ── sink ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task SinkNode_InsertMany_WritesAllDocuments()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("integration");
        var colName = UniqueCollection();

        var config = new MongoConfiguration
        {
            DatabaseName = "integration",
            CollectionName = colName,
            WriteStrategy = MongoWriteStrategy.InsertMany,
        };

        await using var sink = new MongoSinkNode<ProductRecord>(client, config);

        var stream = Enumerable.Range(1, 50)
            .Select(i => new ProductRecord { Name = $"item{i}", Value = i })
            .ToAsyncEnumerable();

        await using var pipe = new DataStream<ProductRecord>(stream, "test");

        await sink.ConsumeAsync(pipe, DefaultContext(), CancellationToken.None);

        (await CountAsync(db, colName)).Should().Be(50);
    }

    // ── round-trip ────────────────────────────────────────────────────────────

    [Fact]
    public async Task SourceToSink_RoundTrip_PreservesDocumentCount()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("integration");
        var sourceCol = UniqueCollection();
        var sinkCol = UniqueCollection();

        await SeedAsync(db, sourceCol, Enumerable.Range(1, 25).Select(i =>
            new BsonDocument { ["name"] = $"prod{i}", ["value"] = i }));

        var srcConfig = new MongoConfiguration { DatabaseName = "integration", CollectionName = sourceCol };

        var snkConfig = new MongoConfiguration
        {
            DatabaseName = "integration",
            CollectionName = sinkCol,
            WriteStrategy = MongoWriteStrategy.InsertMany,
        };

        await using var source = new MongoSourceNode<ProductRecord>(client, srcConfig);
        await using var sink = new MongoSinkNode<ProductRecord>(client, snkConfig);

        var ctx = DefaultContext();
        var pipe = source.OpenStream(ctx, CancellationToken.None);
        await sink.ConsumeAsync(pipe, ctx, CancellationToken.None);

        (await CountAsync(db, sinkCol)).Should().Be(25);
    }

    // ── large dataset ─────────────────────────────────────────────────────────

    [Fact]
    public async Task SourceNode_LargeDataset_StreamsWithoutBufferingAll()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("integration");
        var colName = UniqueCollection();

        const int docCount = 5_000;
        var col = db.GetCollection<BsonDocument>(colName);

        for (var batch = 0; batch < docCount / 1000; batch++)
        {
            var docs = Enumerable.Range(batch * 1000, 1000)
                .Select(i => new BsonDocument { ["name"] = $"item{i}", ["value"] = i })
                .ToList();

            await col.InsertManyAsync(docs);
        }

        var config = new MongoConfiguration
        {
            DatabaseName = "integration",
            CollectionName = colName,
            BatchSize = 250,
        };

        await using var source = new MongoSourceNode<ProductRecord>(client, config);

        var count = 0;

        await foreach (var _ in source.OpenStream(DefaultContext(), CancellationToken.None))
        {
            count++;
        }

        count.Should().Be(docCount);
    }

    // ── model ─────────────────────────────────────────────────────────────────

    private sealed class ProductRecord
    {
        [MongoField("name")]
        public string Name { get; set; } = string.Empty;

        [MongoField("value")]
        public int Value { get; set; }
    }
}
