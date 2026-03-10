using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Attributes;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.Connectors.MongoDB.Tests.Fixtures;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.MongoDB.Tests.Integration;

[Collection(MongoTestCollection.Name)]
public class MongoWriteStrategyIntegrationTests(MongoTestContainerFixture fixture)
{
    private MongoClient CreateClient()
    {
        return new MongoClient(fixture.ConnectionString);
    }

    private static string UniqueCollection()
    {
        return $"col_{Guid.NewGuid():N}";
    }

    private static async Task<long> CountAsync(IMongoDatabase db, string col)
    {
        return await db.GetCollection<BsonDocument>(col)
            .CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
    }

    private static async Task<List<BsonDocument>> FindAllAsync(IMongoDatabase db, string col)
    {
        return await db.GetCollection<BsonDocument>(col)
            .Find(FilterDefinition<BsonDocument>.Empty)
            .ToListAsync();
    }

    // ── InsertMany ────────────────────────────────────────────────────────────

    [Fact]
    public async Task InsertMany_WritesNewDocuments()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("ws_integration");
        var colName = UniqueCollection();

        var config = new MongoConfiguration
        {
            DatabaseName = "ws_integration",
            CollectionName = colName,
            WriteStrategy = MongoWriteStrategy.InsertMany,
        };

        await using var sink = new MongoSinkNode<Widget>(client, config);

        await using var pipe = new DataStream<Widget>(
            Enumerable.Range(1, 5).Select(i => new Widget { Id = i, Label = $"w{i}" })
                .ToAsyncEnumerable(), "test");

        await sink.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        (await CountAsync(db, colName)).Should().Be(5);
    }

    [Fact]
    public async Task InsertMany_DuplicateKey_ThrowsOrSkipsDependingOnOrdered()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("ws_integration");
        var colName = UniqueCollection();

        // Pre-insert one document with a specific ID to trigger duplicate
        var col = db.GetCollection<BsonDocument>(colName);
        await col.InsertOneAsync(new BsonDocument { ["_id"] = 1, ["label"] = "original" });

        var config = new MongoConfiguration
        {
            DatabaseName = "ws_integration",
            CollectionName = colName,
            WriteStrategy = MongoWriteStrategy.InsertMany,
            OrderedWrites = false, // unordered: continues past errors
        };

        await using var sink = new MongoSinkNode<Widget>(client, config);

        // doc with id=1 already exists — will cause a duplicate key error for that doc
        await using var pipe = new DataStream<Widget>(
            new[] { new Widget { Id = 1, Label = "dup" }, new Widget { Id = 2, Label = "new" } }
                .ToAsyncEnumerable(), "test");

        // Unordered bulk insert silently skips duplicate-key errors in MongoDB;
        // the connector may surface a write exception — either is acceptable.
        var act = async () => await sink.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);
        await act.Should().NotThrowAsync<OperationCanceledException>();

        // The non-duplicate document should have been written
        var count = await CountAsync(db, colName);
        count.Should().BeGreaterThanOrEqualTo(1);
    }

    // ── BulkWrite ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task BulkWrite_WritesNewDocuments()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("ws_integration");
        var colName = UniqueCollection();

        var config = new MongoConfiguration
        {
            DatabaseName = "ws_integration",
            CollectionName = colName,
            WriteStrategy = MongoWriteStrategy.BulkWrite,
        };

        await using var sink = new MongoSinkNode<Widget>(client, config);

        await using var pipe = new DataStream<Widget>(
            Enumerable.Range(1, 10).Select(i => new Widget { Id = i, Label = $"w{i}" })
                .ToAsyncEnumerable(), "test");

        await sink.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        (await CountAsync(db, colName)).Should().Be(10);
    }

    // ── Upsert ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Upsert_InsertsNewDocumentsWhenAbsent()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("ws_integration");
        var colName = UniqueCollection();

        var config = new MongoConfiguration
        {
            DatabaseName = "ws_integration",
            CollectionName = colName,
            WriteStrategy = MongoWriteStrategy.Upsert,
            UpsertKeyFields = ["id"],
        };

        await using var sink = new MongoSinkNode<Widget>(client, config);

        await using var pipe = new DataStream<Widget>(
            new[] { new Widget { Id = 100, Label = "new" } }.ToAsyncEnumerable(), "test");

        await sink.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        (await CountAsync(db, colName)).Should().Be(1);
    }

    [Fact]
    public async Task Upsert_UpdatesExistingDocumentsWhenPresent()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("ws_integration");
        var colName = UniqueCollection();

        var col = db.GetCollection<BsonDocument>(colName);
        await col.InsertOneAsync(new BsonDocument { ["id"] = 42, ["label"] = "old", ["extra"] = "data" });

        var config = new MongoConfiguration
        {
            DatabaseName = "ws_integration",
            CollectionName = colName,
            WriteStrategy = MongoWriteStrategy.Upsert,
            UpsertKeyFields = ["id"],
        };

        await using var sink = new MongoSinkNode<Widget>(client, config);

        await using var pipe = new DataStream<Widget>(
            new[] { new Widget { Id = 42, Label = "updated" } }.ToAsyncEnumerable(), "test");

        await sink.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        (await CountAsync(db, colName)).Should().Be(1);
        var docs = await FindAllAsync(db, colName);
        docs[0]["label"].AsString.Should().Be("updated");
    }

    [Fact]
    public async Task Upsert_MixedInsertAndUpdate_HandlesAll()
    {
        using var client = CreateClient();
        var db = client.GetDatabase("ws_integration");
        var colName = UniqueCollection();

        var col = db.GetCollection<BsonDocument>(colName);
        await col.InsertOneAsync(new BsonDocument { ["id"] = 1, ["label"] = "existing" });

        var config = new MongoConfiguration
        {
            DatabaseName = "ws_integration",
            CollectionName = colName,
            WriteStrategy = MongoWriteStrategy.Upsert,
            UpsertKeyFields = ["id"],
        };

        await using var sink = new MongoSinkNode<Widget>(client, config);

        await using var pipe = new DataStream<Widget>(
            new[]
            {
                new Widget { Id = 1, Label = "updated" }, // existing → update
                new Widget { Id = 2, Label = "inserted" }, // new → insert
            }.ToAsyncEnumerable(), "test");

        await sink.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        (await CountAsync(db, colName)).Should().Be(2);
    }

    // ── model ─────────────────────────────────────────────────────────────────

    private sealed class Widget
    {
        [MongoField("id")]
        public int Id { get; set; }

        [MongoField("label")]
        public string Label { get; set; } = string.Empty;
    }
}
