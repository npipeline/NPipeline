using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Attributes;
using NPipeline.Connectors.MongoDB.ChangeStream;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.Connectors.MongoDB.Tests.Fixtures;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.MongoDB.Tests.Integration;

/// <summary>
///     Integration tests for <see cref="MongoChangeStreamSourceNode{T}" />.
///     These tests require a replica set and the shared fixture provisions one.
/// </summary>
[Collection(MongoTestCollection.Name)]
public class MongoChangeStreamIntegrationTests(MongoTestContainerFixture fixture)
{
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

    // ── tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ChangeStreamNode_ReceivesInsertEvents()
    {
        _ = fixture.IsReplicaSetReady.Should().BeTrue("change stream tests require replica set mode");

        using var client = CreateClient();

        var colName = UniqueCollection();

        var csConfig = new MongoChangeStreamConfiguration
        {
            OperationTypes = [MongoChangeStreamOperationType.Insert],
            MaxAwaitTime = TimeSpan.FromSeconds(5),
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        await using var source = new MongoChangeStreamSourceNode<EventDoc>(
            client,
            "cs_integration",
            colName,
            [MongoChangeStreamOperationType.Insert],
            configuration: csConfig);

        var received = new List<EventDoc>();

        // Start consuming in background
        var consumeTask = Task.Run(async () =>
        {
            await foreach (var item in source.Initialize(DefaultContext(), cts.Token).WithCancellation(cts.Token))
            {
                received.Add(item);

                if (received.Count >= 3)
                    break;
            }
        }, cts.Token);

        // Give the watch a moment to start
        await Task.Delay(500, cts.Token);

        // Insert documents into the watched collection
        var db = client.GetDatabase("cs_integration");
        var col = db.GetCollection<BsonDocument>(colName);

        for (var i = 1; i <= 3; i++)
        {
            await col.InsertOneAsync(new BsonDocument { ["name"] = $"event{i}" }, cancellationToken: cts.Token);
        }

        await consumeTask.WaitAsync(cts.Token);

        _ = received.Should().HaveCount(3);
        _ = received.Select(r => r.Name).Should().Contain(n => n.StartsWith("event"));
    }

    [Fact]
    public async Task ChangeStreamNode_ExposesResumeTokenAfterEvents()
    {
        _ = fixture.IsReplicaSetReady.Should().BeTrue("change stream tests require replica set mode");

        using var client = CreateClient();

        var colName = UniqueCollection();

        var config = new MongoChangeStreamConfiguration
        {
            OperationTypes = [MongoChangeStreamOperationType.Insert],
            MaxAwaitTime = TimeSpan.FromSeconds(5),
        };

        await using var source = new MongoChangeStreamSourceNode<EventDoc>(
            client,
            "cs_integration",
            colName,
            [MongoChangeStreamOperationType.Insert],
            configuration: config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var db = client.GetDatabase("cs_integration");
        var col = db.GetCollection<BsonDocument>(colName);

        var consumeTask = Task.Run(async () =>
        {
            await foreach (var _ in source.Initialize(DefaultContext(), cts.Token).WithCancellation(cts.Token))
            {
                break;
            }
        }, cts.Token);

        await Task.Delay(500, cts.Token);
        await col.InsertOneAsync(new BsonDocument { ["name"] = "event-resume" }, cancellationToken: cts.Token);

        await consumeTask.WaitAsync(cts.Token);

        _ = source.ResumeToken.Should().NotBeNull();
    }

    // ── model ─────────────────────────────────────────────────────────────────

    private sealed class EventDoc
    {
        [MongoField("name")]
        public string Name { get; set; } = string.Empty;
    }
}
