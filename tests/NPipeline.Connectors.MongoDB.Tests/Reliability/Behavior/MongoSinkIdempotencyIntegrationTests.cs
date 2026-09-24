using FakeItEasy;
using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.Connectors.MongoDB.Reliability;
using NPipeline.Connectors.MongoDB.Tests.Fixtures;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using NResilience;
using OurMongoWriteException = NPipeline.Connectors.MongoDB.Exceptions.MongoWriteException;

namespace NPipeline.Connectors.MongoDB.Tests.Reliability.Behavior;

/// <summary>
///     A retried batch write against a real server, where the first attempt is applied but its reply is lost, the case
///     a retry can turn into duplicate documents.
/// </summary>
[Collection(MongoTestCollection.Name)]
public sealed class MongoSinkIdempotencyIntegrationTests(MongoTestContainerFixture fixture)
{
    private const string Database = "resilience_idempotency";

    [Theory]
    [InlineData(MongoWriteStrategy.BulkWrite)]
    [InlineData(MongoWriteStrategy.InsertMany)]
    public async Task RetryAfterALostReply_DoesNotDuplicateDocuments(MongoWriteStrategy strategy)
    {
        var (count, error) = await WriteWithLostFirstReplyAsync(strategy, OnDuplicateAction.Ignore);

        error.Should().BeNull();
        count.Should().Be(5);
    }

    [Fact]
    public async Task RetryAfterALostReply_WithOnDuplicateFail_ReportsTheDuplicateButWritesNothingTwice()
    {
        var (count, error) = await WriteWithLostFirstReplyAsync(MongoWriteStrategy.BulkWrite, OnDuplicateAction.Fail);

        // The retry finds the documents the lost attempt wrote. That is reported, not repeated.
        error.Should().BeOfType<OurMongoWriteException>();
        count.Should().Be(5);
    }

    private async Task<(long Count, Exception? Error)> WriteWithLostFirstReplyAsync(MongoWriteStrategy strategy, OnDuplicateAction onDuplicate)
    {
        using var realClient = new MongoClient(fixture.ConnectionString);
        var collectionName = $"col_{Guid.NewGuid():N}";
        var real = realClient.GetDatabase(Database).GetCollection<BsonDocument>(collectionName);

        var lost = false;
        var collection = A.Fake<IMongoCollection<BsonDocument>>(o => o.Wrapping(real));

        // The first attempt reaches the server and is applied; then the connection drops before the reply arrives.
        A.CallTo(() => collection.BulkWriteAsync(A<IEnumerable<WriteModel<BsonDocument>>>._, A<BulkWriteOptions>._, A<CancellationToken>._))
            .ReturnsLazily(async call =>
            {
                var result = await real.BulkWriteAsync(
                    call.GetArgument<IEnumerable<WriteModel<BsonDocument>>>(0)!,
                    call.GetArgument<BulkWriteOptions>(1),
                    call.GetArgument<CancellationToken>(2));

                return LoseFirstReply(ref lost)
                    ? throw MongoResilienceBehaviorTests.ConnectionError()
                    : result;
            });

        A.CallTo(() => collection.InsertManyAsync(A<IEnumerable<BsonDocument>>._, A<InsertManyOptions>._, A<CancellationToken>._))
            .ReturnsLazily(async call =>
            {
                await real.InsertManyAsync(
                    call.GetArgument<IEnumerable<BsonDocument>>(0)!,
                    call.GetArgument<InsertManyOptions>(1),
                    call.GetArgument<CancellationToken>(2));

                if (LoseFirstReply(ref lost))
                    throw MongoResilienceBehaviorTests.ConnectionError();
            });

        var client = A.Fake<IMongoClient>();
        var database = A.Fake<IMongoDatabase>();
        A.CallTo(() => client.GetDatabase(A<string>._, A<MongoDatabaseSettings>._)).Returns(database);
        A.CallTo(() => database.GetCollection<BsonDocument>(A<string>._, A<MongoCollectionSettings>._)).Returns(collection);

        var configuration = new MongoConfiguration
        {
            DatabaseName = Database,
            CollectionName = collectionName,
            WriteStrategy = strategy,
            OnDuplicate = onDuplicate,
            Resilience = MongoConnectorResilience.Default with { Backoff = Backoff.None },
        };

        // No natural key: each document's _id is generated, which is where a retry used to create duplicates.
        await using var sink = new MongoSinkNode<Item>(client, configuration, i => new BsonDocument("label", i.Label));

        var input = new DataStream<Item>(
            Enumerable.Range(1, 5).Select(i => new Item { Label = $"i{i}" }).ToAsyncEnumerable(), "test");

        Exception? error = null;

        try
        {
            await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
        }
        catch (Exception ex)
        {
            error = ex;
        }

        var count = await real.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        return (count, error);
    }

    private static bool LoseFirstReply(ref bool lost)
    {
        if (lost)
            return false;

        lost = true;
        return true;
    }

    public sealed class Item
    {
        public string Label { get; set; } = string.Empty;
    }
}
