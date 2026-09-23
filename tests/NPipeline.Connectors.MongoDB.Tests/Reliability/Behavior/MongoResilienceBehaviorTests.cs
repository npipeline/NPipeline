using System.Net;
using FakeItEasy;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using NPipeline.Connectors.MongoDB.ChangeStream;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.Connectors.MongoDB.Reliability;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using NResilience;
using OurMongoWriteException = NPipeline.Connectors.MongoDB.Exceptions.MongoWriteException;

namespace NPipeline.Connectors.MongoDB.Tests.Reliability.Behavior;

public sealed class MongoResilienceBehaviorTests
{
    private static readonly ConnectionId Connection = new(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017)));

    internal static MongoConnectionException ConnectionError()
    {
        return new MongoConnectionException(Connection, "connection reset");
    }

    private static MongoCommandException CommandError(int code)
    {
        return new MongoCommandException(Connection, $"code {code}", new BsonDocument("ping", 1), new BsonDocument { { "ok", 0 }, { "code", code } });
    }

    // ── Presets ────────────────────────────────────────────────────────────────

    [Fact]
    public void Default_ReproducesTheSinkRetryLoop()
    {
        var preset = MongoConnectorResilience.Default;

        // MaxRetryAttempts = 3 made four calls (the loop ran while attempts <= 3), starting from RetryDelay = 1s.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(1));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse();
        preset.Classifier.Should().BeSameAs(MongoConnectorResilience.Classifier);
        new MongoConfiguration().Resilience.Should().BeSameAs(preset);
    }

    [Fact]
    public void ChangeStream_ReproducesTheOpenLoop()
    {
        var preset = MongoConnectorResilience.ChangeStream;

        // MaxRetryAttempts = 3 made four attempts to open, with RetryDelay = 2s between them.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(2));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Classifier.Should().BeSameAs(MongoConnectorResilience.Classifier);
        new MongoChangeStreamConfiguration().Resilience.Should().BeSameAs(preset);
        new MongoChangeStreamConfiguration().Clone().Resilience.Should().BeSameAs(preset);
    }

    // ── Classifier ─────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(6)] // HostUnreachable
    [InlineData(89)] // NetworkTimeout
    [InlineData(91)] // ShutdownInProgress
    [InlineData(189)] // PrimarySteppedDown
    [InlineData(10107)] // NotWritablePrimary
    [InlineData(11602)] // InterruptedDueToReplStateChange
    public void Classifier_TreatsRetryableCommandCodesAsTransient(int code)
    {
        MongoConnectorResilience.Classifier.ClassifyException(CommandError(code)).Kind.Should().Be(VerdictKind.Transient);
    }

    [Theory]
    [InlineData(2)] // BadValue
    [InlineData(13)] // Unauthorized
    [InlineData(11000)] // DuplicateKey
    public void Classifier_TreatsOtherCommandCodesAsPermanent(int code)
    {
        MongoConnectorResilience.Classifier.ClassifyException(CommandError(code)).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public void Classifier_JudgesConnectionFailuresTimeoutsLabelsAndConnectorErrors()
    {
        var classifier = MongoConnectorResilience.Classifier;

        classifier.ClassifyException(ConnectionError()).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new TimeoutException("server selection")).Kind.Should().Be(VerdictKind.Transient);

        var retryable = CommandError(2);
        retryable.AddErrorLabel("RetryableWriteError");
        classifier.ClassifyException(retryable).Kind.Should().Be(VerdictKind.Transient);

        var overloaded = CommandError(2);
        overloaded.AddErrorLabel("SystemOverloadedError");
        classifier.ClassifyException(overloaded).Kind.Should().Be(VerdictKind.Throttled);

        // The connector's own write exception is a mapping failure or a partly applied bulk write.
        classifier.ClassifyException(new OurMongoWriteException("bulk write error", "c", 10, ConnectionError()))
            .Kind.Should().Be(VerdictKind.Permanent);

        classifier.ClassifyException(new InvalidOperationException("bug")).Kind.Should().Be(VerdictKind.Permanent);
    }

    // ── Sink ───────────────────────────────────────────────────────────────────

    private static (IMongoClient Client, IMongoCollection<BsonDocument> Collection) FakeClient()
    {
        var client = A.Fake<IMongoClient>();
        var database = A.Fake<IMongoDatabase>();
        var collection = A.Fake<IMongoCollection<BsonDocument>>();

        A.CallTo(() => client.GetDatabase(A<string>._, A<MongoDatabaseSettings>._)).Returns(database);
        A.CallTo(() => database.GetCollection<BsonDocument>(A<string>._, A<MongoCollectionSettings>._)).Returns(collection);

        return (client, collection);
    }

    private static MongoConfiguration SinkConfig(NResilience.Resilience? resilience = null)
    {
        return new MongoConfiguration
        {
            DatabaseName = "db",
            CollectionName = "widgets",
            Resilience = resilience ?? MongoConnectorResilience.Default with { Backoff = Backoff.None },
        };
    }

    private static Task WriteAsync(IMongoClient client, MongoConfiguration configuration, Func<Widget, BsonDocument> mapper,
        CancellationToken cancellationToken = default)
    {
        var sink = new MongoSinkNode<Widget>(client, configuration, mapper);
        var input = new DataStream<Widget>(Enumerable.Range(1, 3).Select(i => new Widget { Label = $"w{i}" }).ToAsyncEnumerable(), "test");
        return sink.ConsumeAsync(input, new PipelineContext(), cancellationToken);
    }

    [Fact]
    public async Task Sink_RetriesTransientFailures_AndResendsTheSameDocuments()
    {
        var (client, collection) = FakeClient();
        var sent = new List<List<BsonDocument>>();
        var mapCalls = 0;

        A.CallTo(() => collection.BulkWriteAsync(A<IEnumerable<WriteModel<BsonDocument>>>._, A<BulkWriteOptions>._, A<CancellationToken>._))
            .ReturnsLazily(call =>
            {
                sent.Add(call.GetArgument<IEnumerable<WriteModel<BsonDocument>>>(0)!
                    .Select(m => ((InsertOneModel<BsonDocument>)m).Document).ToList());

                return sent.Count < 3
                    ? Task.FromException<BulkWriteResult<BsonDocument>>(ConnectionError())
                    : Task.FromResult<BulkWriteResult<BsonDocument>>(null!);
            });

        await WriteAsync(client, SinkConfig(), w =>
        {
            mapCalls++;
            return new BsonDocument("label", w.Label);
        });

        sent.Should().HaveCount(3);
        mapCalls.Should().Be(3, "the batch is mapped once, not once per attempt");

        // Every document carries an _id fixed before the first attempt, so a retry can only find what an earlier
        // attempt wrote (a duplicate key), never write it a second time under a new id.
        var firstIds = sent[0].Select(d => d["_id"]).ToList();
        firstIds.Should().OnlyContain(id => id.IsObjectId);

        foreach (var attempt in sent)
        {
            attempt.Select(d => d["_id"]).Should().Equal(firstIds);
        }
    }

    [Fact]
    public async Task Sink_WithPersistentTransientFailure_MakesFourAttempts()
    {
        var (client, collection) = FakeClient();
        var attempts = 0;

        A.CallTo(() => collection.BulkWriteAsync(A<IEnumerable<WriteModel<BsonDocument>>>._, A<BulkWriteOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                attempts++;
                return Task.FromException<BulkWriteResult<BsonDocument>>(ConnectionError());
            });

        var act = () => WriteAsync(client, SinkConfig(), w => new BsonDocument("label", w.Label));

        var thrown = await act.Should().ThrowAsync<OurMongoWriteException>();
        thrown.Which.InnerException.Should().BeOfType<MongoConnectionException>();
        attempts.Should().Be(4);
    }

    [Fact]
    public async Task Sink_DoesNotRetryAMappingFailure()
    {
        var (client, collection) = FakeClient();

        var act = () => WriteAsync(client, SinkConfig(), _ => throw new FormatException("bad widget"));

        _ = await act.Should().ThrowAsync<OurMongoWriteException>();
        A.CallTo(() => collection.BulkWriteAsync(A<IEnumerable<WriteModel<BsonDocument>>>._, A<BulkWriteOptions>._, A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task Sink_WhenPipelineTokenIsCancelledDuringBackoff_StopsWithoutRetrying()
    {
        var (client, collection) = FakeClient();
        using var cts = new CancellationTokenSource();
        var attempts = 0;

        A.CallTo(() => collection.BulkWriteAsync(A<IEnumerable<WriteModel<BsonDocument>>>._, A<BulkWriteOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                attempts++;
                cts.CancelAfter(TimeSpan.FromMilliseconds(50));
                return Task.FromException<BulkWriteResult<BsonDocument>>(ConnectionError());
            });

        // The real preset: the first retry waits up to a second, and the cancellation lands in that wait.
        var act = () => WriteAsync(client, SinkConfig(MongoConnectorResilience.Default), w => new BsonDocument("label", w.Label), cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        attempts.Should().Be(1);
    }

    [Fact]
    public async Task Sink_WithForeignCancellation_FailsWithoutRetrying()
    {
        var (client, collection) = FakeClient();
        var attempts = 0;

        A.CallTo(() => collection.BulkWriteAsync(A<IEnumerable<WriteModel<BsonDocument>>>._, A<BulkWriteOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                attempts++;
                return Task.FromException<BulkWriteResult<BsonDocument>>(new OperationCanceledException("cancelled by something else"));
            });

        var act = () => WriteAsync(client, SinkConfig(), w => new BsonDocument("label", w.Label), CancellationToken.None);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        attempts.Should().Be(1);
    }

    // ── Change stream ──────────────────────────────────────────────────────────

    private static (IMongoClient Client, IMongoDatabase Database) FakeDatabase()
    {
        var client = A.Fake<IMongoClient>();
        var database = A.Fake<IMongoDatabase>();
        A.CallTo(() => client.GetDatabase(A<string>._, A<MongoDatabaseSettings>._)).Returns(database);
        return (client, database);
    }

    private static IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> Cursor(params ChangeStreamDocument<BsonDocument>[] changes)
    {
        var cursor = A.Fake<IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>>();
        var delivered = false;

        A.CallTo(() => cursor.MoveNextAsync(A<CancellationToken>._)).ReturnsLazily(() =>
        {
            var hasBatch = !delivered && changes.Length > 0;
            delivered = true;
            return Task.FromResult(hasBatch);
        });

        A.CallTo(() => cursor.Current).Returns(changes);
        return cursor;
    }

    private static ChangeStreamDocument<BsonDocument> Change(string token, int id)
    {
        var backing = new BsonDocument
        {
            { "_id", new BsonDocument("_data", token) },
            { "operationType", "insert" },
            { "ns", new BsonDocument { { "db", "orders" }, { "coll", "c" } } },
            { "documentKey", new BsonDocument("_id", id) },
            { "fullDocument", new BsonDocument { { "_id", id }, { "label", $"w{id}" } } },
        };

        return new ChangeStreamDocument<BsonDocument>(backing, BsonDocumentSerializer.Instance);
    }

    private static MongoChangeStreamSourceNode<BsonDocument> ChangeStreamNode(IMongoClient client, NResilience.Resilience? resilience = null)
    {
        var configuration = new MongoChangeStreamConfiguration
        {
            DatabaseName = "orders",
            Resilience = resilience ?? MongoConnectorResilience.ChangeStream with { Backoff = Backoff.None },
        };

        return new MongoChangeStreamSourceNode<BsonDocument>(client, "orders", mapper: e => e.FullDocument!, configuration: configuration);
    }

    private static async Task<List<BsonDocument>> DrainAsync(MongoChangeStreamSourceNode<BsonDocument> node, CancellationToken cancellationToken = default)
    {
        var items = new List<BsonDocument>();

        await foreach (var item in node.OpenStream(new PipelineContext(), cancellationToken).WithCancellation(cancellationToken))
        {
            items.Add(item);
        }

        return items;
    }

    [Fact]
    public async Task ChangeStream_RetriesATransientFailureToOpen()
    {
        var (client, database) = FakeDatabase();
        var opens = 0;

        A.CallTo(() => database.WatchAsync(
                A<PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>>>._,
                A<ChangeStreamOptions>._,
                A<CancellationToken>._))
            .ReturnsLazily(() => ++opens < 3
                ? Task.FromException<IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>>(ConnectionError())
                : Task.FromResult(Cursor(Change("t1", 1))));

        var items = await DrainAsync(ChangeStreamNode(client));

        opens.Should().Be(3);
        items.Should().ContainSingle().Which["_id"].AsInt32.Should().Be(1);
    }

    [Fact]
    public async Task ChangeStream_DoesNotRetryAPermanentFailureToOpen()
    {
        var (client, database) = FakeDatabase();
        var opens = 0;

        A.CallTo(() => database.WatchAsync(
                A<PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>>>._,
                A<ChangeStreamOptions>._,
                A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                opens++;
                return Task.FromException<IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>>(CommandError(13));
            });

        var act = () => DrainAsync(ChangeStreamNode(client));

        _ = await act.Should().ThrowAsync<MongoCommandException>();
        opens.Should().Be(1);
    }

    [Fact]
    public async Task ChangeStream_WithForeignCancellationWhileOpening_FailsInsteadOfEndingTheStream()
    {
        var (client, database) = FakeDatabase();
        var opens = 0;

        A.CallTo(() => database.WatchAsync(
                A<PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>>>._,
                A<ChangeStreamOptions>._,
                A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                opens++;
                return Task.FromException<IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>>(new OperationCanceledException("not the pipeline"));
            });

        var act = () => DrainAsync(ChangeStreamNode(client), CancellationToken.None);

        _ = await act.Should().ThrowAsync<OperationCanceledException>("an unexplained cancellation is a failure, not the end of the stream");
        opens.Should().Be(1);
    }

    [Fact]
    public async Task ChangeStream_OpenedAgain_ResumesAfterTheLastEmittedChange()
    {
        var (client, database) = FakeDatabase();
        var seen = new List<ChangeStreamOptions>();

        A.CallTo(() => database.WatchAsync(
                A<PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>>>._,
                A<ChangeStreamOptions>._,
                A<CancellationToken>._))
            .ReturnsLazily(call =>
            {
                seen.Add(call.GetArgument<ChangeStreamOptions>(1)!);
                return Task.FromResult(seen.Count == 1 ? Cursor(Change("t1", 1), Change("t2", 2)) : Cursor());
            });

        var node = ChangeStreamNode(client);

        (await DrainAsync(node)).Should().HaveCount(2);
        _ = await DrainAsync(node);

        seen.Should().HaveCount(2);
        seen[0].ResumeAfter.Should().BeNull();
        seen[1].ResumeAfter.Should().NotBeNull();
        seen[1].ResumeAfter!.Equals(new BsonDocument("_data", "t2")).Should().BeTrue();
    }

    public sealed class Widget
    {
        public string Label { get; set; } = string.Empty;
    }
}
