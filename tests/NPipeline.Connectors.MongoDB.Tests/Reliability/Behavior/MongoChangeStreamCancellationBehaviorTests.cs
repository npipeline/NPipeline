using FakeItEasy;
using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.ChangeStream;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.MongoDB.Tests.Reliability.Behavior;

/// <summary>
///     Cancellation handling while the change stream is being opened (M1 in <c>plans/resilience-improvements.md</c>).
/// </summary>
public sealed class MongoChangeStreamCancellationBehaviorTests
{
    [Fact]
    public async Task CancelledBeforeTheStreamOpens_SurfacesAsCancellation()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var act = () => DrainAsync(CreateNode(A.Fake<IMongoClient>(), new MongoChangeStreamConfiguration()), cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>("a cancelled source must not look like one that drained");
    }

    [Fact]
    public async Task CancelledWhileOpening_IsNotTreatedAsADocumentError()
    {
        using var cts = new CancellationTokenSource();
        var database = A.Fake<IMongoDatabase>();
        var client = A.Fake<IMongoClient>();
        A.CallTo(() => client.GetDatabase(A<string>._, A<MongoDatabaseSettings>._)).Returns(database);

        A.CallTo(database)
            .Where(call => call.Method.Name == nameof(IMongoDatabase.WatchAsync))
            .Invokes(() => cts.Cancel())
            .Throws(() => new OperationCanceledException(cts.Token));

        // The error handler asks to end the stream quietly on errors. Cancellation is not an error it may swallow.
        var configuration = new MongoChangeStreamConfiguration
        {
            ContinueOnError = true,
            DocumentErrorHandler = (_, _) => true,
        };

        var act = () => DrainAsync(CreateNode(client, configuration), cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
    }

    private static MongoChangeStreamSourceNode<BsonDocument> CreateNode(IMongoClient client, MongoChangeStreamConfiguration configuration)
    {
        configuration.DatabaseName = "orders";
        return new MongoChangeStreamSourceNode<BsonDocument>(client, "orders", mapper: e => e.FullDocument!, configuration: configuration);
    }

    private static async Task DrainAsync(MongoChangeStreamSourceNode<BsonDocument> node, CancellationToken cancellationToken)
    {
        await foreach (var _ in node.OpenStream(new PipelineContext(), cancellationToken).WithCancellation(cancellationToken))
        {
        }
    }
}
