using MongoDB.Bson;
using MongoDB.Driver;
using Testcontainers.MongoDb;

namespace NPipeline.Connectors.MongoDB.Tests.Fixtures;

/// <summary>
///     Test fixture that provides a MongoDB container for integration tests.
///     Uses Testcontainers.MongoDb to spin up mongo:8 on a random port.
///     Implements <see cref="IAsyncLifetime" /> so xUnit manages the container lifetime.
/// </summary>
public sealed class MongoTestContainerFixture : IAsyncLifetime
{
    private readonly MongoDbContainer _container = new MongoDbBuilder()
        .WithImage("mongo:8")
        .WithReplicaSet()
        .WithLabel("npipeline-test", "mongo-integration")
        .Build();

    /// <summary>Gets the MongoDB connection string for the running container.</summary>
    public string ConnectionString => _container.GetConnectionString();

    /// <summary>Gets whether the fixture is running against a replica set primary.</summary>
    public bool IsReplicaSetReady { get; private set; }

    /// <summary>Gets whether the container has been started successfully.</summary>
    public bool IsRunning { get; private set; }

    /// <inheritdoc />
    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        await WaitForReplicaSetPrimaryAsync();
        IsRunning = true;
        IsReplicaSetReady = true;
    }

    /// <inheritdoc />
    public async Task DisposeAsync()
    {
        IsRunning = false;
        IsReplicaSetReady = false;
        await _container.DisposeAsync().AsTask();
    }

    private async Task WaitForReplicaSetPrimaryAsync()
    {
        using var client = new MongoClient(ConnectionString);
        var database = client.GetDatabase("admin");

        var timeout = TimeSpan.FromSeconds(30);
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var hello = await database.RunCommandAsync<BsonDocument>(new BsonDocument("hello", 1));

                if (hello.TryGetValue("setName", out var setName) &&
                    setName.AsString == "rs0" &&
                    hello.TryGetValue("isWritablePrimary", out var writablePrimary) &&
                    writablePrimary.ToBoolean())
                    return;
            }
            catch
            {
                // Keep polling until timeout.
            }

            await Task.Delay(500);
        }

        throw new TimeoutException("MongoDB replica set primary was not ready within 30 seconds.");
    }
}
