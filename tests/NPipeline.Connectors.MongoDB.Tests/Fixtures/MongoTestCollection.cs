namespace NPipeline.Connectors.MongoDB.Tests.Fixtures;

/// <summary>
///     Marks tests as belonging to the MongoDB integration test collection.
///     Ensures a single container is shared across all tests in the collection.
/// </summary>
[CollectionDefinition(Name)]
public sealed class MongoTestCollection : ICollectionFixture<MongoTestContainerFixture>
{
    /// <summary>The name of the xUnit collection.</summary>
    public const string Name = "MongoTestCollection";
}
