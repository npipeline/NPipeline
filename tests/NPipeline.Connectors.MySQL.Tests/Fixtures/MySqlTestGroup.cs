namespace NPipeline.Connectors.MySql.Tests.Fixtures;

/// <summary>
///     Test collection fixture for MySQL integration tests.
///     Ensures all tests in the collection share the same container instance.
/// </summary>
[CollectionDefinition("MySql")]
public sealed class MySqlTestGroup : ICollectionFixture<MySqlTestContainerFixture>
{
    // Empty - marker class for collection definition
}
