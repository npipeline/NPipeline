namespace NPipeline.Connectors.SqlServer.Tests.Fixtures;

/// <summary>
///     Test collection fixture for SQL Server integration tests.
///     Ensures all tests in the collection share the same container instance.
/// </summary>
[CollectionDefinition("SqlServer")]
public sealed class SqlServerTestGroup : ICollectionFixture<SqlServerTestContainerFixture>
{
    // Empty - marker class for collection definition
}
