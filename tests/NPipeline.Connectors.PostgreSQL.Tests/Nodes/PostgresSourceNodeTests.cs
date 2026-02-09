using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Connection;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.PostgreSQL.Tests.Nodes;

public class PostgresSourceNodeTests
{
    public class TestRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithConnectionStringAndQuery_CreatesNode()
    {
        // Arrange
        const string connectionString = "Host=localhost;Database=test";
        const string query = "SELECT * FROM test_table";

        // Act
        var node = new PostgresSourceNode<TestRecord>(connectionString, query);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndQueryAndConfiguration_CreatesNode()
    {
        // Arrange
        const string connectionString = "Host=localhost;Database=test";
        const string query = "SELECT * FROM test_table";
        var configuration = new PostgresConfiguration();

        // Act
        var node = new PostgresSourceNode<TestRecord>(connectionString, query, configuration: configuration);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndQueryAndMapper_CreatesNode()
    {
        // Arrange
        const string connectionString = "Host=localhost;Database=test";
        const string query = "SELECT * FROM test_table";

        static TestRecord Mapper(PostgresRow row)
        {
            return new TestRecord();
        }

        // Act
        var node = new PostgresSourceNode<TestRecord>(connectionString, query, Mapper);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndQueryAndParameters_CreatesNode()
    {
        // Arrange
        const string connectionString = "Host=localhost;Database=test";
        const string query = "SELECT * FROM test_table WHERE id = @id";
        var parameters = new[] { new DatabaseParameter("id", 1) };

        // Act
        var node = new PostgresSourceNode<TestRecord>(connectionString, query, parameters: parameters);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndQueryAndContinueOnError_CreatesNode()
    {
        // Arrange
        const string connectionString = "Host=localhost;Database=test";
        const string query = "SELECT * FROM test_table";

        // Act
        var node = new PostgresSourceNode<TestRecord>(connectionString, query, continueOnError: true);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionPoolAndQuery_CreatesNode()
    {
        // Arrange
        var connectionPool = A.Fake<IPostgresConnectionPool>();
        const string query = "SELECT * FROM test_table";

        // Act
        var node = new PostgresSourceNode<TestRecord>(connectionPool, query);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionPoolAndQueryAndConnectionName_CreatesNode()
    {
        // Arrange
        var connectionPool = A.Fake<IPostgresConnectionPool>();
        const string query = "SELECT * FROM test_table";
        const string connectionName = "test-connection";

        // Act
        var node = new PostgresSourceNode<TestRecord>(connectionPool, query, connectionName: connectionName);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithStorageUri_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost:5432/testdb");
        const string query = "SELECT * FROM test_table";

        // Act
        var node = new PostgresSourceNode<TestRecord>(uri, query);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithStorageUriAndResolver_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost:5432/testdb");
        const string query = "SELECT * FROM test_table";
        var resolver = PostgresStorageResolverFactory.CreateResolver();

        // Act
        var node = new PostgresSourceNode<TestRecord>(uri, query, resolver);

        // Assert
        _ = node.Should().NotBeNull();
    }

    #endregion
}
