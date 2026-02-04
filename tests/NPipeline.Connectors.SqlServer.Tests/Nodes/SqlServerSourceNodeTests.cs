using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.Connectors.SqlServer.Nodes;

namespace NPipeline.Connectors.SqlServer.Tests.Nodes;

public class SqlServerSourceNodeTests
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
        const string connectionString = "Server=localhost;Database=test";
        const string query = "SELECT * FROM test_table";

        // Act
        var node = new SqlServerSourceNode<TestRecord>(connectionString, query);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndQueryAndConfiguration_CreatesNode()
    {
        // Arrange
        const string connectionString = "Server=localhost;Database=test";
        const string query = "SELECT * FROM test_table";
        var configuration = new SqlServerConfiguration();

        // Act
        var node = new SqlServerSourceNode<TestRecord>(connectionString, query, configuration);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndQueryAndMapper_CreatesNode()
    {
        // Arrange
        const string connectionString = "Server=localhost;Database=test";
        const string query = "SELECT * FROM test_table";

        static TestRecord Mapper(SqlServerRow row)
        {
            return new TestRecord();
        }

        // Act
        var node = new SqlServerSourceNode<TestRecord>(connectionString, query, Mapper);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionPoolAndQuery_CreatesNode()
    {
        // Arrange
        var connectionPool = A.Fake<ISqlServerConnectionPool>();
        const string query = "SELECT * FROM test_table";

        // Act
        var node = new SqlServerSourceNode<TestRecord>(connectionPool, query);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionPoolAndQueryAndConnectionName_CreatesNode()
    {
        // Arrange
        var connectionPool = A.Fake<ISqlServerConnectionPool>();
        const string query = "SELECT * FROM test_table";
        const string connectionName = "test-connection";

        // Act
        var node = new SqlServerSourceNode<TestRecord>(connectionPool, query, connectionName: connectionName);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithStorageUri_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("sqlserver://localhost:1433/testdb");
        const string query = "SELECT * FROM test_table";

        // Act
        var node = new SqlServerSourceNode<TestRecord>(uri, query);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithStorageUriAndResolver_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("sqlserver://localhost:1433/testdb");
        const string query = "SELECT * FROM test_table";
        var resolver = SqlServerStorageResolverFactory.CreateResolver();

        // Act
        var node = new SqlServerSourceNode<TestRecord>(uri, query, resolver);

        // Assert
        _ = node.Should().NotBeNull();
    }

    #endregion
}
