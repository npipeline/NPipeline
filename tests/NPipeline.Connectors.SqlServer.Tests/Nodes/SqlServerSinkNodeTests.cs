using AwesomeAssertions;
using NPipeline.Connectors;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Nodes;
using WriteStrategy = NPipeline.Connectors.SqlServer.Configuration.SqlServerWriteStrategy;

namespace NPipeline.Connectors.SqlServer.Tests.Nodes;

public class SqlServerSinkNodeTests
{
    [Fact]
    public void Constructor_WithConnectionStringAndTableName_ShouldCreateNode()
    {
        // Arrange
        var connectionString = "Server=localhost;Database=test";
        var tableName = "test_table";

        // Act
        var node = new SqlServerSinkNode<TestRecord>(connectionString, tableName);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndTableNameAndConfiguration_ShouldCreateNode()
    {
        // Arrange
        var connectionString = "Server=localhost;Database=test";
        var tableName = "test_table";
        var configuration = new SqlServerConfiguration();

        // Act
        var node = new SqlServerSinkNode<TestRecord>(connectionString, tableName, configuration);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndTableNameAndSchema_ShouldCreateNode()
    {
        // Arrange
        var connectionString = "Server=localhost;Database=test";
        var tableName = "test_table";
        var schema = "custom_schema";
        var configuration = new SqlServerConfiguration { Schema = schema };

        // Act
        var node = new SqlServerSinkNode<TestRecord>(connectionString, tableName, configuration);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithEmptyTableName_ShouldThrowArgumentExceptionWhenValidateIdentifiersEnabled()
    {
        // Arrange
        var connectionString = "Server=localhost;Database=test";
        var tableName = string.Empty;
        var configuration = new SqlServerConfiguration { ValidateIdentifiers = true };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new SqlServerSinkNode<TestRecord>(connectionString, tableName, configuration));
    }

    [Fact]
    public void Constructor_WithInvalidTableName_ShouldThrowArgumentExceptionWhenValidateIdentifiersEnabled()
    {
        // Arrange
        var connectionString = "Server=localhost;Database=test";
        var tableName = "invalid table name";
        var configuration = new SqlServerConfiguration { ValidateIdentifiers = true };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new SqlServerSinkNode<TestRecord>(connectionString, tableName, configuration));
    }

    [Fact]
    public void Constructor_WithInvalidSchema_ShouldThrowArgumentExceptionWhenValidateIdentifiersEnabled()
    {
        // Arrange
        var connectionString = "Server=localhost;Database=test";
        var tableName = "test_table";
        var schema = "invalid schema";
        var configuration = new SqlServerConfiguration { Schema = schema, ValidateIdentifiers = true };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new SqlServerSinkNode<TestRecord>(connectionString, tableName, configuration));
    }

    [Fact]
    public void Constructor_WithStorageUri_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("sqlserver://localhost:1433/testdb");
        var tableName = "test_table";

        // Act
        var node = new SqlServerSinkNode<TestRecord>(uri, tableName);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithStorageUriAndResolver_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("sqlserver://localhost:1433/testdb");
        var tableName = "test_table";
        var resolver = SqlServerStorageResolverFactory.CreateResolver();

        // Act
        var node = new SqlServerSinkNode<TestRecord>(uri, tableName, resolver: resolver);

        // Assert
        _ = node.Should().NotBeNull();
    }

    public class TestRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
