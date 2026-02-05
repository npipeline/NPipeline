using AwesomeAssertions;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.StorageProviders.Models;
using WriteStrategy = NPipeline.Connectors.PostgreSQL.Configuration.PostgresWriteStrategy;

namespace NPipeline.Connectors.PostgreSQL.Tests.Nodes;

public class PostgresSinkNodeTests
{
    [Fact]
    public void Constructor_WithConnectionStringAndTableName_ShouldCreateNode()
    {
        // Arrange
        var connectionString = "Host=localhost;Database=test";
        var tableName = "test_table";

        // Act
        var node = new PostgresSinkNode<TestRecord>(connectionString, tableName);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndTableNameAndWriteStrategy_ShouldCreateNode()
    {
        // Arrange
        var connectionString = "Host=localhost;Database=test";
        var tableName = "test_table";
        var writeStrategy = WriteStrategy.PerRow;

        // Act
        var node = new PostgresSinkNode<TestRecord>(connectionString, tableName, writeStrategy);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndTableNameAndConfiguration_ShouldCreateNode()
    {
        // Arrange
        var connectionString = "Host=localhost;Database=test";
        var tableName = "test_table";
        var configuration = new PostgresConfiguration();

        // Act
        var node = new PostgresSinkNode<TestRecord>(connectionString, tableName, configuration: configuration);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndTableNameAndSchema_ShouldCreateNode()
    {
        // Arrange
        var connectionString = "Host=localhost;Database=test";
        var tableName = "test_table";
        var schema = "custom_schema";

        // Act
        var node = new PostgresSinkNode<TestRecord>(connectionString, tableName, schema: schema);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithEmptyTableName_ShouldThrowArgumentExceptionWhenValidateIdentifiersEnabled()
    {
        // Arrange
        var connectionString = "Host=localhost;Database=test";
        var tableName = string.Empty;
        var configuration = new PostgresConfiguration { ValidateIdentifiers = true };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new PostgresSinkNode<TestRecord>(connectionString, tableName, configuration: configuration));
    }

    [Fact]
    public void Constructor_WithInvalidTableName_ShouldThrowArgumentExceptionWhenValidateIdentifiersEnabled()
    {
        // Arrange
        var connectionString = "Host=localhost;Database=test";
        var tableName = "invalid table name";
        var configuration = new PostgresConfiguration { ValidateIdentifiers = true };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new PostgresSinkNode<TestRecord>(connectionString, tableName, configuration: configuration));
    }

    [Fact]
    public void Constructor_WithInvalidSchema_ShouldThrowArgumentExceptionWhenValidateIdentifiersEnabled()
    {
        // Arrange
        var connectionString = "Host=localhost;Database=test";
        var tableName = "test_table";
        var schema = "invalid schema";
        var configuration = new PostgresConfiguration { ValidateIdentifiers = true };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new PostgresSinkNode<TestRecord>(connectionString, tableName, schema: schema, configuration: configuration));
    }

    [Fact]
    public void Constructor_WithStorageUri_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost:5432/testdb");
        var tableName = "test_table";

        // Act
        var node = new PostgresSinkNode<TestRecord>(uri, tableName);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithStorageUriAndResolver_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost:5432/testdb");
        var tableName = "test_table";
        var resolver = PostgresStorageResolverFactory.CreateResolver();

        // Act
        var node = new PostgresSinkNode<TestRecord>(uri, tableName, resolver: resolver);

        // Assert
        _ = node.Should().NotBeNull();
    }

    public class TestRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
