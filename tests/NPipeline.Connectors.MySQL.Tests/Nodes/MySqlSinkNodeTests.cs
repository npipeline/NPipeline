using AwesomeAssertions;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MySql.Tests.Nodes;

public class MySqlSinkNodeTests
{
    [Fact]
    public void Constructor_WithConnectionStringAndTableName_ShouldCreateNode()
    {
        // Arrange
        var connectionString = "Server=localhost;Port=3306;Database=test;User=root;Password=root";
        var tableName = "test_table";

        // Act
        var node = new MySqlSinkNode<TestRecord>(connectionString, tableName);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndTableNameAndConfiguration_ShouldCreateNode()
    {
        // Arrange
        var connectionString = "Server=localhost;Port=3306;Database=test;User=root;Password=root";
        var tableName = "test_table";
        var configuration = new MySqlConfiguration();

        // Act
        var node = new MySqlSinkNode<TestRecord>(connectionString, tableName, configuration);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithEmptyTableName_ShouldThrowArgumentExceptionWhenValidateIdentifiersEnabled()
    {
        // Arrange
        var connectionString = "Server=localhost;Port=3306;Database=test;User=root;Password=root";
        var tableName = string.Empty;
        var configuration = new MySqlConfiguration { ValidateIdentifiers = true };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new MySqlSinkNode<TestRecord>(connectionString, tableName, configuration));
    }

    [Fact]
    public void Constructor_WithInvalidTableName_ShouldThrowArgumentExceptionWhenValidateIdentifiersEnabled()
    {
        // Arrange
        var connectionString = "Server=localhost;Port=3306;Database=test;User=root;Password=root";
        var tableName = "invalid table name";
        var configuration = new MySqlConfiguration { ValidateIdentifiers = true };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new MySqlSinkNode<TestRecord>(connectionString, tableName, configuration));
    }

    [Fact]
    public void Constructor_WithStorageUri_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("mysql://localhost:3306/testdb");
        var tableName = "test_table";

        // Act
        var node = new MySqlSinkNode<TestRecord>(uri, tableName);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithStorageUriAndResolver_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("mysql://localhost:3306/testdb");
        var tableName = "test_table";
        var resolver = MySqlStorageResolverFactory.CreateResolver();

        // Act
        var node = new MySqlSinkNode<TestRecord>(uri, tableName, resolver: resolver);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithMariaDbStorageUri_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("mariadb://localhost:3306/testdb");
        var tableName = "test_table";

        // Act
        var node = new MySqlSinkNode<TestRecord>(uri, tableName);

        // Assert
        _ = node.Should().NotBeNull();
    }

    public class TestRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
