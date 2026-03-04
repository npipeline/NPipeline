using AwesomeAssertions;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Mapping;
using NPipeline.Connectors.MySql.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MySql.Tests.Nodes;

public class MySqlSourceNodeTests
{
    private const string ConnectionString = "Server=localhost;Port=3306;Database=test;User=root;Password=root";
    private const string Query = "SELECT id, name FROM test_table";

    [Fact]
    public void Constructor_WithConnectionStringAndQuery_ShouldCreateNode()
    {
        // Act
        var node = new MySqlSourceNode<TestRecord>(ConnectionString, Query);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndQueryAndConfiguration_ShouldCreateNode()
    {
        // Arrange
        var configuration = new MySqlConfiguration();

        // Act
        var node = new MySqlSourceNode<TestRecord>(ConnectionString, Query, configuration);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionStringAndQueryAndCustomMapper_ShouldCreateNode()
    {
        // Arrange
        Func<MySqlRow, TestRecord> mapper = row => new TestRecord
        {
            Id = row.Get<int>("id"),
            Name = row.Get<string>("name"),
        };

        // Act
        var node = new MySqlSourceNode<TestRecord>(ConnectionString, Query, mapper);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullConnectionString_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(
            () => new MySqlSourceNode<TestRecord>((string)null!, Query));
    }

    [Fact]
    public void Constructor_WithEmptyConnectionString_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(
            () => new MySqlSourceNode<TestRecord>(string.Empty, Query));
    }

    [Fact]
    public void Constructor_WithNullQuery_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(
            () => new MySqlSourceNode<TestRecord>(ConnectionString, null!));
    }

    [Fact]
    public void Constructor_WithStorageUri_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("mysql://localhost:3306/testdb");

        // Act
        var node = new MySqlSourceNode<TestRecord>(uri, Query);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithStorageUriAndResolver_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("mysql://localhost:3306/testdb");
        var resolver = MySqlStorageResolverFactory.CreateResolver();

        // Act
        var node = new MySqlSourceNode<TestRecord>(uri, Query, resolver);

        // Assert
        _ = node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithMariaDbUri_CreatesNode()
    {
        // Arrange
        var uri = StorageUri.Parse("mariadb://localhost:3306/testdb");

        // Act
        var node = new MySqlSourceNode<TestRecord>(uri, Query);

        // Assert
        _ = node.Should().NotBeNull();
    }

    public class TestRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
