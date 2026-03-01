using Npgsql;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Nodes;

public class RedshiftSourceNodeTests
{
    [Fact]
    public void Constructor_WithNullConnectionString_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSourceNode<TestRow>(
            (string)null!,
            "SELECT * FROM test");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithEmptyConnectionString_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSourceNode<TestRow>(
            "",
            "SELECT * FROM test");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithWhitespaceConnectionString_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSourceNode<TestRow>(
            "   ",
            "SELECT * FROM test");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullQuery_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSourceNode<TestRow>(
            "Host=localhost;Database=test",
            null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithEmptyQuery_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSourceNode<TestRow>(
            "Host=localhost;Database=test",
            "");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithWhitespaceQuery_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSourceNode<TestRow>(
            "Host=localhost;Database=test",
            "   ");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithValidArguments_ShouldNotThrow()
    {
        // Act
        var node = new RedshiftSourceNode<TestRow>(
            "Host=localhost;Database=test",
            "SELECT * FROM test");

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullConnectionPool_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSourceNode<TestRow>(
            (IRedshiftConnectionPool)null!,
            "SELECT * FROM test");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithConnectionPool_AndNullQuery_ShouldThrow()
    {
        // Arrange
        var mockPool = new MockRedshiftConnectionPool();

        // Act
        var act = () => new RedshiftSourceNode<TestRow>(
            mockPool,
            null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithCustomMapper_ShouldNotThrow()
    {
        // Act
        var node = new RedshiftSourceNode<TestRow>(
            "Host=localhost;Database=test",
            "SELECT * FROM test",
            row => new TestRow { Id = 42, Name = "Custom" });

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConfiguration_ShouldNotThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            StreamResults = true,
            FetchSize = 5000,
        };

        // Act
        var node = new RedshiftSourceNode<TestRow>(
            "Host=localhost;Database=test",
            "SELECT * FROM test",
            configuration: config);

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithParameters_ShouldNotThrow()
    {
        // Arrange
        var parameters = new[]
        {
            new DatabaseParameter("id", 1),
        };

        // Act
        var node = new RedshiftSourceNode<TestRow>(
            "Host=localhost;Database=test",
            "SELECT * FROM test WHERE id = @id",
            parameters: parameters);

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithContinueOnError_ShouldNotThrow()
    {
        // Act
        var node = new RedshiftSourceNode<TestRow>(
            "Host=localhost;Database=test",
            "SELECT * FROM test",
            continueOnError: true);

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionPool_AndValidArguments_ShouldNotThrow()
    {
        // Arrange
        var mockPool = new MockRedshiftConnectionPool();

        // Act
        var node = new RedshiftSourceNode<TestRow>(
            mockPool,
            "SELECT * FROM test");

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionPool_AndConnectionName_ShouldNotThrow()
    {
        // Arrange
        var mockPool = new MockRedshiftConnectionPool();

        // Act
        var node = new RedshiftSourceNode<TestRow>(
            mockPool,
            "SELECT * FROM test",
            connectionName: "myConnection");

        // Assert
        node.Should().NotBeNull();
    }

    public class TestRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    /// <summary>
    ///     Mock connection pool for testing.
    /// </summary>
    private sealed class MockRedshiftConnectionPool : IRedshiftConnectionPool
    {
        public string? ConnectionString => "Host=localhost;Database=test";

        public Task<NpgsqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("Mock not implemented for unit tests.");
        }

        public Task<NpgsqlConnection> GetConnectionAsync(string name, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("Mock not implemented for unit tests.");
        }

        public Task<NpgsqlDataSource> GetDataSourceAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("Mock not implemented for unit tests.");
        }

        public Task<NpgsqlDataSource> GetDataSourceAsync(string name, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("Mock not implemented for unit tests.");
        }

        public bool HasNamedConnection(string name)
        {
            return false;
        }

        public IEnumerable<string> GetNamedConnectionNames()
        {
            return [];
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
