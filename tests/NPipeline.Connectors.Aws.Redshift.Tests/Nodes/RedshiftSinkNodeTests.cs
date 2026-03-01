using Npgsql;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Nodes;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Nodes;

public class RedshiftSinkNodeTests
{
    [Fact]
    public void Constructor_WithNullConnectionString_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSinkNode<TestRow>(
            (string)null!,
            "test_table");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithEmptyConnectionString_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSinkNode<TestRow>(
            "",
            "test_table");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithWhitespaceConnectionString_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSinkNode<TestRow>(
            "   ",
            "test_table");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullTableName_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSinkNode<TestRow>(
            "Host=localhost;Database=test",
            null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithEmptyTableName_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSinkNode<TestRow>(
            "Host=localhost;Database=test",
            "");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithWhitespaceTableName_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSinkNode<TestRow>(
            "Host=localhost;Database=test",
            "   ");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithValidArguments_ShouldNotThrow()
    {
        // Act
        var node = new RedshiftSinkNode<TestRow>(
            "Host=localhost;Database=test",
            "test_table");

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullConnectionPool_ShouldThrow()
    {
        // Act
        var act = () => new RedshiftSinkNode<TestRow>(
            (IRedshiftConnectionPool)null!,
            "test_table");

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithConnectionPool_AndNullTableName_ShouldThrow()
    {
        // Arrange
        var mockPool = new MockRedshiftConnectionPool();

        // Act
        var act = () => new RedshiftSinkNode<TestRow>(
            mockPool,
            null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_PerRowStrategy_ShouldNotThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.PerRow,
        };

        // Act
        var node = new RedshiftSinkNode<TestRow>(
            "Host=localhost;Database=test",
            "test_table",
            RedshiftWriteStrategy.PerRow,
            config);

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_BatchStrategy_ShouldNotThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.Batch,
        };

        // Act
        var node = new RedshiftSinkNode<TestRow>(
            "Host=localhost;Database=test",
            "test_table",
            RedshiftWriteStrategy.Batch,
            config);

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_CopyFromS3Strategy_WithoutBucket_ShouldThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            IamRoleArn = "arn:aws:iam::123456789012:role/TestRole",

            // Missing S3BucketName
        };

        // Act
        var act = () => new RedshiftSinkNode<TestRow>(
            "Host=localhost;Database=test",
            "test_table",
            RedshiftWriteStrategy.CopyFromS3,
            config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*S3BucketName*");
    }

    [Fact]
    public void Constructor_CopyFromS3Strategy_WithoutIamRole_ShouldThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            S3BucketName = "my-bucket",

            // Missing IamRoleArn
        };

        // Act
        var act = () => new RedshiftSinkNode<TestRow>(
            "Host=localhost;Database=test",
            "test_table",
            RedshiftWriteStrategy.CopyFromS3,
            config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*IamRoleArn*");
    }

    [Fact]
    public void Constructor_WithCustomSchema_ShouldNotThrow()
    {
        // Act
        var node = new RedshiftSinkNode<TestRow>(
            "Host=localhost;Database=test",
            "test_table",
            schema: "custom_schema");

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConfiguration_ShouldNotThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            BatchSize = 5000,
            UseTransaction = true,
        };

        // Act
        var node = new RedshiftSinkNode<TestRow>(
            "Host=localhost;Database=test",
            "test_table",
            configuration: config);

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionPool_AndValidArguments_ShouldNotThrow()
    {
        // Arrange
        var mockPool = new MockRedshiftConnectionPool();

        // Act
        var node = new RedshiftSinkNode<TestRow>(
            mockPool,
            "test_table");

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConnectionPool_AndConnectionName_ShouldNotThrow()
    {
        // Arrange
        var mockPool = new MockRedshiftConnectionPool();

        // Act
        var node = new RedshiftSinkNode<TestRow>(
            mockPool,
            "test_table",
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
