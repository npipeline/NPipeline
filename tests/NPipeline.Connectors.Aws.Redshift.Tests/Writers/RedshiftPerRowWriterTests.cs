using FakeItEasy;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Writers;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Writers;

public class RedshiftPerRowWriterTests
{
    [Fact]
    public void Constructor_WithNullConnectionPool_ThrowsArgumentNullException()
    {
        // Arrange
        var configuration = new RedshiftConfiguration();

        // Act
        var act = () => new RedshiftPerRowWriter<TestRow>(
            null!, "public", "test_table", configuration);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("connectionPool");
    }

    [Fact]
    public void Constructor_WithNullSchema_ThrowsArgumentNullException()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();
        var configuration = new RedshiftConfiguration();

        // Act
        var act = () => new RedshiftPerRowWriter<TestRow>(
            connectionPool, null!, "test_table", configuration);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("schema");
    }

    [Fact]
    public void Constructor_WithNullTable_ThrowsArgumentNullException()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();
        var configuration = new RedshiftConfiguration();

        // Act
        var act = () => new RedshiftPerRowWriter<TestRow>(
            connectionPool, "public", null!, configuration);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("table");
    }

    [Fact]
    public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        // Act
        var act = () => new RedshiftPerRowWriter<TestRow>(
            connectionPool, "public", "test_table", null!);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("configuration");
    }

    [Fact]
    public async Task WriteAsync_ExecutesInsert()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Use a connection string that will fail fast - we're just testing that the writer tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftPerRowWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        var row = new TestRow { Id = 1, Name = "Test" };

        // Act & Assert - The writer should attempt to get a connection
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.WriteAsync(row));

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task FlushAsync_WithEmptyBuffer_IsNoOp()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();
        var configuration = new RedshiftConfiguration();

        var writer = new RedshiftPerRowWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act
        await writer.FlushAsync();

        // Assert - Should not throw and should not get connection
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task DisposeAsync_WithNoPendingWrites_CompletesSuccessfully()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();
        var configuration = new RedshiftConfiguration();

        var writer = new RedshiftPerRowWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act
        await writer.DisposeAsync();

        // Assert - Should not throw
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();
        var configuration = new RedshiftConfiguration();

        var writer = new RedshiftPerRowWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act
        await writer.DisposeAsync();
        await writer.DisposeAsync();

        // Assert - Should not throw
    }

    [Fact]
    public async Task WriteAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();
        var configuration = new RedshiftConfiguration();

        var writer = new RedshiftPerRowWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        await writer.DisposeAsync();

        // Act
        var act = async () => await writer.WriteAsync(new TestRow { Id = 1, Name = "Test" });

        // Assert
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task FlushAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();
        var configuration = new RedshiftConfiguration();

        var writer = new RedshiftPerRowWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        await writer.DisposeAsync();

        // Act
        var act = async () => await writer.FlushAsync();

        // Assert
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    public class TestRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
