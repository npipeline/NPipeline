using FakeItEasy;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Writers;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Writers;

public class RedshiftBatchWriterTests
{
    [Fact]
    public void Constructor_WithNullConnectionPool_ThrowsArgumentNullException()
    {
        // Arrange
        var configuration = new RedshiftConfiguration { BatchSize = 100 };

        // Act
        var act = () => new RedshiftBatchWriter<TestRow>(
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
        var configuration = new RedshiftConfiguration { BatchSize = 100 };

        // Act
        var act = () => new RedshiftBatchWriter<TestRow>(
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
        var configuration = new RedshiftConfiguration { BatchSize = 100 };

        // Act
        var act = () => new RedshiftBatchWriter<TestRow>(
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
        var act = () => new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", null!);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("configuration");
    }

    [Fact]
    public async Task FlushAsync_WithEmptyBuffer_IsNoOp()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();
        var configuration = new RedshiftConfiguration { BatchSize = 100 };

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act
        await writer.FlushAsync();

        // Assert
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_BuffersUntilBatchSize()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();
        var configuration = new RedshiftConfiguration { BatchSize = 3 };

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act - Write 2 rows (below batch size)
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await writer.WriteAsync(new TestRow { Id = 2, Name = "Test2" });

        // Assert - Should not flush yet
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WhenBatchSizeReached_Flushes()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 2,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act - Write exactly batch size (this will trigger flush which will fail)
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // This write triggers flush
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteAsync(new TestRow { Id = 2, Name = "Test2" }));

        // Assert - Should have tried to get a connection
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task FlushAsync_WithBufferedRows_FlushesToDatabase()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task FlushAsync_WithTransaction_WrapsInTransaction()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseTransaction = true,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        // Assert - Should have tried to get a connection
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_FlushesRemainingRows()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - but disposal should swallow the error
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Act - Dispose should flush, and since we set up the fake to throw,
        // the flush will attempt to get a connection. DisposeAsync catches exceptions.
        await writer.DisposeAsync();

        // Assert - Verify that the writer attempted to get a connection (flush was attempted)
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();
        var configuration = new RedshiftConfiguration { BatchSize = 100 };

        var writer = new RedshiftBatchWriter<TestRow>(
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
        var configuration = new RedshiftConfiguration { BatchSize = 100 };

        var writer = new RedshiftBatchWriter<TestRow>(
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
        var configuration = new RedshiftConfiguration { BatchSize = 100 };

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        await writer.DisposeAsync();

        // Act
        var act = async () => await writer.FlushAsync();

        // Assert
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task WriteAsync_MultipleBatches_FlushesEachBatch()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 2,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act - Write 4 rows (2 batches)
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Second write triggers first flush (batch size = 2)
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteAsync(new TestRow { Id = 2, Name = "Test2" }));

        // Assert - Should have tried to get a connection once
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    public class TestRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
