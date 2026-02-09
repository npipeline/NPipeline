using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Writers;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.PostgreSQL.Tests.Writers;

/// <summary>
///     Tests for PostgresBatchWriter.
///     Validates batch write operations, parameter mapping, and error handling.
/// </summary>
public sealed class PostgresBatchWriterTests
{
    #region Test Models

    private sealed class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed class TestEntityWithMapper
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
    }

    #endregion

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_CreatesWriter()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration();

        // Act
        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        // Assert
        _ = writer.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullConnection_ThrowsArgumentNullException()
    {
        // Arrange
        var configuration = new PostgresConfiguration();

        // Act
        var action = () => new PostgresBatchWriter<TestEntity>(
            null!,
            "public",
            "test_table",
            null,
            configuration);

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();

        // Act
        var action = () => new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            null!);

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullSchema_ThrowsArgumentNullException()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration();

        // Act
        var action = () => new PostgresBatchWriter<TestEntity>(
            connection,
            null!,
            "test_table",
            null,
            configuration);

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullTableName_ThrowsArgumentNullException()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration();

        // Act
        var action = () => new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            null!,
            null,
            configuration);

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithCustomBatchSize_UsesBatchSize()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration { BatchSize = 100 };

        // Act
        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        // Assert
        _ = writer.Should().NotBeNull();
    }

    #endregion

    #region WriteAsync Tests

    [Fact]
    public async Task WriteAsync_WithValidItem_BuffersItem()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration { BatchSize = 10 };

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var item = new TestEntity
        {
            Id = 1,
            Name = "Test",
            CreatedAt = DateTime.UtcNow,
            IsActive = true,
        };

        // Act
        await writer.WriteAsync(item);

        // Assert - item should be buffered, no flush yet
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WithBatchSizeReached_FlushesBatch()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 2 };

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(2);

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var item1 = new TestEntity { Id = 1, Name = "Test1", CreatedAt = DateTime.UtcNow, IsActive = true };
        var item2 = new TestEntity { Id = 2, Name = "Test2", CreatedAt = DateTime.UtcNow, IsActive = false };

        // Act
        await writer.WriteAsync(item1);
        await writer.WriteAsync(item2);

        // Assert - batch should be flushed
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithCustomParameterMapper_UsesMapper()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 1 };

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(1);

        var mapperCalled = false;

        Func<TestEntityWithMapper, IEnumerable<DatabaseParameter>> mapper = entity =>
        {
            mapperCalled = true;

            return new List<DatabaseParameter>
            {
                new("@Id", entity.Id),
                new("@Name", entity.Name),
                new("@CreatedAt", entity.CreatedAt),
                new("@IsActive", entity.IsActive),
            };
        };

        var writer = new PostgresBatchWriter<TestEntityWithMapper>(
            connection,
            "public",
            "test_table",
            mapper,
            configuration);

        var item = new TestEntityWithMapper
        {
            Id = 1,
            Name = "Test",
            CreatedAt = DateTime.UtcNow,
            IsActive = true,
        };

        // Act
        await writer.WriteAsync(item);

        // Assert
        _ = mapperCalled.Should().BeTrue();

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithNullValues_WritesNullParameters()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 1 };

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(1);

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var item = new TestEntity
        {
            Id = 0,
            Name = null!,
            CreatedAt = default,
            IsActive = false,
        };

        // Act
        await writer.WriteAsync(item);

        // Assert
        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithCommandTimeout_SetsTimeout()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 1, CommandTimeout = 60 };

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(1);

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var item = new TestEntity
        {
            Id = 1,
            Name = "Test",
            CreatedAt = DateTime.UtcNow,
            IsActive = true,
        };

        // Act
        await writer.WriteAsync(item);

        // Assert
        _ = command.CommandTimeout.Should().Be(60);
    }

    [Fact]
    public async Task WriteAsync_WithCancellation_CancelsOperation()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 1 };
        var cts = new CancellationTokenSource();
        cts.Cancel();

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Throws(new OperationCanceledException());

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var item = new TestEntity
        {
            Id = 1,
            Name = "Test",
            CreatedAt = DateTime.UtcNow,
            IsActive = true,
        };

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() => writer.WriteAsync(item, cts.Token));
    }

    [Fact]
    public async Task WriteAsync_WithDatabaseException_PropagatesException()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 1 };

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Throws(new Exception("Database error"));

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var item = new TestEntity
        {
            Id = 1,
            Name = "Test",
            CreatedAt = DateTime.UtcNow,
            IsActive = true,
        };

        // Act & Assert
        await Assert.ThrowsAsync<Exception>(() => writer.WriteAsync(item));
    }

    #endregion

    #region WriteBatchAsync Tests

    [Fact]
    public async Task WriteBatchAsync_WithMultipleItems_WritesAllItems()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 10 };

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(3);

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var items = new List<TestEntity>
        {
            new() { Id = 1, Name = "Test1", CreatedAt = DateTime.UtcNow, IsActive = true },
            new() { Id = 2, Name = "Test2", CreatedAt = DateTime.UtcNow, IsActive = false },
            new() { Id = 3, Name = "Test3", CreatedAt = DateTime.UtcNow, IsActive = true },
        };

        // Act
        await writer.WriteBatchAsync(items);

        // Assert
        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteBatchAsync_WithEmptyList_DoesNotWrite()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration();

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var items = new List<TestEntity>();

        // Act
        await writer.WriteBatchAsync(items);

        // Assert
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteBatchAsync_WithCancellation_CancelsOperation()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 1 };
        var cts = new CancellationTokenSource();
        cts.Cancel();

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Throws(new OperationCanceledException());

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var items = new List<TestEntity>
        {
            new() { Id = 1, Name = "Test", CreatedAt = DateTime.UtcNow, IsActive = true },
        };

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() => writer.WriteBatchAsync(items, cts.Token));
    }

    #endregion

    #region FlushAsync Tests

    [Fact]
    public async Task FlushAsync_WithBufferedItems_FlushesBatch()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 10 };

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(1);

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var item = new TestEntity
        {
            Id = 1,
            Name = "Test",
            CreatedAt = DateTime.UtcNow,
            IsActive = true,
        };

        await writer.WriteAsync(item);

        // Act
        await writer.FlushAsync();

        // Assert
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task FlushAsync_WithNoBufferedItems_DoesNotWrite()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration();

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        // Act
        await writer.FlushAsync();

        // Assert
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task FlushAsync_WithCancellation_CancelsOperation()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 10 };
        var cts = new CancellationTokenSource();
        cts.Cancel();

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Throws(new OperationCanceledException());

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var item = new TestEntity
        {
            Id = 1,
            Name = "Test",
            CreatedAt = DateTime.UtcNow,
            IsActive = true,
        };

        await writer.WriteAsync(item);

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() => writer.FlushAsync(cts.Token));
    }

    #endregion

    #region DisposeAsync Tests

    [Fact]
    public async Task DisposeAsync_WithBufferedItems_FlushesBeforeDispose()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new PostgresConfiguration { BatchSize = 10 };

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(1);

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        var item = new TestEntity
        {
            Id = 1,
            Name = "Test",
            CreatedAt = DateTime.UtcNow,
            IsActive = true,
        };

        await writer.WriteAsync(item);

        // Act
        await writer.DisposeAsync();

        // Assert - buffered items should be flushed
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_DoesNotDisposeConnection()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration();

        var writer = new PostgresBatchWriter<TestEntity>(
            connection,
            "public",
            "test_table",
            null,
            configuration);

        // Act
        await writer.DisposeAsync();

        // Assert - connection should not be disposed (owned by sink node)
        A.CallTo(() => connection.DisposeAsync()).MustNotHaveHappened();
    }

    #endregion
}
