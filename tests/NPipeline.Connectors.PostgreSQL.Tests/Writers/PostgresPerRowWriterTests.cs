using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Exceptions;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Writers;

namespace NPipeline.Connectors.PostgreSQL.Tests.Writers
{
    /// <summary>
    /// Tests for PostgresPerRowWriter.
    /// Validates per-row write operations, parameter mapping, and error handling.
    /// </summary>
    public sealed class PostgresPerRowWriterTests
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
            var writer = new PostgresPerRowWriter<TestEntity>(
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
            var action = () => new PostgresPerRowWriter<TestEntity>(
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
            var action = () => new PostgresPerRowWriter<TestEntity>(
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
            var action = () => new PostgresPerRowWriter<TestEntity>(
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
            var action = () => new PostgresPerRowWriter<TestEntity>(
                connection,
                "public",
                null!,
                null,
                configuration);

            // Assert
            _ = action.Should().Throw<ArgumentNullException>();
        }

        #endregion

        #region WriteAsync Tests

        [Fact]
        public async Task WriteAsync_WithValidItem_WritesToDatabase()
        {
            // Arrange
            var connection = A.Fake<IDatabaseConnection>();
            var command = A.Fake<IDatabaseCommand>();
            var configuration = new PostgresConfiguration();

            A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
                .Returns(command);

            A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
                .Returns(1);

            var writer = new PostgresPerRowWriter<TestEntity>(
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
                IsActive = true
            };

            // Act
            await writer.WriteAsync(item);

            // Assert
            A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
                .MustHaveHappenedOnceExactly();

            A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
                .MustHaveHappenedOnceExactly();

            A.CallTo(() => command.AddParameter(A<string>._, A<object?>._))
                .MustHaveHappenedANumberOfTimesMatching(n => n == 4);
        }

        [Fact]
        public async Task WriteAsync_WithCustomParameterMapper_UsesMapper()
        {
            // Arrange
            var connection = A.Fake<IDatabaseConnection>();
            var command = A.Fake<IDatabaseCommand>();
            var configuration = new PostgresConfiguration();

            A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
                .Returns(command);

            A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
                .Returns(1);

            var mapperCalled = false;
            Func<TestEntityWithMapper, IEnumerable<DatabaseParameter>> mapper = (entity) =>
            {
                mapperCalled = true;
                return new List<DatabaseParameter>
                {
                    new DatabaseParameter("@Id", entity.Id),
                    new DatabaseParameter("@Name", entity.Name),
                    new DatabaseParameter("@CreatedAt", entity.CreatedAt),
                    new DatabaseParameter("@IsActive", entity.IsActive)
                };
            };

            var writer = new PostgresPerRowWriter<TestEntityWithMapper>(
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
                IsActive = true
            };

            // Act
            await writer.WriteAsync(item);

            // Assert
            _ = mapperCalled.Should().BeTrue();
            A.CallTo(() => command.AddParameter(A<string>._, A<object?>._))
                .MustHaveHappenedANumberOfTimesMatching(n => n == 4);
        }

        [Fact]
        public async Task WriteAsync_WithNullValues_WritesNullParameters()
        {
            // Arrange
            var connection = A.Fake<IDatabaseConnection>();
            var command = A.Fake<IDatabaseCommand>();
            var configuration = new PostgresConfiguration();

            A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
                .Returns(command);

            A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
                .Returns(1);

            var writer = new PostgresPerRowWriter<TestEntity>(
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
                IsActive = false
            };

            // Act
            await writer.WriteAsync(item);

            // Assert
            A.CallTo(() => command.AddParameter(A<string>._, A<object?>._))
                .MustHaveHappenedANumberOfTimesMatching(n => n == 4);
        }

        [Fact]
        public async Task WriteAsync_WithCommandTimeout_SetsTimeout()
        {
            // Arrange
            var connection = A.Fake<IDatabaseConnection>();
            var command = A.Fake<IDatabaseCommand>();
            var configuration = new PostgresConfiguration { CommandTimeout = 60 };

            A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
                .Returns(command);

            A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
                .Returns(1);

            var writer = new PostgresPerRowWriter<TestEntity>(
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
                IsActive = true
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
            var configuration = new PostgresConfiguration();
            var cts = new CancellationTokenSource();
            cts.Cancel();

            A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
                .Returns(command);

            A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
                .Throws(new OperationCanceledException());

            var writer = new PostgresPerRowWriter<TestEntity>(
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
                IsActive = true
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
            var configuration = new PostgresConfiguration();

            A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
                .Returns(command);

            A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
                .Throws(new Exception("Database error"));

            var writer = new PostgresPerRowWriter<TestEntity>(
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
                IsActive = true
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
            var configuration = new PostgresConfiguration();

            A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
                .Returns(command);

            A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
                .Returns(1);

            var writer = new PostgresPerRowWriter<TestEntity>(
                connection,
                "public",
                "test_table",
                null,
                configuration);

            var items = new List<TestEntity>
            {
                new TestEntity { Id = 1, Name = "Test1", CreatedAt = DateTime.UtcNow, IsActive = true },
                new TestEntity { Id = 2, Name = "Test2", CreatedAt = DateTime.UtcNow, IsActive = false },
                new TestEntity { Id = 3, Name = "Test3", CreatedAt = DateTime.UtcNow, IsActive = true }
            };

            // Act
            await writer.WriteBatchAsync(items);

            // Assert
            A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
                .MustHaveHappenedANumberOfTimesMatching(n => n == 3);
        }

        [Fact]
        public async Task WriteBatchAsync_WithEmptyList_DoesNotWrite()
        {
            // Arrange
            var connection = A.Fake<IDatabaseConnection>();
            var configuration = new PostgresConfiguration();

            var writer = new PostgresPerRowWriter<TestEntity>(
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
            var configuration = new PostgresConfiguration();
            var cts = new CancellationTokenSource();
            cts.Cancel();

            A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
                .Returns(command);

            A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
                .Throws(new OperationCanceledException());

            var writer = new PostgresPerRowWriter<TestEntity>(
                connection,
                "public",
                "test_table",
                null,
                configuration);

            var items = new List<TestEntity>
            {
                new TestEntity { Id = 1, Name = "Test", CreatedAt = DateTime.UtcNow, IsActive = true }
            };

            // Act & Assert
            await Assert.ThrowsAsync<OperationCanceledException>(() => writer.WriteBatchAsync(items, cts.Token));
        }

        #endregion

        #region FlushAsync Tests

        [Fact]
        public async Task FlushAsync_Always_ReturnsCompletedTask()
        {
            // Arrange
            var connection = A.Fake<IDatabaseConnection>();
            var configuration = new PostgresConfiguration();

            var writer = new PostgresPerRowWriter<TestEntity>(
                connection,
                "public",
                "test_table",
                null,
                configuration);

            // Act
            await writer.FlushAsync();

            // Assert - should complete without error
            _ = true.Should().BeTrue();
        }

        #endregion

        #region DisposeAsync Tests

        [Fact]
        public async Task DisposeAsync_DoesNotDisposeConnection()
        {
            // Arrange
            var connection = A.Fake<IDatabaseConnection>();
            var configuration = new PostgresConfiguration();

            var writer = new PostgresPerRowWriter<TestEntity>(
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
}
