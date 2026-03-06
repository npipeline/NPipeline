using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Writers;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MySql.Tests.Writers;

/// <summary>
///     Tests for MySqlPerRowWriter.
///     Validates per-row write operations, parameter mapping, and error handling.
/// </summary>
public sealed class MySqlPerRowWriterTests
{
    #region FlushAsync Tests

    [Fact]
    public async Task FlushAsync_Always_ReturnsCompletedTask()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new MySqlConfiguration();

        var writer = new MySqlPerRowWriter<TestEntity>(
            connection,
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
        var configuration = new MySqlConfiguration();

        var writer = new MySqlPerRowWriter<TestEntity>(
            connection,
            "test_table",
            null,
            configuration);

        // Act
        await writer.DisposeAsync();

        // Assert - connection should not be disposed (owned by sink node)
        A.CallTo(() => connection.DisposeAsync()).MustNotHaveHappened();
    }

    #endregion

    #region Test Models

    private sealed class TestEntity
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
        var configuration = new MySqlConfiguration();

        // Act
        var writer = new MySqlPerRowWriter<TestEntity>(
            connection,
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
        var configuration = new MySqlConfiguration();

        // Act
        var action = () => new MySqlPerRowWriter<TestEntity>(
            null!,
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
        var action = () => new MySqlPerRowWriter<TestEntity>(
            connection,
            "test_table",
            null,
            null!);

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullTableName_ThrowsArgumentNullException()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new MySqlConfiguration();

        // Act
        var action = () => new MySqlPerRowWriter<TestEntity>(
            connection,
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
        var configuration = new MySqlConfiguration();

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(1);

        var writer = new MySqlPerRowWriter<TestEntity>(
            connection,
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
        var configuration = new MySqlConfiguration();

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(1);

        var mapperCalled = false;

        Func<TestEntity, IEnumerable<DatabaseParameter>> mapper = entity =>
        {
            mapperCalled = true;

            return
            [
                new DatabaseParameter("@Id", entity.Id),
                new DatabaseParameter("@Name", entity.Name),
                new DatabaseParameter("@CreatedAt", entity.CreatedAt),
                new DatabaseParameter("@IsActive", entity.IsActive),
            ];
        };

        var writer = new MySqlPerRowWriter<TestEntity>(
            connection,
            "test_table",
            mapper,
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
        _ = mapperCalled.Should().BeTrue();

        A.CallTo(() => command.AddParameter(A<string>._, A<object?>._))
            .MustHaveHappenedANumberOfTimesMatching(n => n == 4);
    }

    #endregion
}
