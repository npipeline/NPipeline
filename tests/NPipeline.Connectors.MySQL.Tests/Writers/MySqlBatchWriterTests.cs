using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Writers;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.MySql.Tests.Writers;

/// <summary>
///     Tests for MySqlBatchWriter.
///     Validates batch write operations, upsert logic, and multi-row SQL generation.
/// </summary>
public sealed class MySqlBatchWriterTests
{
    #region DisposeAsync Tests

    [Fact]
    public async Task DisposeAsync_DoesNotDisposeConnection()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new MySqlConfiguration();

        var writer = new MySqlBatchWriter<TestEntity>(
            connection,
            "test_table",
            null,
            configuration);

        // Act
        await writer.DisposeAsync();

        // Assert
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
        var writer = new MySqlBatchWriter<TestEntity>(
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
        var action = () => new MySqlBatchWriter<TestEntity>(
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
        var action = () => new MySqlBatchWriter<TestEntity>(
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
        var action = () => new MySqlBatchWriter<TestEntity>(
            connection,
            null!,
            null,
            configuration);

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithUpsertEnabled_CreatesWriter()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();

        var configuration = new MySqlConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = ["Id"],
        };

        // Act
        var writer = new MySqlBatchWriter<TestEntity>(
            connection,
            "test_table",
            null,
            configuration);

        // Assert
        _ = writer.Should().NotBeNull();
    }

    #endregion

    #region FlushAsync Tests

    [Fact]
    public async Task FlushAsync_WithNoPendingItems_CompletesSuccessfully()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new MySqlConfiguration();

        var writer = new MySqlBatchWriter<TestEntity>(
            connection,
            "test_table",
            null,
            configuration);

        // Act & Assert
        await writer.FlushAsync();
    }

    [Fact]
    public async Task FlushAsync_WithPendingItems_WritesItems()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        var configuration = new MySqlConfiguration { BatchSize = 10 };

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(1);

        var writer = new MySqlBatchWriter<TestEntity>(
            connection,
            "test_table",
            null,
            configuration);

        var item = new TestEntity { Id = 1, Name = "Test" };
        await writer.WriteAsync(item);

        // Act
        await writer.FlushAsync();

        // Assert
        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    #endregion
}
