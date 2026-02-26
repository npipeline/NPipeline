using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Checkpointing.Strategies;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.Postgres.Tests.Checkpointing;

/// <summary>
///     Tests for OffsetCheckpointHandler.
///     Validates numeric offset tracking and WHERE clause generation.
/// </summary>
public sealed class OffsetCheckpointHandlerTests
{
    private const string TestPipelineId = "test-pipeline";
    private const string TestNodeId = "test-node";
    private const string TestOffsetColumn = "id";

    #region Helper Methods

    private CheckpointManager CreateCheckpointManager(ICheckpointStorage storage)
    {
        return new CheckpointManager(
            storage,
            TestPipelineId,
            TestNodeId,
            CheckpointStrategy.Offset);
    }

    #endregion

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_CreatesHandler()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var offsetColumn = "sequence_id";

        // Act
        var handler = new OffsetCheckpointHandler(manager, offsetColumn);

        // Assert
        _ = handler.Should().NotBeNull();
        _ = handler.OffsetColumn.Should().Be("sequence_id");
    }

    [Fact]
    public void Constructor_WithNullCheckpointManager_ThrowsArgumentNullException()
    {
        // Act
        var action = () => new OffsetCheckpointHandler(null!, "id");

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullOffsetColumn_ThrowsArgumentNullException()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var action = () => new OffsetCheckpointHandler(manager, null!);

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    #endregion

    #region GetCurrentOffset Tests

    [Fact]
    public void GetCurrentOffset_WhenNoCheckpoint_ReturnsZero()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns((Checkpoint?)null);

        var manager = CreateCheckpointManager(storage);
        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        // Act
        var offset = handler.GetCurrentOffset();

        // Assert
        _ = offset.Should().Be(0);
    }

    [Fact]
    public async Task GetCurrentOffset_WhenCheckpointExists_ReturnsOffsetValue()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var checkpoint = Checkpoint.FromOffset(12345L);

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        await manager.LoadAsync(); // Load the checkpoint

        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        // Act
        var offset = handler.GetCurrentOffset();

        // Assert
        _ = offset.Should().Be(12345L);
    }

    [Fact]
    public async Task LoadOffsetAsync_WhenCheckpointExists_ReturnsOffsetValue()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var checkpoint = Checkpoint.FromOffset(99999L);

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        // Act
        var offset = await handler.LoadOffsetAsync();

        // Assert
        _ = offset.Should().Be(99999L);
    }

    [Fact]
    public async Task LoadOffsetAsync_WhenNoCheckpoint_ReturnsZero()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns((Checkpoint?)null);

        var manager = CreateCheckpointManager(storage);
        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        // Act
        var offset = await handler.LoadOffsetAsync();

        // Assert
        _ = offset.Should().Be(0);
    }

    #endregion

    #region UpdateOffsetAsync Tests

    [Fact]
    public async Task UpdateOffsetAsync_WithNewOffset_UpdatesCheckpoint()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        // Act
        await handler.UpdateOffsetAsync(500L);

        // Assert
        _ = handler.GetCurrentOffset().Should().Be(500L);
    }

    [Fact]
    public async Task UpdateOffsetAsync_WithForceSave_SavesImmediately()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        // Act
        await handler.UpdateOffsetAsync(500L, true);

        // Assert
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task UpdateOffsetAsync_WithLargeOffset_HandlesLargeValues()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        var largeOffset = long.MaxValue / 2; // Large but not max to avoid overflow issues

        // Act
        await handler.UpdateOffsetAsync(largeOffset);

        // Assert
        _ = handler.GetCurrentOffset().Should().Be(largeOffset);
    }

    [Fact]
    public async Task UpdateOffsetAsync_WithZeroOffset_AcceptsZero()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        // First set to non-zero
        await handler.UpdateOffsetAsync(100L);

        // Act - Update to zero
        await handler.UpdateOffsetAsync(0L);

        // Assert
        _ = handler.GetCurrentOffset().Should().Be(0L);
    }

    #endregion

    #region GenerateWhereClause Tests

    [Fact]
    public void GenerateWhereClause_WithZeroOffset_ReturnsEmptyClause()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        // Act
        var (whereClause, parameterValue) = handler.GenerateWhereClause();

        // Assert
        _ = whereClause.Should().BeEmpty();
        _ = parameterValue.Should().Be(0L);
    }

    [Fact]
    public async Task GenerateWhereClause_WithNonZeroOffset_ReturnsValidClause()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var checkpoint = Checkpoint.FromOffset(1000L);

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        await manager.LoadAsync();

        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        // Act
        var (whereClause, parameterValue) = handler.GenerateWhereClause();

        // Assert
        _ = whereClause.Should().Be("\"id\" > @offset");
        _ = parameterValue.Should().Be(1000L);
    }

    [Fact]
    public async Task GenerateWhereClause_WithCustomParameterName_UsesCustomName()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var checkpoint = Checkpoint.FromOffset(500L);

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        await manager.LoadAsync();

        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        // Act
        var (whereClause, parameterValue) = handler.GenerateWhereClause("@lastId");

        // Assert
        _ = whereClause.Should().Be("\"id\" > @lastId");
        _ = parameterValue.Should().Be(500L);
    }

    [Fact]
    public async Task GenerateWhereClause_WithCustomOffsetColumn_UsesCustomColumn()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var checkpoint = Checkpoint.FromOffset(750L);

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        await manager.LoadAsync();

        var handler = new OffsetCheckpointHandler(manager, "sequence_num");

        // Act
        var (whereClause, _) = handler.GenerateWhereClause();

        // Assert
        _ = whereClause.Should().Be("\"sequence_num\" > @offset");
    }

    #endregion

    #region SaveAsync and ClearAsync Tests

    [Fact]
    public async Task SaveAsync_WhenCalled_SavesCheckpoint()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        await handler.UpdateOffsetAsync(100L);

        // Act
        await handler.SaveAsync();

        // Assert
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task ClearAsync_WhenCalled_DeletesCheckpoint()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new OffsetCheckpointHandler(manager, TestOffsetColumn);

        await handler.UpdateOffsetAsync(100L);

        // Act
        await handler.ClearAsync();

        // Assert
        A.CallTo(() => storage.DeleteAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();

        _ = handler.GetCurrentOffset().Should().Be(0);
    }

    #endregion
}
