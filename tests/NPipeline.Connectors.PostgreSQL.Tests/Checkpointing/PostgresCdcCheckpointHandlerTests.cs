using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.PostgreSQL.Checkpointing;

namespace NPipeline.Connectors.PostgreSQL.Tests.Checkpointing;

/// <summary>
///     Tests for PostgresCdcCheckpointHandler.
///     Validates WAL position tracking for PostgreSQL CDC.
/// </summary>
public sealed class PostgresCdcCheckpointHandlerTests
{
    private const string TestPipelineId = "test-pipeline";
    private const string TestNodeId = "test-node";
    private const string TestSlotName = "test_replication_slot";
    private const string TestPublicationName = "test_publication";

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_CreatesHandler()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName, TestPublicationName);

        // Assert
        _ = handler.Should().NotBeNull();
        _ = handler.SlotName.Should().Be(TestSlotName);
        _ = handler.PublicationName.Should().Be(TestPublicationName);
    }

    [Fact]
    public void Constructor_WithNullCheckpointManager_ThrowsArgumentNullException()
    {
        // Act
        var action = () => new PostgresCdcCheckpointHandler(null!, TestSlotName);

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullSlotName_ThrowsArgumentException()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var action = () => new PostgresCdcCheckpointHandler(manager, null!);

        // Assert
        _ = action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithEmptySlotName_ThrowsArgumentException()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var action = () => new PostgresCdcCheckpointHandler(manager, string.Empty);

        // Assert
        _ = action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithoutPublicationName_AcceptsNullPublication()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName, null);

        // Assert
        _ = handler.Should().NotBeNull();
        _ = handler.PublicationName.Should().BeNull();
    }

    #endregion

    #region Properties Tests

    [Fact]
    public void SlotName_ReturnsConfiguredSlotName()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, "my_slot");

        // Act & Assert
        _ = handler.SlotName.Should().Be("my_slot");
    }

    [Fact]
    public void PublicationName_WhenSet_ReturnsPublicationName()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName, "my_publication");

        // Act & Assert
        _ = handler.PublicationName.Should().Be("my_publication");
    }

    #endregion

    #region LoadPositionAsync Tests

    [Fact]
    public async Task LoadPositionAsync_WhenNoCheckpoint_ReturnsNull()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns((Checkpoint?)null);

        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        // Act
        var position = await handler.LoadPositionAsync();

        // Assert
        _ = position.Should().BeNull();
    }

    [Fact]
    public async Task LoadPositionAsync_WhenCheckpointExists_ReturnsPosition()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var cdcPosition = new PostgresCdcPosition
        {
            WalLsn = "0/16B6F48",
            TransactionId = "12345",
            TransactionCount = 100,
        };

        var checkpoint = new Checkpoint(
            SerializePosition(cdcPosition),
            DateTimeOffset.UtcNow,
            new Dictionary<string, string> { ["slot_name"] = TestSlotName });

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        // Act
        var position = await handler.LoadPositionAsync();

        // Assert
        _ = position.Should().NotBeNull();
        _ = position!.WalLsn.Should().Be("0/16B6F48");
        _ = position.TransactionId.Should().Be("12345");
        _ = position.TransactionCount.Should().Be(100);
    }

    [Fact]
    public async Task LoadPositionAsync_WithInvalidJson_ReturnsNull()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var checkpoint = new Checkpoint("invalid json {{{", DateTimeOffset.UtcNow);

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        // Act
        var position = await handler.LoadPositionAsync();

        // Assert
        _ = position.Should().BeNull();
    }

    #endregion

    #region UpdatePositionAsync Tests

    [Fact]
    public async Task UpdatePositionAsync_WithValidPosition_UpdatesCheckpoint()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        var position = new PostgresCdcPosition
        {
            WalLsn = "0/16B6F48",
            TransactionId = "12345",
        };

        // Act
        await handler.UpdatePositionAsync(position);

        // Assert - Verify save was called
        await handler.SaveAsync();
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task UpdatePositionAsync_WithForceSave_SavesImmediately()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        var position = new PostgresCdcPosition
        {
            WalLsn = "0/16B6F48",
        };

        // Act
        await handler.UpdatePositionAsync(position, forceSave: true);

        // Assert
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task UpdatePositionAsync_WithNullPosition_ThrowsArgumentNullException()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        // Act
        var action = async () => await handler.UpdatePositionAsync(null!);

        // Assert
        await action.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task UpdatePositionAsync_WithTransactionCount_StoresInMetadata()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        Checkpoint? savedCheckpoint = null;
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .Invokes((string _, string _, Checkpoint cp, CancellationToken _) => savedCheckpoint = cp);

        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        var position = new PostgresCdcPosition
        {
            WalLsn = "0/16B6F48",
            TransactionCount = 500,
        };

        // Act
        await handler.UpdatePositionAsync(position, forceSave: true);

        // Assert
        _ = savedCheckpoint.Should().NotBeNull();
        _ = savedCheckpoint!.Metadata.Should().ContainKey("transaction_count");
        savedCheckpoint.Metadata!["transaction_count"].Should().Be("500");
    }

    #endregion

    #region UpdateFromWalLsnAsync Tests

    [Fact]
    public async Task UpdateFromWalLsnAsync_WithValidLsn_UpdatesPosition()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        // Act
        await handler.UpdateFromWalLsnAsync("0/16B6F48", forceSave: true);

        // Assert
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task UpdateFromWalLsnAsync_WithTransactionId_StoresTransactionId()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        Checkpoint? savedCheckpoint = null;
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .Invokes((string _, string _, Checkpoint cp, CancellationToken _) => savedCheckpoint = cp);

        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        // Act
        await handler.UpdateFromWalLsnAsync("0/16B6F48", "tx-12345", forceSave: true);

        // Assert
        _ = savedCheckpoint.Should().NotBeNull();

        // Verify the position can be deserialized with the transaction ID
        var deserializedPosition = DeserializePosition(savedCheckpoint!.Value);
        _ = deserializedPosition.Should().NotBeNull();
        _ = deserializedPosition!.TransactionId.Should().Be("tx-12345");
    }

    #endregion

    #region SaveAsync and ClearAsync Tests

    [Fact]
    public async Task SaveAsync_WhenCalled_SavesCheckpoint()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        var position = new PostgresCdcPosition { WalLsn = "0/16B6F48" };
        await handler.UpdatePositionAsync(position);

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
        var handler = new PostgresCdcCheckpointHandler(manager, TestSlotName);

        var position = new PostgresCdcPosition { WalLsn = "0/16B6F48" };
        await handler.UpdatePositionAsync(position);

        // Act
        await handler.ClearAsync();

        // Assert
        A.CallTo(() => storage.DeleteAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    #endregion

    #region PostgresCdcPosition Tests

    [Fact]
    public void PostgresCdcPosition_FromWalLsn_CreatesPosition()
    {
        // Act
        var position = PostgresCdcPosition.FromWalLsn("0/16B6F48");

        // Assert
        _ = position.WalLsn.Should().Be("0/16B6F48");
    }

    [Fact]
    public void ParseWalLsnAsInt64_WithValidLsn_ReturnsNumericValue()
    {
        // Arrange
        var position = new PostgresCdcPosition { WalLsn = "0/16B6F48" };

        // Act
        var result = position.ParseWalLsnAsInt64();

        // Assert
        _ = result.Should().NotBeNull();
        // 0x16B6F48 = 23932744
        _ = result.Should().Be(0x16B6F48);
    }

    [Fact]
    public void ParseWalLsnAsInt64_WithHighAndLowParts_ReturnsCorrectValue()
    {
        // Arrange
        var position = new PostgresCdcPosition { WalLsn = "1/100" };

        // Act
        var result = position.ParseWalLsnAsInt64();

        // Assert
        _ = result.Should().NotBeNull();
        // High part: 1, Low part: 0x100 = 256
        // Result: (1 << 32) | 256 = 4294967296 + 256 = 4294967552
        _ = result.Should().Be((1UL << 32) | 0x100);
    }

    [Fact]
    public void ParseWalLsnAsInt64_WithEmptyLsn_ReturnsNull()
    {
        // Arrange
        var position = new PostgresCdcPosition { WalLsn = "" };

        // Act
        var result = position.ParseWalLsnAsInt64();

        // Assert
        _ = result.Should().BeNull();
    }

    [Fact]
    public void ParseWalLsnAsInt64_WithInvalidFormat_ReturnsNull()
    {
        // Arrange
        var position = new PostgresCdcPosition { WalLsn = "invalid_format" };

        // Act
        var result = position.ParseWalLsnAsInt64();

        // Assert
        _ = result.Should().BeNull();
    }

    [Fact]
    public void ParseWalLsnAsInt64_WithMissingSlash_ReturnsNull()
    {
        // Arrange
        var position = new PostgresCdcPosition { WalLsn = "016B6F48" };

        // Act
        var result = position.ParseWalLsnAsInt64();

        // Assert
        _ = result.Should().BeNull();
    }

    [Fact]
    public void PostgresCdcPosition_WithAllProperties_PreservesAllValues()
    {
        // Arrange & Act
        var position = new PostgresCdcPosition
        {
            WalLsn = "0/16B6F48",
            RestartLsn = "0/16B6F00",
            TransactionId = "tx-123",
            TransactionCount = 1000,
            LastChangeTimestamp = new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.Zero),
            IsCaughtUp = true,
        };

        // Assert
        _ = position.WalLsn.Should().Be("0/16B6F48");
        _ = position.RestartLsn.Should().Be("0/16B6F00");
        _ = position.TransactionId.Should().Be("tx-123");
        _ = position.TransactionCount.Should().Be(1000);
        _ = position.LastChangeTimestamp.Should().Be(new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.Zero));
        _ = position.IsCaughtUp.Should().BeTrue();
    }

    #endregion

    #region Helper Methods

    private static readonly System.Text.Json.JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase
    };

    private CheckpointManager CreateCheckpointManager(ICheckpointStorage storage)
    {
        return new CheckpointManager(
            storage,
            TestPipelineId,
            TestNodeId,
            CheckpointStrategy.CDC);
    }

    private static string SerializePosition(PostgresCdcPosition position)
    {
        return System.Text.Json.JsonSerializer.Serialize(position, JsonOptions);
    }

    private static PostgresCdcPosition? DeserializePosition(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        try
        {
            return System.Text.Json.JsonSerializer.Deserialize<PostgresCdcPosition>(value, JsonOptions);
        }
        catch
        {
            return null;
        }
    }

    #endregion
}
