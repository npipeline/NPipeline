using System.Text.Json;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.SqlServer.Checkpointing;

namespace NPipeline.Connectors.SqlServer.Tests.Checkpointing;

/// <summary>
///     Tests for SqlServerCdcCheckpointHandler.
///     Validates LSN (Log Sequence Number) position tracking for SQL Server CDC.
/// </summary>
public sealed class SqlServerCdcCheckpointHandlerTests
{
    private const string TestPipelineId = "test-pipeline";
    private const string TestNodeId = "test-node";
    private const string TestCaptureInstance = "dbo_TestTable";

    #region Properties Tests

    [Fact]
    public void CaptureInstance_ReturnsConfiguredCaptureInstance()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, "my_capture_instance");

        // Act & Assert
        _ = handler.CaptureInstance.Should().Be("my_capture_instance");
    }

    #endregion

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_CreatesHandler()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        // Assert
        _ = handler.Should().NotBeNull();
        _ = handler.CaptureInstance.Should().Be(TestCaptureInstance);
    }

    [Fact]
    public void Constructor_WithNullCheckpointManager_ThrowsArgumentNullException()
    {
        // Act
        var action = () => new SqlServerCdcCheckpointHandler(null!, TestCaptureInstance);

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullCaptureInstance_ThrowsArgumentException()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var action = () => new SqlServerCdcCheckpointHandler(manager, null!);

        // Assert
        _ = action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithEmptyCaptureInstance_ThrowsArgumentException()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var action = () => new SqlServerCdcCheckpointHandler(manager, string.Empty);

        // Assert
        _ = action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithWhitespaceCaptureInstance_ThrowsArgumentException()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var action = () => new SqlServerCdcCheckpointHandler(manager, "   ");

        // Assert
        _ = action.Should().Throw<ArgumentException>();
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
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

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

        var cdcPosition = new SqlServerCdcPosition
        {
            StartLsnHex = "0x0000123456789ABC",
            SeqValHex = "0x0000123456789ABD",
            Operation = 2,
            ChangeCount = 100,
        };

        var checkpoint = new Checkpoint(
            SerializePosition(cdcPosition),
            DateTimeOffset.UtcNow,
            new Dictionary<string, string> { ["capture_instance"] = TestCaptureInstance });

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        // Act
        var position = await handler.LoadPositionAsync();

        // Assert
        _ = position.Should().NotBeNull();
        _ = position!.StartLsnHex.Should().Be("0x0000123456789ABC");
        _ = position.SeqValHex.Should().Be("0x0000123456789ABD");
        _ = position.Operation.Should().Be(2);
        _ = position.ChangeCount.Should().Be(100);
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
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        // Act
        var position = await handler.LoadPositionAsync();

        // Assert
        _ = position.Should().BeNull();
    }

    [Fact]
    public async Task LoadPositionAsync_WithEmptyValue_ReturnsNull()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var checkpoint = new Checkpoint("", DateTimeOffset.UtcNow);

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

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
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        var position = new SqlServerCdcPosition
        {
            StartLsnHex = "0x0000123456789ABC",
            Operation = 2,
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
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        var position = new SqlServerCdcPosition
        {
            StartLsnHex = "0x0000123456789ABC",
        };

        // Act
        await handler.UpdatePositionAsync(position, true);

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
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        // Act
        var action = async () => await handler.UpdatePositionAsync(null!);

        // Assert
        await action.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task UpdatePositionAsync_WithChangeCount_StoresInMetadata()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        Checkpoint? savedCheckpoint = null;

        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .Invokes((string _, string _, Checkpoint cp, CancellationToken _) => savedCheckpoint = cp);

        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        var position = new SqlServerCdcPosition
        {
            StartLsnHex = "0x0000123456789ABC",
            ChangeCount = 500,
        };

        // Act
        await handler.UpdatePositionAsync(position, true);

        // Assert
        _ = savedCheckpoint.Should().NotBeNull();
        _ = savedCheckpoint!.Metadata.Should().ContainKey("change_count");
        savedCheckpoint.Metadata!["change_count"].Should().Be("500");
    }

    #endregion

    #region UpdateFromLsnAsync Tests

    [Fact]
    public async Task UpdateFromLsnAsync_WithValidLsn_UpdatesPosition()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        var lsnBytes = Convert.FromHexString("0000123456789ABC");

        // Act
        await handler.UpdateFromLsnAsync(lsnBytes, forceSave: true);

        // Assert
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task UpdateFromLsnAsync_WithSeqVal_StoresSeqVal()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        Checkpoint? savedCheckpoint = null;

        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .Invokes((string _, string _, Checkpoint cp, CancellationToken _) => savedCheckpoint = cp);

        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        var startLsn = Convert.FromHexString("0000123456789ABC");
        var seqVal = Convert.FromHexString("0000123456789ABD");

        // Act
        await handler.UpdateFromLsnAsync(startLsn, seqVal, forceSave: true);

        // Assert
        _ = savedCheckpoint.Should().NotBeNull();

        // Verify the position can be deserialized with the seq val
        var deserializedPosition = DeserializePosition(savedCheckpoint!.Value);
        _ = deserializedPosition.Should().NotBeNull();
        _ = deserializedPosition!.SeqVal.Should().NotBeNull();
    }

    [Fact]
    public async Task UpdateFromLsnAsync_WithOperation_StoresOperation()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        Checkpoint? savedCheckpoint = null;

        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .Invokes((string _, string _, Checkpoint cp, CancellationToken _) => savedCheckpoint = cp);

        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        var lsnBytes = Convert.FromHexString("0000123456789ABC");

        // Act
        await handler.UpdateFromLsnAsync(lsnBytes, operation: 2, forceSave: true);

        // Assert
        _ = savedCheckpoint.Should().NotBeNull();
        var deserializedPosition = DeserializePosition(savedCheckpoint!.Value);
        _ = deserializedPosition.Should().NotBeNull();
        _ = deserializedPosition!.Operation.Should().Be(2);
    }

    #endregion

    #region UpdateFromLsnHexAsync Tests

    [Fact]
    public async Task UpdateFromLsnHexAsync_WithValidHexLsn_UpdatesPosition()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        // Act
        await handler.UpdateFromLsnHexAsync("0x0000123456789ABC", forceSave: true);

        // Assert
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task UpdateFromLsnHexAsync_WithSeqValHex_StoresSeqValHex()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        Checkpoint? savedCheckpoint = null;

        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .Invokes((string _, string _, Checkpoint cp, CancellationToken _) => savedCheckpoint = cp);

        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        // Act
        await handler.UpdateFromLsnHexAsync("0x0000123456789ABC", "0x0000123456789ABD", forceSave: true);

        // Assert
        _ = savedCheckpoint.Should().NotBeNull();
        var deserializedPosition = DeserializePosition(savedCheckpoint!.Value);
        _ = deserializedPosition.Should().NotBeNull();
        _ = deserializedPosition!.SeqValHex.Should().Be("0x0000123456789ABD");
    }

    #endregion

    #region SaveAsync and ClearAsync Tests

    [Fact]
    public async Task SaveAsync_WhenCalled_SavesCheckpoint()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        var position = new SqlServerCdcPosition { StartLsnHex = "0x0000123456789ABC" };
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
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        var position = new SqlServerCdcPosition { StartLsnHex = "0x0000123456789ABC" };
        await handler.UpdatePositionAsync(position);

        // Act
        await handler.ClearAsync();

        // Assert
        A.CallTo(() => storage.DeleteAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    #endregion

    #region GetLsnRangeAsync Tests

    [Fact]
    public async Task GetLsnRangeAsync_WhenNoCheckpoint_ReturnsNullFromLsn()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns((Checkpoint?)null);

        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        // Act
        var (fromLsn, toLsn) = await handler.GetLsnRangeAsync();

        // Assert
        _ = fromLsn.Should().BeNull();
        _ = toLsn.Should().Be("sys.fn_cdc_get_max_lsn()");
    }

    [Fact]
    public async Task GetLsnRangeAsync_WhenCheckpointExists_ReturnsFromLsn()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();

        var cdcPosition = new SqlServerCdcPosition
        {
            StartLsnHex = "0x0000123456789ABC",
        };

        var checkpoint = new Checkpoint(
            SerializePosition(cdcPosition),
            DateTimeOffset.UtcNow);

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        var handler = new SqlServerCdcCheckpointHandler(manager, TestCaptureInstance);

        // Act
        var (fromLsn, toLsn) = await handler.GetLsnRangeAsync();

        // Assert
        _ = fromLsn.Should().Be("0x0000123456789ABC");
        _ = toLsn.Should().Be("sys.fn_cdc_get_max_lsn()");
    }

    #endregion

    #region SqlServerCdcPosition Tests

    [Fact]
    public void SqlServerCdcPosition_FromLsnHex_CreatesPosition()
    {
        // Act
        var position = SqlServerCdcPosition.FromLsnHex("0x0000123456789ABC");

        // Assert
        _ = position.StartLsnHex.Should().Be("0x0000123456789ABC");
    }

    [Fact]
    public void SqlServerCdcPosition_FromLsn_CreatesPositionWithBothFormats()
    {
        // Arrange
        var lsnBytes = Convert.FromHexString("0000123456789ABC");

        // Act
        var position = SqlServerCdcPosition.FromLsn(lsnBytes);

        // Assert
        _ = position.StartLsn.Should().NotBeNull();
        _ = position.StartLsnHex.Should().Be("0x0000123456789ABC");
    }

    [Fact]
    public void ParseLsnAsBytes_WithBase64Lsn_ReturnsByteArray()
    {
        // Arrange
        var originalBytes = Convert.FromHexString("0000123456789ABC");
        var position = SqlServerCdcPosition.FromLsn(originalBytes);

        // Act
        var result = position.ParseLsnAsBytes();

        // Assert
        _ = result.Should().NotBeNull();
        _ = result.Should().Equal(originalBytes);
    }

    [Fact]
    public void ParseLsnAsBytes_WithHexLsn_ReturnsByteArray()
    {
        // Arrange
        var position = new SqlServerCdcPosition { StartLsnHex = "0x0000123456789ABC" };

        // Act
        var result = position.ParseLsnAsBytes();

        // Assert
        _ = result.Should().NotBeNull();
        _ = result.Should().Equal(Convert.FromHexString("0000123456789ABC"));
    }

    [Fact]
    public void ParseLsnAsBytes_WithHexLsnWithoutPrefix_ReturnsByteArray()
    {
        // Arrange
        var position = new SqlServerCdcPosition { StartLsnHex = "0000123456789ABC" };

        // Act
        var result = position.ParseLsnAsBytes();

        // Assert
        _ = result.Should().NotBeNull();
        _ = result.Should().Equal(Convert.FromHexString("0000123456789ABC"));
    }

    [Fact]
    public void ParseLsnAsBytes_WithEmptyLsn_ReturnsNull()
    {
        // Arrange
        var position = new SqlServerCdcPosition { StartLsnHex = "" };

        // Act
        var result = position.ParseLsnAsBytes();

        // Assert
        _ = result.Should().BeNull();
    }

    [Fact]
    public void SqlServerCdcPosition_WithAllProperties_PreservesAllValues()
    {
        // Arrange & Act
        var position = new SqlServerCdcPosition
        {
            StartLsn = "AAAAAASdfpq8",
            StartLsnHex = "0x0000123456789ABC",
            SeqVal = "AAAAAASdfpq9",
            SeqValHex = "0x0000123456789ABD",
            Operation = 2,
            ChangeCount = 1000,
            LastChangeTimestamp = new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.Zero),
            TransactionId = Guid.Parse("12345678-1234-1234-1234-123456789ABC"),
        };

        // Assert
        _ = position.StartLsn.Should().Be("AAAAAASdfpq8");
        _ = position.StartLsnHex.Should().Be("0x0000123456789ABC");
        _ = position.SeqVal.Should().Be("AAAAAASdfpq9");
        _ = position.SeqValHex.Should().Be("0x0000123456789ABD");
        _ = position.Operation.Should().Be(2);
        _ = position.ChangeCount.Should().Be(1000);
        _ = position.LastChangeTimestamp.Should().Be(new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.Zero));
        _ = position.TransactionId.Should().Be(Guid.Parse("12345678-1234-1234-1234-123456789ABC"));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void SqlServerCdcPosition_WithOperationTypes_StoresCorrectly(int operation)
    {
        // Arrange & Act
        var position = new SqlServerCdcPosition
        {
            Operation = operation,
        };

        // Assert
        _ = position.Operation.Should().Be(operation);
    }

    #endregion

    #region Serialization Tests

    [Fact]
    public void Serialization_RoundTrip_PreservesData()
    {
        // Arrange
        var original = new SqlServerCdcPosition
        {
            StartLsnHex = "0x0000123456789ABC",
            SeqValHex = "0x0000123456789ABD",
            Operation = 2,
            ChangeCount = 500,
        };

        // Act
        var serialized = SerializePosition(original);
        var deserialized = DeserializePosition(serialized);

        // Assert
        _ = deserialized.Should().NotBeNull();
        _ = deserialized!.StartLsnHex.Should().Be(original.StartLsnHex);
        _ = deserialized.SeqValHex.Should().Be(original.SeqValHex);
        _ = deserialized.Operation.Should().Be(original.Operation);
        _ = deserialized.ChangeCount.Should().Be(original.ChangeCount);
    }

    [Fact]
    public void Serialization_WithNullOptionalFields_PreservesRequiredFields()
    {
        // Arrange
        var original = new SqlServerCdcPosition
        {
            StartLsnHex = "0x0000123456789ABC",
        };

        // Act
        var serialized = SerializePosition(original);
        var deserialized = DeserializePosition(serialized);

        // Assert
        _ = deserialized.Should().NotBeNull();
        _ = deserialized!.StartLsnHex.Should().Be(original.StartLsnHex);
        _ = deserialized.SeqValHex.Should().BeNull();
        _ = deserialized.Operation.Should().BeNull();
    }

    #endregion

    #region Helper Methods

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private CheckpointManager CreateCheckpointManager(ICheckpointStorage storage)
    {
        return new CheckpointManager(
            storage,
            TestPipelineId,
            TestNodeId,
            CheckpointStrategy.CDC);
    }

    private static string SerializePosition(SqlServerCdcPosition position)
    {
        return JsonSerializer.Serialize(position, JsonOptions);
    }

    private static SqlServerCdcPosition? DeserializePosition(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        try
        {
            return JsonSerializer.Deserialize<SqlServerCdcPosition>(value, JsonOptions);
        }
        catch
        {
            return null;
        }
    }

    #endregion
}
