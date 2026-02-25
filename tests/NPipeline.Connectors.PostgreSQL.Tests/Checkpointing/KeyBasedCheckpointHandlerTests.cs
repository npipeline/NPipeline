using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Checkpointing.Strategies;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.PostgreSQL.Tests.Checkpointing;

/// <summary>
///     Tests for KeyBasedCheckpointHandler.
///     Validates composite key tracking and WHERE clause generation.
/// </summary>
public sealed class KeyBasedCheckpointHandlerTests
{
    private const string TestPipelineId = "test-pipeline";
    private const string TestNodeId = "test-node";

    #region KeyColumns Property Tests

    [Fact]
    public void KeyColumns_WhenSet_ReturnsConfiguredColumns()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var keyColumns = new[] { "region", "country", "city" };

        // Act
        var handler = new KeyBasedCheckpointHandler(manager, keyColumns);

        // Assert
        _ = handler.KeyColumns.Should().HaveCount(3);
        _ = handler.KeyColumns.Should().ContainInOrder("region", "country", "city");
    }

    #endregion

    #region Helper Methods

    private CheckpointManager CreateCheckpointManager(ICheckpointStorage storage)
    {
        return new CheckpointManager(
            storage,
            TestPipelineId,
            TestNodeId,
            CheckpointStrategy.KeyBased);
    }

    #endregion

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_CreatesHandler()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var keyColumns = new[] { "tenant_id", "user_id" };

        // Act
        var handler = new KeyBasedCheckpointHandler(manager, keyColumns);

        // Assert
        _ = handler.Should().NotBeNull();
        _ = handler.KeyColumns.Should().ContainInOrder("tenant_id", "user_id");
    }

    [Fact]
    public void Constructor_WithNullCheckpointManager_ThrowsArgumentNullException()
    {
        // Act
        var action = () => new KeyBasedCheckpointHandler(null!, "id");

        // Assert
        _ = action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullKeyColumns_ThrowsArgumentException()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var action = () => new KeyBasedCheckpointHandler(manager, null!);

        // Assert
        _ = action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithEmptyKeyColumns_ThrowsArgumentException()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var action = () => new KeyBasedCheckpointHandler(manager);

        // Assert
        _ = action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithSingleKeyColumn_AcceptsSingleColumn()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);

        // Act
        var handler = new KeyBasedCheckpointHandler(manager, "id");

        // Assert
        _ = handler.KeyColumns.Should().ContainSingle("id");
    }

    #endregion

    #region LoadKeyValuesAsync Tests

    [Fact]
    public async Task LoadKeyValuesAsync_WhenNoCheckpoint_ReturnsNull()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns((Checkpoint?)null);

        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "id");

        // Act
        var keyValues = await handler.LoadKeyValuesAsync();

        // Assert
        _ = keyValues.Should().BeNull();
    }

    [Fact]
    public async Task LoadKeyValuesAsync_WhenCheckpointExists_ReturnsDeserializedValues()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();

        // The handler expects JSON format in the Value property
        var jsonValue = """{"tenant_id":100,"user_id":"user-123"}""";
        var checkpoint = Checkpoint.Create(jsonValue);

        A.CallTo(() => storage.LoadAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .Returns(checkpoint);

        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "tenant_id", "user_id");

        // Act
        var result = await handler.LoadKeyValuesAsync();

        // Assert
        _ = result.Should().NotBeNull();
        _ = result!["tenant_id"].Should().Be(100L);
        _ = result["user_id"].Should().Be("user-123");
    }

    #endregion

    #region UpdateKeyValuesAsync Tests

    [Fact]
    public async Task UpdateKeyValuesAsync_WithValidValues_UpdatesCheckpoint()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "tenant_id", "user_id");

        var keyValues = new Dictionary<string, object?>
        {
            ["tenant_id"] = 50,
            ["user_id"] = "user-456",
        };

        // Act
        await handler.UpdateKeyValuesAsync(keyValues);

        // Assert
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .MustNotHaveHappened(); // Not forced, so no immediate save
    }

    [Fact]
    public async Task UpdateKeyValuesAsync_WithForceSave_SavesImmediately()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "id");

        var keyValues = new Dictionary<string, object?>
        {
            ["id"] = 999,
        };

        // Act
        await handler.UpdateKeyValuesAsync(keyValues, true);

        // Assert
        A.CallTo(() => storage.SaveAsync(TestPipelineId, TestNodeId, A<Checkpoint>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task UpdateKeyValuesAsync_WithStringValue_AcceptsString()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "code");

        var keyValues = new Dictionary<string, object?>
        {
            ["code"] = "ABC-123-XYZ",
        };

        // Act
        await handler.UpdateKeyValuesAsync(keyValues);

        // Assert - Should not throw
    }

    [Fact]
    public async Task UpdateKeyValuesAsync_WithNullValue_AcceptsNull()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "optional_key");

        var keyValues = new Dictionary<string, object?>
        {
            ["optional_key"] = null,
        };

        // Act
        await handler.UpdateKeyValuesAsync(keyValues);

        // Assert - Should not throw
    }

    #endregion

    #region GenerateWhereClause Tests

    [Fact]
    public void GenerateWhereClause_WithEmptyKeyValues_ReturnsEmptyClause()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "id");

        var keyValues = new Dictionary<string, object?>();

        // Act
        var (whereClause, parameters) = handler.GenerateWhereClause(keyValues);

        // Assert
        _ = whereClause.Should().BeEmpty();
        _ = parameters.Should().BeEmpty();
    }

    [Fact]
    public void GenerateWhereClause_WithNullKeyValues_ReturnsEmptyClause()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "id");

        // Act
        var (whereClause, parameters) = handler.GenerateWhereClause(null!);

        // Assert
        _ = whereClause.Should().BeEmpty();
        _ = parameters.Should().BeEmpty();
    }

    [Fact]
    public void GenerateWhereClause_WithSingleKey_GeneratesSimpleCondition()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "id");

        var keyValues = new Dictionary<string, object?>
        {
            ["id"] = 100,
        };

        // Act
        var (whereClause, parameters) = handler.GenerateWhereClause(keyValues);

        // Assert
        _ = whereClause.Should().Be("\"id\" > @key_0");
        _ = parameters.Should().ContainKey("@key_0");
        _ = parameters["@key_0"].Should().Be(100);
    }

    [Fact]
    public void GenerateWhereClause_WithCompositeKey_GeneratesAndConditions()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "tenant_id", "user_id");

        var keyValues = new Dictionary<string, object?>
        {
            ["tenant_id"] = 10,
            ["user_id"] = "user-001",
        };

        // Act
        var (whereClause, parameters) = handler.GenerateWhereClause(keyValues);

        // Assert
        _ = whereClause.Should().Contain("\"tenant_id\" > @key_0");
        _ = whereClause.Should().Contain("AND");
        _ = whereClause.Should().Contain("\"user_id\" > @key_1");
        _ = parameters.Should().HaveCount(2);
    }

    [Fact]
    public void GenerateWhereClause_WithCustomPrefix_UsesCustomPrefix()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "id");

        var keyValues = new Dictionary<string, object?>
        {
            ["id"] = 500,
        };

        // Act
        var (whereClause, parameters) = handler.GenerateWhereClause(keyValues, "@myParam");

        // Assert
        _ = whereClause.Should().Be("\"id\" > @myParam_0");
        _ = parameters.Should().ContainKey("@myParam_0");
    }

    #endregion

    #region GenerateRowValueWhereClause Tests

    [Fact]
    public void GenerateRowValueWhereClause_WithEmptyKeyValues_ReturnsEmptyClause()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "id");

        var keyValues = new Dictionary<string, object?>();

        // Act
        var (whereClause, parameters) = handler.GenerateRowValueWhereClause(keyValues);

        // Assert
        _ = whereClause.Should().BeEmpty();
        _ = parameters.Should().BeEmpty();
    }

    [Fact]
    public void GenerateRowValueWhereClause_WithCompositeKey_GeneratesRowValueComparison()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "tenant_id", "user_id");

        var keyValues = new Dictionary<string, object?>
        {
            ["tenant_id"] = 5,
            ["user_id"] = "abc",
        };

        // Act
        var (whereClause, parameters) = handler.GenerateRowValueWhereClause(keyValues);

        // Assert
        _ = whereClause.Should().Be("(\"tenant_id\", \"user_id\") > (@key_0, @key_1)");
        _ = parameters.Should().HaveCount(2);
        _ = parameters["@key_0"].Should().Be(5);
        _ = parameters["@key_1"].Should().Be("abc");
    }

    [Fact]
    public void GenerateRowValueWhereClause_WithMissingKey_UsesNullForMissing()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "tenant_id", "user_id");

        var keyValues = new Dictionary<string, object?>
        {
            ["tenant_id"] = 5,

            // user_id is missing
        };

        // Act
        var (whereClause, parameters) = handler.GenerateRowValueWhereClause(keyValues);

        // Assert
        _ = parameters["@key_1"].Should().BeNull();
    }

    [Fact]
    public void GenerateRowValueWhereClause_WithCustomPrefix_UsesCustomPrefix()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "a", "b");

        var keyValues = new Dictionary<string, object?>
        {
            ["a"] = 1,
            ["b"] = 2,
        };

        // Act
        var (whereClause, parameters) = handler.GenerateRowValueWhereClause(keyValues, "@p");

        // Assert
        _ = whereClause.Should().Contain("@p_0");
        _ = whereClause.Should().Contain("@p_1");
    }

    #endregion

    #region SaveAsync and ClearAsync Tests

    [Fact]
    public async Task SaveAsync_WhenCalled_SavesCheckpoint()
    {
        // Arrange
        var storage = A.Fake<ICheckpointStorage>();
        var manager = CreateCheckpointManager(storage);
        var handler = new KeyBasedCheckpointHandler(manager, "id");

        var keyValues = new Dictionary<string, object?>
        {
            ["id"] = 999,
        };

        await handler.UpdateKeyValuesAsync(keyValues);

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
        var handler = new KeyBasedCheckpointHandler(manager, "id");

        var keyValues = new Dictionary<string, object?>
        {
            ["id"] = 999,
        };

        await handler.UpdateKeyValuesAsync(keyValues);

        // Act
        await handler.ClearAsync();

        // Assert
        A.CallTo(() => storage.DeleteAsync(TestPipelineId, TestNodeId, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    #endregion
}
