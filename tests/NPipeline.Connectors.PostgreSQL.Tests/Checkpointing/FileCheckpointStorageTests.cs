using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using NPipeline.Connectors.Checkpointing;

namespace NPipeline.Connectors.PostgreSQL.Tests.Checkpointing;

/// <summary>
///     Tests for FileCheckpointStorage.
///     Validates file-based checkpoint persistence.
/// </summary>
public sealed class FileCheckpointStorageTests : IDisposable
{
    private readonly string _testDirectory;

    public FileCheckpointStorageTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"checkpoint_tests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
            Directory.Delete(_testDirectory, true);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidPath_CreatesStorage()
    {
        // Arrange & Act
        var storage = new FileCheckpointStorage(_testDirectory);

        // Assert
        _ = storage.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithEmptyPath_ThrowsArgumentException()
    {
        // Act
        var action = () => new FileCheckpointStorage(string.Empty);

        // Assert
        _ = action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithNullPath_ThrowsArgumentException()
    {
        // Act
        var action = () => new FileCheckpointStorage(null!);

        // Assert
        _ = action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithCreateDirectoryTrue_CreatesDirectory()
    {
        // Arrange
        var newDirectory = Path.Combine(_testDirectory, "new_subdir");

        // Act
        _ = new FileCheckpointStorage(newDirectory);

        // Assert
        Directory.Exists(newDirectory).Should().BeTrue();
    }

    [Fact]
    public void Constructor_WithCreateDirectoryFalse_DoesNotCreateDirectory()
    {
        // Arrange
        var nonExistentDir = Path.Combine(_testDirectory, "nonexistent");

        // Act - Constructor should not throw, but directory shouldn't be created
        var storage = new FileCheckpointStorage(nonExistentDir, false);

        // Assert - Storage is created but directory doesn't exist
        _ = storage.Should().NotBeNull();
        Directory.Exists(nonExistentDir).Should().BeFalse();
    }

    #endregion

    #region SaveAsync Tests

    [Fact]
    public async Task SaveAsync_WithValidCheckpoint_SavesToFile()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var checkpoint = Checkpoint.FromOffset(12345L);

        // Act
        await storage.SaveAsync("pipeline1", "node1", checkpoint);

        // Assert
        var filePath = GetCheckpointFilePath("pipeline1", "node1");
        File.Exists(filePath).Should().BeTrue();
    }

    [Fact]
    public async Task SaveAsync_WithNullCheckpoint_ThrowsArgumentNullException()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);

        // Act
        var action = async () => await storage.SaveAsync("pipeline1", "node1", null!);

        // Assert
        await action.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task SaveAsync_WithMetadata_SavesMetadata()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);

        var metadata = new Dictionary<string, string>
        {
            ["key1"] = "value1",
            ["key2"] = "value2",
        };

        var checkpoint = new Checkpoint("test_value", DateTimeOffset.UtcNow, metadata);

        // Act
        await storage.SaveAsync("pipeline1", "node1", checkpoint);

        // Assert
        var loaded = await storage.LoadAsync("pipeline1", "node1");
        _ = loaded.Should().NotBeNull();
        _ = loaded!.Metadata.Should().ContainKey("key1");
        _ = loaded.Metadata!["key1"].Should().Be("value1");
    }

    [Fact]
    public async Task SaveAsync_WithSpecialCharactersInIds_SanitizesPath()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var checkpoint = Checkpoint.FromOffset(1L);

        // Act - Pipeline and node IDs with special characters
        await storage.SaveAsync("pipeline/with/slashes", "node:with:colons", checkpoint);

        // Assert - Should save without throwing
        var exists = await storage.ExistsAsync("pipeline/with/slashes", "node:with:colons");
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task SaveAsync_WithLongIdentifier_HashesIdentifier()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var checkpoint = Checkpoint.FromOffset(1L);
        var longId = new string('a', 150); // Longer than 100 chars

        // Act
        await storage.SaveAsync(longId, "node1", checkpoint);

        // Assert - Should save without throwing
        var exists = await storage.ExistsAsync(longId, "node1");
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task SaveAsync_OverwritesExistingCheckpoint()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var checkpoint1 = Checkpoint.FromOffset(100L);
        var checkpoint2 = Checkpoint.FromOffset(200L);

        await storage.SaveAsync("pipeline1", "node1", checkpoint1);

        // Act
        await storage.SaveAsync("pipeline1", "node1", checkpoint2);

        // Assert
        var loaded = await storage.LoadAsync("pipeline1", "node1");
        _ = loaded!.GetAsOffset().Should().Be(200L);
    }

    #endregion

    #region LoadAsync Tests

    [Fact]
    public async Task LoadAsync_WhenNoCheckpoint_ReturnsNull()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);

        // Act
        var checkpoint = await storage.LoadAsync("nonexistent", "node");

        // Assert
        _ = checkpoint.Should().BeNull();
    }

    [Fact]
    public async Task LoadAsync_WhenCheckpointExists_ReturnsCheckpoint()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var savedCheckpoint = Checkpoint.FromOffset(99999L);
        await storage.SaveAsync("pipeline1", "node1", savedCheckpoint);

        // Act
        var loadedCheckpoint = await storage.LoadAsync("pipeline1", "node1");

        // Assert
        _ = loadedCheckpoint.Should().NotBeNull();
        _ = loadedCheckpoint!.GetAsOffset().Should().Be(99999L);
    }

    [Fact]
    public async Task LoadAsync_PreservesTimestamp()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var timestamp = new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.Zero);
        var checkpoint = new Checkpoint("value", timestamp);
        await storage.SaveAsync("pipeline1", "node1", checkpoint);

        // Act
        var loaded = await storage.LoadAsync("pipeline1", "node1");

        // Assert
        _ = loaded!.Timestamp.Should().Be(timestamp);
    }

    [Fact]
    public async Task LoadAsync_WithCorruptedFile_ThrowsJsonException()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var filePath = GetCheckpointFilePath("pipeline1", "node1");
        var directoryPath = Path.GetDirectoryName(filePath)!;
        Directory.CreateDirectory(directoryPath);

        // Write invalid JSON
        await File.WriteAllTextAsync(filePath, "not valid json {{{");

        // Act
        var action = async () => await storage.LoadAsync("pipeline1", "node1");

        // Assert - Should throw JsonException for corrupted file
        await action.Should().ThrowAsync<JsonException>();
    }

    #endregion

    #region DeleteAsync Tests

    [Fact]
    public async Task DeleteAsync_WhenCheckpointExists_DeletesFile()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var checkpoint = Checkpoint.FromOffset(1L);
        await storage.SaveAsync("pipeline1", "node1", checkpoint);

        // Act
        await storage.DeleteAsync("pipeline1", "node1");

        // Assert
        var exists = await storage.ExistsAsync("pipeline1", "node1");
        exists.Should().BeFalse();
    }

    [Fact]
    public async Task DeleteAsync_WhenNoCheckpoint_DoesNotThrow()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);

        // Act & Assert - Should not throw
        await storage.DeleteAsync("nonexistent", "node");
    }

    #endregion

    #region ExistsAsync Tests

    [Fact]
    public async Task ExistsAsync_WhenCheckpointExists_ReturnsTrue()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var checkpoint = Checkpoint.FromOffset(1L);
        await storage.SaveAsync("pipeline1", "node1", checkpoint);

        // Act
        var exists = await storage.ExistsAsync("pipeline1", "node1");

        // Assert
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_WhenNoCheckpoint_ReturnsFalse()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);

        // Act
        var exists = await storage.ExistsAsync("nonexistent", "node");

        // Assert
        exists.Should().BeFalse();
    }

    #endregion

    #region Concurrency Tests

    [Fact]
    public async Task SaveAsync_ConcurrentSaves_HandlesConcurrency()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var tasks = new List<Task>();

        // Act - Multiple concurrent saves to different pipelines
        for (var i = 0; i < 10; i++)
        {
            var index = i;

            tasks.Add(Task.Run(async () =>
            {
                var checkpoint = Checkpoint.FromOffset(index);
                await storage.SaveAsync($"pipeline{index}", "node1", checkpoint);
            }));
        }

        await Task.WhenAll(tasks);

        // Assert - All checkpoints should exist
        for (var i = 0; i < 10; i++)
        {
            var exists = await storage.ExistsAsync($"pipeline{i}", "node1");
            exists.Should().BeTrue();
        }
    }

    [Fact]
    public async Task SaveAndLoad_ConcurrentOperations_AreThreadSafe()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);
        var checkpoint = Checkpoint.FromOffset(12345L);
        await storage.SaveAsync("pipeline1", "node1", checkpoint);

        var tasks = new List<Task>();

        // Act - Concurrent reads and writes
        for (var i = 0; i < 5; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                var loaded = await storage.LoadAsync("pipeline1", "node1");
                _ = loaded.Should().NotBeNull();
            }));

            tasks.Add(Task.Run(async () =>
            {
                var newCheckpoint = Checkpoint.FromOffset(i);
                await storage.SaveAsync($"pipeline{i}", "node1", newCheckpoint);
            }));
        }

        await Task.WhenAll(tasks);

        // Assert - No exceptions should have occurred
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public void Dispose_WhenCalled_DisposesCleanly()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);

        // Act & Assert - Should not throw
        storage.Dispose();
    }

    [Fact]
    public void Dispose_CalledMultipleTimes_DoesNotThrow()
    {
        // Arrange
        var storage = new FileCheckpointStorage(_testDirectory);

        // Act & Assert - Multiple disposes should not throw
        storage.Dispose();
        storage.Dispose();
        storage.Dispose();
    }

    #endregion

    #region Helper Methods

    private string GetCheckpointFilePath(string pipelineId, string nodeId)
    {
        // Sanitize identifiers the same way the storage does
        var safePipelineId = SanitizeIdentifier(pipelineId);
        var safeNodeId = SanitizeIdentifier(nodeId);
        return Path.Combine(_testDirectory, safePipelineId, $"{safeNodeId}.json");
    }

    private static string SanitizeIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            return "unknown";

        var invalidChars = Path.GetInvalidFileNameChars();
        var safe = new StringBuilder(identifier);

        foreach (var c in invalidChars)
        {
            safe.Replace(c, '_');
        }

        safe.Replace('/', '_').Replace('\\', '_');

        if (safe.Length > 100)
        {
            var hash = Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(identifier)));

            return $"checkpoint_{hash}";
        }

        return safe.ToString();
    }

    #endregion
}
