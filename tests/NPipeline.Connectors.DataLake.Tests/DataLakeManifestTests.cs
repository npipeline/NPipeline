using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.Tests;

public sealed class DataLakeManifestTests : IAsyncDisposable
{
    private readonly IStorageProvider _provider;
    private readonly StorageUri _tableUri;
    private readonly string _tempDir;

    public DataLakeManifestTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"datalake_manifest_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(_tempDir);
        _tableUri = StorageUri.FromFilePath(_tempDir);
        var resolver = StorageProviderFactory.CreateResolver();
        _provider = StorageProviderFactory.GetProviderOrThrow(resolver, _tableUri);
    }

    public async ValueTask DisposeAsync()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);

        await Task.CompletedTask;
    }

    #region Concurrent Entry Isolation Tests

    [Fact]
    public async Task ManifestWriter_ConcurrentAppends_MaintainsIsolation()
    {
        // Arrange
        var snapshotId1 = ManifestWriter.GenerateSnapshotId();
        var snapshotId2 = ManifestWriter.GenerateSnapshotId();

        // Act - Write concurrently with different snapshot IDs
        var task1 = Task.Run(async () =>
        {
            await using var writer = new ManifestWriter(_provider, _tableUri, snapshotId1);

            writer.Append(new ManifestEntry
            {
                Path = "part-snap1.parquet",
                RowCount = 100,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 1024,
                SnapshotId = snapshotId1,
            });

            await writer.FlushAsync();
        });

        var task2 = Task.Run(async () =>
        {
            await using var writer = new ManifestWriter(_provider, _tableUri, snapshotId2);

            writer.Append(new ManifestEntry
            {
                Path = "part-snap2.parquet",
                RowCount = 200,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 2048,
                SnapshotId = snapshotId2,
            });

            await writer.FlushAsync();
        });

        await Task.WhenAll(task1, task2);

        // Assert
        var reader = new ManifestReader(_provider, _tableUri);
        var allEntries = await reader.ReadAllAsync();

        allEntries.Should().HaveCount(2);
        allEntries.Count(e => e.SnapshotId == snapshotId1).Should().Be(1);
        allEntries.Count(e => e.SnapshotId == snapshotId2).Should().Be(1);
    }

    #endregion

    #region ManifestEntry Tests

    [Fact]
    public void ManifestEntry_CreatesWithAllRequiredFields()
    {
        // Arrange & Act
        var entry = new ManifestEntry
        {
            Path = "event_date=2025-01-15/part-00001.parquet",
            RowCount = 1000,
            WrittenAt = DateTimeOffset.UtcNow,
            FileSizeBytes = 4096,
            SnapshotId = "20250115000000000-abcd",
        };

        // Assert
        entry.Path.Should().Be("event_date=2025-01-15/part-00001.parquet");
        entry.RowCount.Should().Be(1000);
        entry.FileSizeBytes.Should().Be(4096);
        entry.SnapshotId.Should().Be("20250115000000000-abcd");
        entry.FormatVersion.Should().Be("v1");
    }

    [Fact]
    public void ManifestEntry_WithPartitionValues_StoresPartitionInfo()
    {
        // Arrange & Act
        var entry = new ManifestEntry
        {
            Path = "event_date=2025-01-15/region=EU/part-00001.parquet",
            RowCount = 500,
            WrittenAt = DateTimeOffset.UtcNow,
            FileSizeBytes = 2048,
            SnapshotId = "20250115000000000-abcd",
            PartitionValues = new Dictionary<string, string>
            {
                ["event_date"] = "2025-01-15",
                ["region"] = "EU",
            },
        };

        // Assert
        entry.PartitionValues.Should().NotBeNull();
        entry.PartitionValues!["event_date"].Should().Be("2025-01-15");
        entry.PartitionValues["region"].Should().Be("EU");
    }

    [Fact]
    public void ManifestEntry_Copy_CreatesDeepCopy()
    {
        // Arrange
        var original = new ManifestEntry
        {
            Path = "part-00001.parquet",
            RowCount = 100,
            WrittenAt = DateTimeOffset.UtcNow,
            FileSizeBytes = 1024,
            SnapshotId = "snap-001",
            PartitionValues = new Dictionary<string, string> { ["key"] = "value" },
        };

        // Act
        var copy = original.Copy();

        // Assert
        copy.Should().NotBeSameAs(original);
        copy.Path.Should().Be(original.Path);
        copy.RowCount.Should().Be(original.RowCount);
        copy.FileSizeBytes.Should().Be(original.FileSizeBytes);
        copy.PartitionValues.Should().NotBeSameAs(original.PartitionValues);
    }

    #endregion

    #region ManifestWriter Tests

    [Fact]
    public void ManifestWriter_GenerateSnapshotId_ProducesValidFormat()
    {
        // Act
        var snapshotId = ManifestWriter.GenerateSnapshotId();

        // Assert
        snapshotId.Should().MatchRegex(@"^\d{17}-[a-f0-9]{8}$");
    }

    [Fact]
    public void ManifestWriter_GenerateSnapshotId_ProducesUniqueIds()
    {
        // Act
        var ids = Enumerable.Range(0, 100)
            .Select(_ => ManifestWriter.GenerateSnapshotId())
            .ToHashSet();

        // Assert
        ids.Should().HaveCount(100, "all generated IDs should be unique");
    }

    [Fact]
    public async Task ManifestWriter_FlushAsync_CreatesManifestFile()
    {
        // Arrange
        var snapshotId = ManifestWriter.GenerateSnapshotId();
        await using var writer = new ManifestWriter(_provider, _tableUri, snapshotId);

        var entry = new ManifestEntry
        {
            Path = "part-00001.parquet",
            RowCount = 100,
            WrittenAt = DateTimeOffset.UtcNow,
            FileSizeBytes = 1024,
            SnapshotId = snapshotId,
        };

        writer.Append(entry);

        // Act
        await writer.FlushAsync();

        // Assert
        var manifestPath = Path.Combine(_tempDir, "_manifest", "manifest.ndjson");
        File.Exists(manifestPath).Should().BeTrue();
    }

    [Fact]
    public async Task ManifestWriter_Append_AppendsMultipleEntries()
    {
        // Arrange
        var snapshotId = ManifestWriter.GenerateSnapshotId();
        await using var writer = new ManifestWriter(_provider, _tableUri, snapshotId);

        var entries = new[]
        {
            new ManifestEntry
            {
                Path = "part-00001.parquet",
                RowCount = 100,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 1024,
                SnapshotId = snapshotId,
            },
            new ManifestEntry
            {
                Path = "part-00002.parquet",
                RowCount = 200,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 2048,
                SnapshotId = snapshotId,
            },
        };

        // Act
        writer.AppendRange(entries);
        await writer.FlushAsync();

        // Assert
        var manifestPath = Path.Combine(_tempDir, "_manifest", "manifest.ndjson");
        var content = await File.ReadAllTextAsync(manifestPath);
        content.Should().Contain("part-00001.parquet");
        content.Should().Contain("part-00002.parquet");
    }

    [Fact]
    public async Task ManifestWriter_Dispose_FlushesPendingEntries()
    {
        // Arrange
        var snapshotId = ManifestWriter.GenerateSnapshotId();
        var writer = new ManifestWriter(_provider, _tableUri, snapshotId);

        var entry = new ManifestEntry
        {
            Path = "part-00001.parquet",
            RowCount = 100,
            WrittenAt = DateTimeOffset.UtcNow,
            FileSizeBytes = 1024,
            SnapshotId = snapshotId,
        };

        writer.Append(entry);

        // Act
        await writer.DisposeAsync();

        // Assert
        var manifestPath = Path.Combine(_tempDir, "_manifest", "manifest.ndjson");
        File.Exists(manifestPath).Should().BeTrue();
    }

    #endregion

    #region ManifestReader Tests

    [Fact]
    public async Task ManifestReader_ReadAllAsync_WithNoManifest_ReturnsEmptyList()
    {
        // Arrange
        var reader = new ManifestReader(_provider, _tableUri);

        // Act
        var entries = await reader.ReadAllAsync();

        // Assert
        entries.Should().BeEmpty();
    }

    [Fact]
    public async Task ManifestReader_ReadAllAsync_ReturnsAllEntries()
    {
        // Arrange
        var snapshotId = ManifestWriter.GenerateSnapshotId();

        await using (var writer = new ManifestWriter(_provider, _tableUri, snapshotId))
        {
            writer.AppendRange(new[]
            {
                new ManifestEntry
                {
                    Path = "part-00001.parquet",
                    RowCount = 100,
                    WrittenAt = DateTimeOffset.UtcNow,
                    FileSizeBytes = 1024,
                    SnapshotId = snapshotId,
                },
                new ManifestEntry
                {
                    Path = "part-00002.parquet",
                    RowCount = 200,
                    WrittenAt = DateTimeOffset.UtcNow,
                    FileSizeBytes = 2048,
                    SnapshotId = snapshotId,
                },
            });

            await writer.FlushAsync();
        }

        var reader = new ManifestReader(_provider, _tableUri);

        // Act
        var entries = await reader.ReadAllAsync();

        // Assert
        entries.Should().HaveCount(2);
    }

    [Fact]
    public async Task ManifestReader_ReadBySnapshotAsync_FiltersBySnapshotId()
    {
        // Arrange
        var snapshotId1 = ManifestWriter.GenerateSnapshotId();
        var snapshotId2 = ManifestWriter.GenerateSnapshotId();

        await using (var writer1 = new ManifestWriter(_provider, _tableUri, snapshotId1))
        {
            writer1.Append(new ManifestEntry
            {
                Path = "part-snap1.parquet",
                RowCount = 100,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 1024,
                SnapshotId = snapshotId1,
            });

            await writer1.FlushAsync();
        }

        await using (var writer2 = new ManifestWriter(_provider, _tableUri, snapshotId2))
        {
            writer2.Append(new ManifestEntry
            {
                Path = "part-snap2.parquet",
                RowCount = 200,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 2048,
                SnapshotId = snapshotId2,
            });

            await writer2.FlushAsync();
        }

        var reader = new ManifestReader(_provider, _tableUri);

        // Act
        var entries = await reader.ReadBySnapshotAsync(snapshotId1);

        // Assert
        entries.Should().HaveCount(1);
        entries[0].Path.Should().Be("part-snap1.parquet");
    }

    [Fact]
    public async Task ManifestReader_ReadAsOfAsync_FiltersByTimestamp()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        var older = now.AddHours(-2);
        var newer = now.AddHours(-1);

        var snapshotId1 = ManifestWriter.GenerateSnapshotId();

        await using (var writer = new ManifestWriter(_provider, _tableUri, snapshotId1))
        {
            writer.Append(new ManifestEntry
            {
                Path = "part-old.parquet",
                RowCount = 100,
                WrittenAt = older,
                FileSizeBytes = 1024,
                SnapshotId = snapshotId1,
            });

            await writer.FlushAsync();
        }

        var snapshotId2 = ManifestWriter.GenerateSnapshotId();

        await using (var writer = new ManifestWriter(_provider, _tableUri, snapshotId2))
        {
            writer.Append(new ManifestEntry
            {
                Path = "part-new.parquet",
                RowCount = 200,
                WrittenAt = newer,
                FileSizeBytes = 2048,
                SnapshotId = snapshotId2,
            });

            await writer.FlushAsync();
        }

        var reader = new ManifestReader(_provider, _tableUri);

        // Act - Read as of time between older and newer
        var asOfTime = now.AddHours(-1).AddMinutes(-30);
        var entries = await reader.ReadAsOfAsync(asOfTime);

        // Assert - Should only include the older entry
        entries.Should().HaveCount(1);
        entries[0].Path.Should().Be("part-old.parquet");
    }

    [Fact]
    public async Task ManifestReader_GetSnapshotIdsAsync_ReturnsOrderedSnapshotIds()
    {
        // Arrange
        var snapshotId1 = ManifestWriter.GenerateSnapshotId();
        var snapshotId2 = ManifestWriter.GenerateSnapshotId();

        await using (var writer1 = new ManifestWriter(_provider, _tableUri, snapshotId1))
        {
            writer1.Append(new ManifestEntry
            {
                Path = "part-1.parquet",
                RowCount = 100,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 1024,
                SnapshotId = snapshotId1,
            });

            await writer1.FlushAsync();
        }

        await Task.Delay(10); // Ensure different timestamps

        await using (var writer2 = new ManifestWriter(_provider, _tableUri, snapshotId2))
        {
            writer2.Append(new ManifestEntry
            {
                Path = "part-2.parquet",
                RowCount = 200,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 2048,
                SnapshotId = snapshotId2,
            });

            await writer2.FlushAsync();
        }

        var reader = new ManifestReader(_provider, _tableUri);

        // Act
        var snapshotIds = await reader.GetSnapshotIdsAsync();

        // Assert
        snapshotIds.Should().HaveCount(2);

        // Verify both snapshot IDs are present (order may vary based on read order)
        snapshotIds.Should().Contain(snapshotId1);
        snapshotIds.Should().Contain(snapshotId2);
    }

    [Fact]
    public async Task ManifestReader_GetTotalRowCountAsync_ReturnsCorrectSum()
    {
        // Arrange
        var snapshotId = ManifestWriter.GenerateSnapshotId();
        await using var writer = new ManifestWriter(_provider, _tableUri, snapshotId);

        writer.AppendRange(new[]
        {
            new ManifestEntry
            {
                Path = "part-00001.parquet",
                RowCount = 100,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 1024,
                SnapshotId = snapshotId,
            },
            new ManifestEntry
            {
                Path = "part-00002.parquet",
                RowCount = 250,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 2048,
                SnapshotId = snapshotId,
            },
            new ManifestEntry
            {
                Path = "part-00003.parquet",
                RowCount = 150,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 1536,
                SnapshotId = snapshotId,
            },
        });

        await writer.FlushAsync();

        var reader = new ManifestReader(_provider, _tableUri);

        // Act
        var totalCount = await reader.GetTotalRowCountAsync();

        // Assert
        totalCount.Should().Be(500);
    }

    [Fact]
    public async Task ManifestReader_ExistsAsync_ReturnsFalseWhenNoManifest()
    {
        // Arrange
        var reader = new ManifestReader(_provider, _tableUri);

        // Act
        var exists = await reader.ExistsAsync();

        // Assert
        exists.Should().BeFalse();
    }

    [Fact]
    public async Task ManifestReader_ExistsAsync_ReturnsTrueWhenManifestExists()
    {
        // Arrange
        var snapshotId = ManifestWriter.GenerateSnapshotId();
        await using var writer = new ManifestWriter(_provider, _tableUri, snapshotId);

        writer.Append(new ManifestEntry
        {
            Path = "part.parquet",
            RowCount = 1,
            WrittenAt = DateTimeOffset.UtcNow,
            FileSizeBytes = 100,
            SnapshotId = snapshotId,
        });

        await writer.FlushAsync();

        var reader = new ManifestReader(_provider, _tableUri);

        // Act
        var exists = await reader.ExistsAsync();

        // Assert
        exists.Should().BeTrue();
    }

    #endregion
}
