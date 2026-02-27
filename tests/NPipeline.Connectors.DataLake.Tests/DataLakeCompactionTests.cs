using NPipeline.Connectors.DataLake.FormatAdapters;
using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace NPipeline.Connectors.DataLake.Tests;

public sealed class DataLakeCompactionTests : IAsyncDisposable
{
    private readonly IStorageProvider _provider;
    private readonly StorageUri _tableUri;
    private readonly string _tempDir;

    public DataLakeCompactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"datalake_compaction_{Guid.NewGuid():N}");
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

    #region Row Count Preservation Tests

    [Fact]
    public async Task CompactAsync_PreservesRowCount()
    {
        // Arrange
        await CreateSmallFilesAsync(5, 100);

        var request = new TableCompactRequest
        {
            TableBasePath = _tableUri,
            Provider = _provider,
            SmallFileThresholdBytes = 1024 * 1024,
            MinFilesToCompact = 3,
            MaxFilesToCompact = 10,
            DeleteOriginalFiles = false,
        };

        var compactor = new DataLakeCompactor(_provider, _tableUri);

        // Act
        var result = await compactor.CompactAsync(request);

        // Assert
        result.RowsProcessed.Should().Be(500); // 5 files * 100 rows
    }

    #endregion

    #region Manifest Update Tests

    [Fact]
    public async Task CompactAsync_UpdatesManifestWithNewFiles()
    {
        // Arrange
        await CreateSmallFilesAsync(5);

        var request = new TableCompactRequest
        {
            TableBasePath = _tableUri,
            Provider = _provider,
            SmallFileThresholdBytes = 1024 * 1024,
            MinFilesToCompact = 3,
            DeleteOriginalFiles = false,
        };

        var compactor = new DataLakeCompactor(_provider, _tableUri);

        // Act
        var result = await compactor.CompactAsync(request);

        // Assert - Check manifest has new entries
        var reader = new ManifestReader(_provider, _tableUri);
        var entries = await reader.ReadAllAsync();

        // Should have original 5 + new compacted file(s)
        entries.Count.Should().BeGreaterOrEqualTo(5);
        entries.Count(e => e.Path.StartsWith("compacted-", StringComparison.Ordinal)).Should().BeGreaterThan(0);
    }

    #endregion

    #region Max Files Limit Tests

    [Fact]
    public async Task CompactAsync_RespectsMaxFilesLimit()
    {
        // Arrange - Create 20 small files
        await CreateSmallFilesAsync(20);

        var request = new TableCompactRequest
        {
            TableBasePath = _tableUri,
            Provider = _provider,
            SmallFileThresholdBytes = 1024 * 1024,
            MinFilesToCompact = 5,
            MaxFilesToCompact = 10,
            DeleteOriginalFiles = false,
        };

        var compactor = new DataLakeCompactor(_provider, _tableUri);

        // Act
        var result = await compactor.CompactAsync(request);

        // Assert
        result.FilesCompacted.Should().Be(10); // Limited to MaxFilesToCompact
    }

    #endregion

    #region Result Summary Tests

    [Fact]
    public async Task CompactAsync_ReturnsCorrectSummary()
    {
        // Arrange
        await CreateSmallFilesAsync(5);

        var request = new TableCompactRequest
        {
            TableBasePath = _tableUri,
            Provider = _provider,
            SmallFileThresholdBytes = 1024 * 1024,
            MinFilesToCompact = 3,
            DeleteOriginalFiles = false,
        };

        var compactor = new DataLakeCompactor(_provider, _tableUri);

        // Act
        var result = await compactor.CompactAsync(request);

        // Assert - BytesBefore should be positive since we have actual Parquet files
        result.BytesBefore.Should().BeGreaterThan(0);
        result.Duration.Should().BeGreaterThan(TimeSpan.Zero);
        result.ToString().Should().Contain("Compacted");
    }

    #endregion

    #region Test Record Types

    public sealed class SalesRecord
    {
        public int Id { get; set; }
        public string ProductName { get; set; } = string.Empty;

        [ParquetDecimal(18, 2)]
        public decimal Amount { get; set; }

        public DateOnly EventDate { get; set; }
        public string Region { get; set; } = string.Empty;
    }

    #endregion

    #region Dry Run Tests

    [Fact]
    public async Task CompactAsync_DryRun_DoesNotModifyFiles()
    {
        // Arrange - Create small files
        await CreateSmallFilesAsync(5);

        var request = new TableCompactRequest
        {
            TableBasePath = _tableUri,
            Provider = _provider,
            SmallFileThresholdBytes = 1024 * 1024, // 1 MB
            MinFilesToCompact = 3,
            DryRun = true,
        };

        var compactor = new DataLakeCompactor(_provider, _tableUri);

        // Act
        var result = await compactor.CompactAsync(request);

        // Assert
        result.WasDryRun.Should().BeTrue();
        result.FilesCompacted.Should().Be(5);
        result.Message.Should().Contain("Dry run");

        // Verify no compacted files were created
        var compactedFiles = Directory.GetFiles(_tempDir, "compacted-*.parquet", SearchOption.AllDirectories);
        compactedFiles.Should().BeEmpty();
    }

    [Fact]
    public async Task CompactAsync_DryRun_ReportsCorrectFileCount()
    {
        // Arrange
        await CreateSmallFilesAsync(7);

        var request = new TableCompactRequest
        {
            TableBasePath = _tableUri,
            Provider = _provider,
            SmallFileThresholdBytes = 1024 * 1024,
            MinFilesToCompact = 5,
            DryRun = true,
        };

        var compactor = new DataLakeCompactor(_provider, _tableUri);

        // Act
        var result = await compactor.CompactAsync(request);

        // Assert
        result.FilesCompacted.Should().Be(7);
        result.CompactedFiles.Should().HaveCount(7);
    }

    #endregion

    #region Small File Threshold Tests

    [Fact]
    public async Task CompactAsync_NotEnoughSmallFiles_ReturnsEarly()
    {
        // Arrange - Create only 2 small files (below minimum)
        await CreateSmallFilesAsync(2);

        var request = new TableCompactRequest
        {
            TableBasePath = _tableUri,
            Provider = _provider,
            SmallFileThresholdBytes = 1024 * 1024,
            MinFilesToCompact = 5,
        };

        var compactor = new DataLakeCompactor(_provider, _tableUri);

        // Act
        var result = await compactor.CompactAsync(request);

        // Assert
        result.FilesCompacted.Should().Be(0);
        result.FilesCreated.Should().Be(0);
        result.Message.Should().Contain("Not enough small files");
    }

    [Fact]
    public async Task CompactAsync_AboveThreshold_NotCompacted()
    {
        // Arrange - Create manifest entries for "large" files
        var snapshotId = ManifestWriter.GenerateSnapshotId();

        await using (var writer = new ManifestWriter(_provider, _tableUri, snapshotId))
        {
            // Create entries for files that are above the threshold
            writer.Append(new ManifestEntry
            {
                Path = "large-file-1.parquet",
                RowCount = 10000,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 100L * 1024 * 1024, // 100 MB - above threshold
                SnapshotId = snapshotId,
            });

            writer.Append(new ManifestEntry
            {
                Path = "large-file-2.parquet",
                RowCount = 10000,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 100L * 1024 * 1024, // 100 MB - above threshold
                SnapshotId = snapshotId,
            });

            await writer.FlushAsync();
        }

        var request = new TableCompactRequest
        {
            TableBasePath = _tableUri,
            Provider = _provider,
            SmallFileThresholdBytes = 32L * 1024 * 1024, // 32 MB
            MinFilesToCompact = 2,
        };

        var compactor = new DataLakeCompactor(_provider, _tableUri);

        // Act
        var result = await compactor.CompactAsync(request);

        // Assert
        result.FilesCompacted.Should().Be(0);
        result.Message.Should().Contain("Not enough small files");
    }

    #endregion

    #region Partition Value Preservation Tests

    [Fact]
    public async Task CompactAsync_WithPartitions_PreservesPartitionValues()
    {
        // Arrange - Create partitioned small files with actual Parquet data
        var snapshotId = ManifestWriter.GenerateSnapshotId();

        await using (var writer = new ManifestWriter(_provider, _tableUri, snapshotId))
        {
            // Create the actual directory structure
            var partitionDir = Path.Combine(_tempDir, "event_date=2025-01-15");
            _ = Directory.CreateDirectory(partitionDir);

            for (var i = 0; i < 5; i++)
            {
                var fileName = $"part-{i:D5}.parquet";
                var filePath = Path.Combine(partitionDir, fileName);

                // Create actual Parquet file
                await CreateTestParquetFileAsync(filePath, 100, i * 100);
                var fileInfo = new FileInfo(filePath);

                writer.Append(new ManifestEntry
                {
                    Path = $"event_date=2025-01-15/{fileName}",
                    RowCount = 100,
                    WrittenAt = DateTimeOffset.UtcNow,
                    FileSizeBytes = fileInfo.Length,
                    SnapshotId = snapshotId,
                    PartitionValues = new Dictionary<string, string>
                    {
                        ["event_date"] = "2025-01-15",
                    },
                });
            }

            await writer.FlushAsync();
        }

        var request = new TableCompactRequest
        {
            TableBasePath = _tableUri,
            Provider = _provider,
            SmallFileThresholdBytes = 1024 * 1024,
            MinFilesToCompact = 3,
            DeleteOriginalFiles = false,
        };

        var compactor = new DataLakeCompactor(_provider, _tableUri);

        // Act
        var result = await compactor.CompactAsync(request);

        // Assert
        result.FilesCompacted.Should().Be(5);
        result.NewFiles.Should().NotBeEmpty();

        // Verify new files are in the same partition
        foreach (var newFile in result.NewFiles ?? [])
        {
            newFile.Should().StartWith("event_date=2025-01-15/");
        }
    }

    [Fact]
    public async Task CompactAsync_WithPartitionFilter_OnlyCompactsMatchingPartitions()
    {
        // Arrange - Create files in different partitions with actual Parquet data
        var snapshotId = ManifestWriter.GenerateSnapshotId();

        // Create directory structure
        var euDir = Path.Combine(_tempDir, "region=EU");
        var usDir = Path.Combine(_tempDir, "region=US");
        _ = Directory.CreateDirectory(euDir);
        _ = Directory.CreateDirectory(usDir);

        await using (var writer = new ManifestWriter(_provider, _tableUri, snapshotId))
        {
            // Files in EU partition
            for (var i = 0; i < 3; i++)
            {
                var fileName = $"part-{i:D5}.parquet";
                var filePath = Path.Combine(euDir, fileName);

                await CreateTestParquetFileAsync(filePath, 50, i * 50);
                var fileInfo = new FileInfo(filePath);

                writer.Append(new ManifestEntry
                {
                    Path = $"region=EU/{fileName}",
                    RowCount = 50,
                    WrittenAt = DateTimeOffset.UtcNow,
                    FileSizeBytes = fileInfo.Length,
                    SnapshotId = snapshotId,
                    PartitionValues = new Dictionary<string, string> { ["region"] = "EU" },
                });
            }

            // Files in US partition
            for (var i = 0; i < 3; i++)
            {
                var fileName = $"part-{i:D5}.parquet";
                var filePath = Path.Combine(usDir, fileName);

                await CreateTestParquetFileAsync(filePath, 50, 150 + i * 50);
                var fileInfo = new FileInfo(filePath);

                writer.Append(new ManifestEntry
                {
                    Path = $"region=US/{fileName}",
                    RowCount = 50,
                    WrittenAt = DateTimeOffset.UtcNow,
                    FileSizeBytes = fileInfo.Length,
                    SnapshotId = snapshotId,
                    PartitionValues = new Dictionary<string, string> { ["region"] = "US" },
                });
            }

            await writer.FlushAsync();
        }

        var request = new TableCompactRequest
        {
            TableBasePath = _tableUri,
            Provider = _provider,
            SmallFileThresholdBytes = 1024 * 1024,
            MinFilesToCompact = 2,
            PartitionFilters = new Dictionary<string, string> { ["region"] = "EU" },
            DeleteOriginalFiles = false,
        };

        var compactor = new DataLakeCompactor(_provider, _tableUri);

        // Act
        var result = await compactor.CompactAsync(request);

        // Assert - Only EU files should be compacted
        result.FilesCompacted.Should().Be(3);
        result.CompactedFiles.Should().OnlyContain(f => f.StartsWith("region=EU"));
    }

    #endregion

    #region Helper Methods

    private async Task CreateSmallFilesAsync(int count, int rowsPerFile = 100)
    {
        var snapshotId = ManifestWriter.GenerateSnapshotId();
        await using var writer = new ManifestWriter(_provider, _tableUri, snapshotId);

        for (var i = 0; i < count; i++)
        {
            var fileName = $"small-file-{i:D5}.parquet";
            var filePath = Path.Combine(_tempDir, fileName);

            // Create actual Parquet file with test data
            await CreateTestParquetFileAsync(filePath, rowsPerFile, i * rowsPerFile);

            // Get actual file size
            var fileInfo = new FileInfo(filePath);

            // Create manifest entry
            writer.Append(new ManifestEntry
            {
                Path = fileName,
                RowCount = rowsPerFile,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = fileInfo.Length,
                SnapshotId = snapshotId,
            });
        }

        await writer.FlushAsync();
    }

    private async Task CreateTestParquetFileAsync(string filePath, int rowCount, int idOffset)
    {
        var schema = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("ProductName"),
            new DataField<decimal>("Amount"),
            new DataField<DateTime>("EventDate"),
            new DataField<string>("Region")
        );

        await using var fileStream = File.Create(filePath);
        await using var parquetWriter = await ParquetWriter.CreateAsync(schema, fileStream);

        using var rowGroupWriter = parquetWriter.CreateRowGroup();

        // Create column data
        var ids = new int[rowCount];
        var productNames = new string?[rowCount];
        var amounts = new decimal[rowCount];
        var eventDates = new DateTime[rowCount];
        var regions = new string?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            ids[i] = idOffset + i;
            productNames[i] = $"Product-{idOffset + i}";
            amounts[i] = 100.00m + i;
            eventDates[i] = new DateTime(2025, 1, 15);
            regions[i] = "US";
        }

        await rowGroupWriter.WriteColumnAsync(new DataColumn(schema.DataFields[0], ids));
        await rowGroupWriter.WriteColumnAsync(new DataColumn(schema.DataFields[1], productNames));
        await rowGroupWriter.WriteColumnAsync(new DataColumn(schema.DataFields[2], amounts));
        await rowGroupWriter.WriteColumnAsync(new DataColumn(schema.DataFields[3], eventDates));
        await rowGroupWriter.WriteColumnAsync(new DataColumn(schema.DataFields[4], regions));
    }

    #endregion
}
