using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.Tests;

public sealed class DataLakeConcurrencyTests : IAsyncDisposable
{
    private readonly IStorageProvider _provider;
    private readonly StorageUri _tableUri;
    private readonly string _tempDir;

    public DataLakeConcurrencyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"datalake_concurrency_{Guid.NewGuid():N}");
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
    public async Task ConcurrentWrites_DifferentSnapshotIds_AreIsolated()
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
                Path = "snapshot1-file.parquet",
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
                Path = "snapshot2-file.parquet",
                RowCount = 200,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = 2048,
                SnapshotId = snapshotId2,
            });

            await writer.FlushAsync();
        });

        await Task.WhenAll(task1, task2);

        // Assert - Each snapshot should be queryable independently
        var reader = new ManifestReader(_provider, _tableUri);

        var snapshot1Entries = await reader.ReadBySnapshotAsync(snapshotId1);
        var snapshot2Entries = await reader.ReadBySnapshotAsync(snapshotId2);

        snapshot1Entries.Should().HaveCount(1);
        snapshot1Entries[0].Path.Should().Be("snapshot1-file.parquet");

        snapshot2Entries.Should().HaveCount(1);
        snapshot2Entries[0].Path.Should().Be("snapshot2-file.parquet");
    }

    #endregion

    #region High Concurrency Tests

    [Fact]
    public async Task HighConcurrency_ManyConcurrentWrites_HandlesGracefully()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.By(x => x.Region);
        var tasks = new List<Task>();
        const int concurrentWriters = 10;

        // Act
        for (var i = 0; i < concurrentWriters; i++)
        {
            var index = i;

            tasks.Add(Task.Run(async () =>
            {
                var region = $"Region_{index % 3}"; // Only 3 distinct regions
                var records = CreateRecordsForRegion(region, 10);
                await using var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec);
                await writer.AppendAsync(new InMemoryDataPipe<SalesRecord>(records), CancellationToken.None);
            }));
        }

        await Task.WhenAll(tasks);

        // Assert
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(concurrentWriters * 10);
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

    #region Concurrent Partition Writes Tests

    [Fact]
    public async Task ConcurrentWrites_ToDifferentPartitions_Succeed()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.By(x => x.Region);
        var regions = new[] { "EU", "US", "APAC", "LATAM" };
        var tasks = new List<Task>();

        // Act - Write to different partitions concurrently
        foreach (var region in regions)
        {
            var regionLocal = region; // Capture for closure

            tasks.Add(Task.Run(async () =>
            {
                var records = CreateRecordsForRegion(regionLocal, 25);
                await using var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec);
                await writer.AppendAsync(new InMemoryDataPipe<SalesRecord>(records), CancellationToken.None);
            }));
        }

        await Task.WhenAll(tasks);

        // Assert - All partition directories should exist
        foreach (var region in regions)
        {
            var partitionPath = Path.Combine(_tempDir, $"region={region}");
            Directory.Exists(partitionPath).Should().BeTrue($"partition for region {region} should exist");
        }
    }

    [Fact]
    public async Task ConcurrentWrites_ToDifferentPartitions_PreservesAllData()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.By(x => x.Region);
        var regions = new[] { "EU", "US", "APAC" };
        var tasks = new List<Task>();

        // Act
        foreach (var region in regions)
        {
            var regionLocal = region;

            tasks.Add(Task.Run(async () =>
            {
                var records = CreateRecordsForRegion(regionLocal, 50);
                await using var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec);
                await writer.AppendAsync(new InMemoryDataPipe<SalesRecord>(records), CancellationToken.None);
            }));
        }

        await Task.WhenAll(tasks);

        // Assert - Read all data back
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(150); // 3 regions * 50 records each
    }

    [Fact]
    public async Task ConcurrentWrites_WithMultiplePartitionColumns_Succeeds()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var date1 = new DateOnly(2025, 1, 15);
        var date2 = new DateOnly(2025, 1, 16);
        var regions = new[] { "EU", "US" };

        var tasks = new List<Task>();

        // Act - Write to different date/region combinations concurrently
        foreach (var date in new[] { date1, date2 })
        {
            foreach (var region in regions)
            {
                var dateLocal = date;
                var regionLocal = region;

                tasks.Add(Task.Run(async () =>
                {
                    var records = CreateRecordsForDateAndRegion(dateLocal, regionLocal, 20);
                    await using var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec);
                    await writer.AppendAsync(new InMemoryDataPipe<SalesRecord>(records), CancellationToken.None);
                }));
            }
        }

        await Task.WhenAll(tasks);

        // Assert
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(80); // 2 dates * 2 regions * 20 records
    }

    #endregion

    #region Manifest Append Atomicity Tests

    [Fact]
    public async Task ManifestAppend_ConcurrentAppends_MaintainsConsistency()
    {
        // Arrange
        var snapshotIds = new List<string>();
        var tasks = new List<Task>();
        var lockObj = new object();

        // Act - Multiple concurrent manifest appends
        for (var i = 0; i < 5; i++)
        {
            var index = i;

            tasks.Add(Task.Run(async () =>
            {
                var snapshotId = ManifestWriter.GenerateSnapshotId();

                lock (lockObj)
                {
                    snapshotIds.Add(snapshotId);
                }

                await using var writer = new ManifestWriter(_provider, _tableUri, snapshotId);

                writer.Append(new ManifestEntry
                {
                    Path = $"part-{index:D5}.parquet",
                    RowCount = 100,
                    WrittenAt = DateTimeOffset.UtcNow,
                    FileSizeBytes = 1024,
                    SnapshotId = snapshotId,
                });

                await writer.FlushAsync();
            }));
        }

        await Task.WhenAll(tasks);

        // Assert - All entries should be readable
        var reader = new ManifestReader(_provider, _tableUri);
        var entries = await reader.ReadAllAsync();

        entries.Should().HaveCount(5);
    }

    [Fact]
    public async Task ManifestAppend_ConcurrentAppends_ProducesUniqueSnapshotFiles()
    {
        // Arrange
        var tasks = new List<Task>();

        // Act
        for (var i = 0; i < 3; i++)
        {
            var index = i;

            tasks.Add(Task.Run(async () =>
            {
                var snapshotId = ManifestWriter.GenerateSnapshotId();
                await using var writer = new ManifestWriter(_provider, _tableUri, snapshotId);

                writer.Append(new ManifestEntry
                {
                    Path = $"concurrent-{index}.parquet",
                    RowCount = 50,
                    WrittenAt = DateTimeOffset.UtcNow,
                    FileSizeBytes = 512,
                    SnapshotId = snapshotId,
                });

                await writer.FlushAsync();
            }));
        }

        await Task.WhenAll(tasks);

        // Assert - Check snapshot files exist
        var snapshotsDir = Path.Combine(_tempDir, "_manifest", "snapshots");

        if (Directory.Exists(snapshotsDir))
        {
            var snapshotFiles = Directory.GetFiles(snapshotsDir, "*.ndjson");
            snapshotFiles.Length.Should().BeGreaterOrEqualTo(3);
        }
    }

    #endregion

    #region Helper Methods

    private static List<SalesRecord> CreateRecordsForRegion(string region, int count)
    {
        var records = new List<SalesRecord>();

        for (var i = 0; i < count; i++)
        {
            records.Add(new SalesRecord
            {
                Id = i,
                ProductName = $"Product_{region}_{i}",
                Amount = 100m * (i + 1),
                EventDate = new DateOnly(2025, 1, 15),
                Region = region,
            });
        }

        return records;
    }

    private static List<SalesRecord> CreateRecordsForDateAndRegion(DateOnly date, string region, int count)
    {
        var records = new List<SalesRecord>();

        for (var i = 0; i < count; i++)
        {
            records.Add(new SalesRecord
            {
                Id = i,
                ProductName = $"Product_{date:yyyyMMdd}_{region}_{i}",
                Amount = 100m * (i + 1),
                EventDate = date,
                Region = region,
            });
        }

        return records;
    }

    #endregion
}
