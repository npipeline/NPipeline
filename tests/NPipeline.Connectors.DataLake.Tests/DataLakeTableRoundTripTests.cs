using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.Tests;

public sealed class DataLakeTableRoundTripTests : IAsyncDisposable
{
    private readonly IStorageProvider _provider;
    private readonly StorageUri _tableUri;
    private readonly string _tempDir;

    public DataLakeTableRoundTripTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"datalake_roundtrip_{Guid.NewGuid():N}");
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

    #region Snapshot ID Tests

    [Fact]
    public async Task GetSnapshotIdsAsync_ReturnsAllSnapshotIds()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.None();
        var snapshotIds = new List<string>();

        for (var i = 0; i < 3; i++)
        {
            var records = CreateTestRecords(i * 10, 10);
            await using var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec);
            snapshotIds.Add(writer.SnapshotId);
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records), CancellationToken.None);
        }

        // Act
        await using var writer2 = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec);
        var result = await writer2.GetSnapshotIdsAsync();

        // Assert
        result.Should().HaveCount(3);
        result.Should().Contain(snapshotIds);
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

    #region Single Partition Write Tests

    [Fact]
    public async Task AppendAsync_WithSinglePartition_CreatesHiveStylePaths()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.By(x => x.EventDate);
        var records = CreateTestRecordsForDate(new DateOnly(2025, 1, 15), 10);

        // Act
        await using var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec);
        await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records), CancellationToken.None);

        // Assert
        var partitionPath = Path.Combine(_tempDir, "event_date=2025-01-15");
        Directory.Exists(partitionPath).Should().BeTrue();
    }

    [Fact]
    public async Task AppendAsync_WithMultiplePartitions_CreatesNestedHivePaths()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var records = new List<SalesRecord>
        {
            new() { Id = 1, ProductName = "Product A", Amount = 100m, EventDate = new DateOnly(2025, 1, 15), Region = "EU" },
            new() { Id = 2, ProductName = "Product B", Amount = 200m, EventDate = new DateOnly(2025, 1, 15), Region = "US" },
            new() { Id = 3, ProductName = "Product C", Amount = 150m, EventDate = new DateOnly(2025, 1, 16), Region = "EU" },
        };

        // Act
        await using var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec);
        await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records), CancellationToken.None);

        // Assert
        Directory.Exists(Path.Combine(_tempDir, "event_date=2025-01-15", "region=EU")).Should().BeTrue();
        Directory.Exists(Path.Combine(_tempDir, "event_date=2025-01-15", "region=US")).Should().BeTrue();
        Directory.Exists(Path.Combine(_tempDir, "event_date=2025-01-16", "region=EU")).Should().BeTrue();
    }

    #endregion

    #region Full Round-Trip Tests

    [Fact]
    public async Task RoundTrip_Unpartitioned_PreservesData()
    {
        // Arrange
        var records = CreateTestRecords(50);
        var spec = PartitionSpec<SalesRecord>.None();

        // Act - Write
        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records), CancellationToken.None);
        }

        // Act - Read
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri);
        var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

        // Assert
        result.Should().HaveCount(50);
        result.OrderBy(r => r.Id).Select(r => r.Id).Should().BeEquivalentTo(Enumerable.Range(0, 50));
    }

    [Fact]
    public async Task RoundTrip_WithPartitioning_PreservesData()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var records = new List<SalesRecord>();
        var date1 = new DateOnly(2025, 1, 15);
        var date2 = new DateOnly(2025, 1, 16);

        // Create records across multiple partitions
        for (var i = 0; i < 20; i++)
        {
            records.Add(new SalesRecord
            {
                Id = i,
                ProductName = $"Product_{i}",
                Amount = 100m * (i + 1),
                EventDate = i < 10
                    ? date1
                    : date2,
                Region = i % 2 == 0
                    ? "EU"
                    : "US",
            });
        }

        // Act - Write
        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records), CancellationToken.None);
        }

        // Act - Read
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri);
        var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

        // Assert
        result.Should().HaveCount(20);
        result.Select(r => r.Id).Order().Should().BeEquivalentTo(Enumerable.Range(0, 20));
    }

    #endregion

    #region Time Travel Tests

    [Fact]
    public async Task TimeTravel_AsOf_ReturnsCorrectHistoricalSlice()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.None();

        // First write
        var records1 = CreateTestRecords(0, 10);

        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records1), CancellationToken.None);
        }

        // Capture time after first write
        var timeAfterFirstWrite = DateTimeOffset.UtcNow;

        await Task.Delay(100); // Small delay to ensure different timestamps

        // Second write
        var records2 = CreateTestRecords(10, 10);

        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records2), CancellationToken.None);
        }

        // Act - Read as of time after first write (before second write was committed)
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri, timeAfterFirstWrite);
        var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

        // Assert - Should only have first 10 records
        result.Should().HaveCount(10);
        result.All(r => r.Id < 10).Should().BeTrue();
    }

    [Fact]
    public async Task TimeTravel_BySnapshotId_ReturnsCorrectSnapshot()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.None();

        // First write
        var records1 = CreateTestRecords(0, 10);
        string snapshotId1;

        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            snapshotId1 = writer.SnapshotId;
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records1), CancellationToken.None);
        }

        // Second write
        var records2 = CreateTestRecords(10, 15);

        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records2), CancellationToken.None);
        }

        // Act - Read first snapshot
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri, snapshotId1);
        var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

        // Assert
        result.Should().HaveCount(10);
        result.All(r => r.Id < 10).Should().BeTrue();
    }

    #endregion

    #region Multi-Append Tests

    [Fact]
    public async Task MultiAppend_AppendsToExistingTable_PreservesAllData()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.None();

        // First append
        var records1 = CreateTestRecords(0, 25);

        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records1), CancellationToken.None);
        }

        // Second append
        var records2 = CreateTestRecords(25, 25);

        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records2), CancellationToken.None);
        }

        // Act - Read all
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri);
        var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

        // Assert
        result.Should().HaveCount(50);
        result.Select(r => r.Id).Order().Should().BeEquivalentTo(Enumerable.Range(0, 50));
    }

    [Fact]
    public async Task MultiAppend_WithPartitioning_AppendsToCorrectPartitions()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.By(x => x.EventDate);

        // First append - date1
        var date1 = new DateOnly(2025, 1, 15);
        var records1 = CreateTestRecordsForDate(date1, 20);

        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records1), CancellationToken.None);
        }

        // Second append - date2
        var date2 = new DateOnly(2025, 1, 16);
        var records2 = CreateTestRecordsForDate(date2, 15);

        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records2), CancellationToken.None);
        }

        // Act - Read all
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri);
        var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

        // Assert
        result.Should().HaveCount(35);
        result.Count(r => r.EventDate == date1).Should().Be(20);
        result.Count(r => r.EventDate == date2).Should().Be(15);
    }

    #endregion

    #region Large Record Count Tests

    [Fact]
    public async Task RoundTrip_WithLargeRecordCount_PreservesData()
    {
        // Arrange
        var records = CreateTestRecords(5000);
        var spec = PartitionSpec<SalesRecord>.None();

        // Act - Write
        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records), CancellationToken.None);
        }

        // Act - Read
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri);
        var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

        // Assert
        result.Should().HaveCount(5000);
    }

    [Fact]
    public async Task RoundTrip_WithPartitionedLargeRecordCount_DistributesAcrossPartitions()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var records = new List<SalesRecord>();
        var dates = new[] { new DateOnly(2025, 1, 15), new DateOnly(2025, 1, 16), new DateOnly(2025, 1, 17) };
        var regions = new[] { "EU", "US", "APAC" };

        // Create records distributed across ALL date/region combinations (not just matching indices)
        var recordId = 0;

        foreach (var date in dates)
        {
            foreach (var region in regions)
            {
                for (var i = 0; i < 100; i++)
                {
                    records.Add(new SalesRecord
                    {
                        Id = recordId++,
                        ProductName = $"Product_{recordId}",
                        Amount = 100m * recordId,
                        EventDate = date,
                        Region = region,
                    });
                }
            }
        }

        // Act - Write
        await using (var writer = new DataLakeTableWriter<SalesRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataStream<SalesRecord>(records), CancellationToken.None);
        }

        // Act - Read
        var source = new DataLakeTableSourceNode<SalesRecord>(_provider, _tableUri);
        var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

        // Assert
        result.Should().HaveCount(900);

        // Verify partition directories exist
        foreach (var date in dates)
        {
            foreach (var region in regions)
            {
                var partitionPath = Path.Combine(_tempDir, $"event_date={date:yyyy-MM-dd}", $"region={region}");
                Directory.Exists(partitionPath).Should().BeTrue($"partition path {partitionPath} should exist");
            }
        }
    }

    #endregion

    #region Helper Methods

    private static List<SalesRecord> CreateTestRecords(int count)
    {
        return CreateTestRecords(0, count);
    }

    private static List<SalesRecord> CreateTestRecords(int startId, int count)
    {
        var records = new List<SalesRecord>();

        for (var i = 0; i < count; i++)
        {
            records.Add(new SalesRecord
            {
                Id = startId + i,
                ProductName = $"Product_{startId + i}",
                Amount = 100m * (i + 1),
                EventDate = new DateOnly(2025, 1, 15),
                Region = "EU",
            });
        }

        return records;
    }

    private static List<SalesRecord> CreateTestRecordsForDate(DateOnly date, int count)
    {
        var records = new List<SalesRecord>();

        for (var i = 0; i < count; i++)
        {
            records.Add(new SalesRecord
            {
                Id = i,
                ProductName = $"Product_{i}",
                Amount = 100m * (i + 1),
                EventDate = date,
                Region = "EU",
            });
        }

        return records;
    }

    #endregion
}
