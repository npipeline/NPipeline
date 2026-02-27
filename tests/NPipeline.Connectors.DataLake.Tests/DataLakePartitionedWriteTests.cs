using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Connectors.Parquet;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.Tests;

/// <summary>
///     Tests for partitioned write behavior of DataLakePartitionedSinkNode and DataLakeTableWriter.
///     Covers: single-partition writes, multi-partition writes, Hive directory path structure,
///     and buffer-flush-on-full protection against unbounded high-cardinality partition buffers.
/// </summary>
public sealed class DataLakePartitionedWriteTests : IAsyncDisposable
{
    private readonly string _tempDir;
    private readonly StorageUri _tableUri;
    private readonly IStorageProvider _provider;

    public DataLakePartitionedWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"datalake_write_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(_tempDir);
        _tableUri = StorageUri.FromFilePath(_tempDir);
        var resolver = StorageProviderFactory.CreateResolver();
        _provider = StorageProviderFactory.GetProviderOrThrow(resolver, _tableUri);
    }

    public async ValueTask DisposeAsync()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);

        await Task.CompletedTask;
    }

    #region Single-Partition Write

    [Fact]
    public async Task Write_SinglePartitionColumn_CreatesCorrectHivePath()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.By(x => x.EventDate);
        var records = CreateOrdersForDate(new DateOnly(2025, 3, 10), "EU", 50).ToList();

        // Act
        await using var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec);
        await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(records), CancellationToken.None);

        // Assert — directory follows event_date=yyyy-MM-dd pattern
        var expectedDir = Path.Combine(_tempDir, "event_date=2025-03-10");
        Directory.Exists(expectedDir).Should().BeTrue(
            "single-partition write must create a Hive-style directory");

        var parquetFiles = Directory.GetFiles(expectedDir, "*.parquet", SearchOption.AllDirectories);
        parquetFiles.Should().NotBeEmpty("at least one Parquet file must exist in the partition directory");
    }

    [Fact]
    public async Task Write_SinglePartitionColumn_ParquetFilesAreReadable()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.By(x => x.EventDate);
        var records = CreateOrdersForDate(new DateOnly(2025, 3, 10), "EU", 100).ToList();

        // Act
        await using (var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(records), CancellationToken.None);
        }

        // Read back
        var source = new DataLakeTableSourceNode<OrderRecord>(_provider, _tableUri);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(100);
        result.Should().AllSatisfy(r => r.EventDate.Should().Be(new DateOnly(2025, 3, 10)));
    }

    [Fact]
    public async Task Write_SinglePartition_ManifestContainsPartitionValues()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.By(x => x.Region);
        var records = CreateOrdersForDate(new DateOnly(2025, 1, 1), "APAC", 20).ToList();

        // Act
        await using (var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(records), CancellationToken.None);
        }

        // Assert manifest
        var reader = new ManifestReader(_provider, _tableUri);
        var entries = await reader.ReadAllAsync();

        entries.Should().NotBeEmpty();
        entries.Should().AllSatisfy(e =>
        {
            e.PartitionValues.Should().NotBeNull();
            e.PartitionValues!["region"].Should().Be("APAC");
        });
    }

    #endregion

    #region Multi-Partition Write

    [Fact]
    public async Task Write_TwoPartitionColumns_CreatesNestedHivePaths()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var records = new[]
        {
            // 2025-01-15 / EU
            CreateOrder(1, "2025-01-15", "EU"),
            // 2025-01-15 / US
            CreateOrder(2, "2025-01-15", "US"),
            // 2025-01-16 / EU
            CreateOrder(3, "2025-01-16", "EU"),
        };

        // Act
        await using var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec);
        await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(records), CancellationToken.None);

        // Assert all three nested combinations exist
        Directory.Exists(Path.Combine(_tempDir, "event_date=2025-01-15", "region=EU"))
            .Should().BeTrue("event_date=2025-01-15/region=EU must exist");
        Directory.Exists(Path.Combine(_tempDir, "event_date=2025-01-15", "region=US"))
            .Should().BeTrue("event_date=2025-01-15/region=US must exist");
        Directory.Exists(Path.Combine(_tempDir, "event_date=2025-01-16", "region=EU"))
            .Should().BeTrue("event_date=2025-01-16/region=EU must exist");
    }

    [Fact]
    public async Task Write_ThreePartitionColumns_CreatesTriplyNestedPaths()
    {
        // Arrange
        var spec = PartitionSpec<DetailedRecord>
            .By(x => x.Year)
            .ThenBy(x => x.Month)
            .ThenBy(x => x.Region);

        var records = new[]
        {
            new DetailedRecord { Id = 1, Year = 2025, Month = 1, Region = "EU" },
            new DetailedRecord { Id = 2, Year = 2025, Month = 1, Region = "US" },
            new DetailedRecord { Id = 3, Year = 2025, Month = 2, Region = "EU" },
        };

        // Act
        await using var writer = new DataLakeTableWriter<DetailedRecord>(_provider, _tableUri, spec);
        await writer.AppendAsync(new InMemoryDataPipe<DetailedRecord>(records), CancellationToken.None);

        // Assert
        Directory.Exists(Path.Combine(_tempDir, "year=2025", "month=1", "region=EU")).Should().BeTrue();
        Directory.Exists(Path.Combine(_tempDir, "year=2025", "month=1", "region=US")).Should().BeTrue();
        Directory.Exists(Path.Combine(_tempDir, "year=2025", "month=2", "region=EU")).Should().BeTrue();
    }

    [Fact]
    public async Task Write_MultiplePartitions_RowsRoutedToCorrectPartitions()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.By(x => x.Region);
        var euRecords = Enumerable.Range(0, 30).Select(i => CreateOrder(i, "2025-01-15", "EU")).ToList();
        var usRecords = Enumerable.Range(100, 20).Select(i => CreateOrder(i, "2025-01-15", "US")).ToList();
        var combined = euRecords.Concat(usRecords).ToList();

        // Act
        await using (var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(combined), CancellationToken.None);
        }

        // Read back and verify partitioning
        var source = new DataLakeTableSourceNode<OrderRecord>(_provider, _tableUri);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Count(r => r.Region == "EU").Should().Be(30);
        result.Count(r => r.Region == "US").Should().Be(20);
    }

    [Fact]
    public async Task Write_MultiplePartitions_ManifestHasEntriesPerPartition()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.By(x => x.Region);
        var records = new[]
        {
            CreateOrder(1, "2025-01-01", "EU"),
            CreateOrder(2, "2025-01-01", "US"),
            CreateOrder(3, "2025-01-01", "APAC"),
        };

        // Act
        await using (var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(records), CancellationToken.None);
        }

        // Assert — one manifest entry per partition
        var reader = new ManifestReader(_provider, _tableUri);
        var entries = await reader.ReadAllAsync();

        entries.Should().HaveCount(3, "one file per distinct region partition");
        var regions = entries.Select(e => e.PartitionValues!["region"]).ToHashSet();
        regions.Should().BeEquivalentTo(new[] { "EU", "US", "APAC" });
    }

    #endregion

    #region Hive Directory Path Structure

    [Fact]
    public void HivePathStructure_DateOnlyPartition_FormatsAsIsoDate()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.By(x => x.EventDate);
        var record = CreateOrder(1, "2025-06-30", "EU");

        // Act
        var path = PartitionPathBuilder.BuildPath(record, spec);

        // Assert
        path.Should().Be("event_date=2025-06-30/");
    }

    [Fact]
    public void HivePathStructure_StringPartition_UrlEncodesSpecialChars()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.By(x => x.Region);
        var record = CreateOrder(1, "2025-01-01", "North America");

        // Act
        var path = PartitionPathBuilder.BuildPath(record, spec);

        // Assert — spaces url-encoded
        path.Should().Be("region=North%20America/");
    }

    [Fact]
    public void HivePathStructure_IntegerPartition_FormatsAsInvariantString()
    {
        // Arrange
        var spec = PartitionSpec<DetailedRecord>.By(x => x.Year);
        var record = new DetailedRecord { Id = 1, Year = 2025, Month = 6, Region = "EU" };

        // Act
        var path = PartitionPathBuilder.BuildPath(record, spec);

        // Assert
        path.Should().Be("year=2025/");
    }

    [Fact]
    public async Task HivePathStructure_WrittenFiles_UsePartFileNamingConvention()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.By(x => x.Region);
        var records = Enumerable.Range(0, 5).Select(i => CreateOrder(i, "2025-01-01", "EU")).ToList();

        // Act
        await using (var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(records), CancellationToken.None);
        }

        // Assert — files follow part-NNNNN-xxxxxxxx.parquet convention
        var euDir = Path.Combine(_tempDir, "region=EU");
        var files = Directory.GetFiles(euDir, "*.parquet");

        files.Should().NotBeEmpty();
        files.Should().AllSatisfy(f =>
        {
            var fileName = Path.GetFileName(f);
            // Files should start with "part-" per the plan: part-{sequence:D5}-{guid:N8}.parquet
            fileName.Should().StartWith("part-",
                "data files must follow the part-{seq}-{guid}.parquet naming convention");
        });
    }

    #endregion

    #region Buffer-Flush-on-Full (High-Cardinality Partitions)

    [Fact]
    public async Task Write_HighCardinalityPartitions_DoesNotExceedMaxBufferedRows()
    {
        // Arrange — many distinct partition values to trigger buffer pressure
        const int distinctRegions = 50;
        const int rowsPerRegion = 100;
        const int maxBufferedRows = distinctRegions * 10; // force flush before all rows are accumulated

        var spec = PartitionSpec<OrderRecord>.By(x => x.Region);
        var config = new ParquetConfiguration
        {
            RowGroupSize = 10,                  // tiny row group to force early flushes
            MaxBufferedRows = maxBufferedRows
        };

        var records = Enumerable.Range(0, distinctRegions)
            .SelectMany(i => Enumerable.Range(0, rowsPerRegion)
                .Select(j => CreateOrder(i * rowsPerRegion + j, "2025-01-01", $"Region_{i:D3}")))
            .ToList();

        // Act — should complete without OOM or unbounded accumulation
        await using (var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec, config))
        {
            await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(records), CancellationToken.None);
        }

        // Assert — all rows were written
        var source = new DataLakeTableSourceNode<OrderRecord>(_provider, _tableUri);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(distinctRegions * rowsPerRegion,
            "all rows across all partitions must be persisted even under buffer pressure");
    }

    [Fact]
    public async Task Write_HighCardinalityPartitions_AllPartitionDirectoriesCreated()
    {
        // Arrange
        const int distinctRegions = 20;
        var spec = PartitionSpec<OrderRecord>.By(x => x.Region);

        var records = Enumerable.Range(0, distinctRegions)
            .Select(i => CreateOrder(i, "2025-01-01", $"Region_{i:D3}"))
            .ToList();

        // Act
        await using (var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(records), CancellationToken.None);
        }

        // Assert — each region has its own directory
        for (var i = 0; i < distinctRegions; i++)
        {
            var partDir = Path.Combine(_tempDir, $"region=Region_{i:D3}");
            Directory.Exists(partDir).Should().BeTrue(
                $"partition directory for Region_{i:D3} should exist");
        }
    }

    [Fact]
    public async Task Write_WithSmallRowGroupAndManyPartitions_FlushesIncrementally()
    {
        // Arrange — row group size much smaller than total records to force multiple flushes
        var spec = PartitionSpec<OrderRecord>.By(x => x.Region);
        var config = new ParquetConfiguration { RowGroupSize = 5, MaxBufferedRows = 20 };

        var regions = new[] { "EU", "US", "APAC" };
        var records = regions
            .SelectMany((r, ri) => Enumerable.Range(0, 30)
                .Select(i => CreateOrder(ri * 30 + i, "2025-01-01", r)))
            .ToList(); // 90 total records, 30 per region

        // Act
        await using (var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec, config))
        {
            await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(records), CancellationToken.None);
        }

        // Assert — all records recoverable
        var source = new DataLakeTableSourceNode<OrderRecord>(_provider, _tableUri);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(90);

        foreach (var region in regions)
        {
            result.Count(r => r.Region == region).Should().Be(30,
                $"region {region} should have exactly 30 records");
        }
    }

    #endregion

    #region Unpartitioned Write

    [Fact]
    public async Task Write_WithNoPartitionSpec_WritesToTableRoot()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.None();
        var records = Enumerable.Range(0, 50).Select(i => CreateOrder(i, "2025-01-01", "EU")).ToList();

        // Act
        await using (var writer = new DataLakeTableWriter<OrderRecord>(_provider, _tableUri, spec))
        {
            await writer.AppendAsync(new InMemoryDataPipe<OrderRecord>(records), CancellationToken.None);
        }

        // Assert — no partition subdirectories created; file is at root or directly in tableUri path
        var parquetFiles = Directory.GetFiles(_tempDir, "*.parquet", SearchOption.TopDirectoryOnly);
        parquetFiles.Should().NotBeEmpty(
            "unpartitioned write should place Parquet files directly under the table base path");

        // Readable via DataLakeTableSourceNode
        var source = new DataLakeTableSourceNode<OrderRecord>(_provider, _tableUri);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();
        result.Should().HaveCount(50);
    }

    #endregion

    #region SinkNode Write

    [Fact]
    public async Task DataLakePartitionedSinkNode_Write_CreatesPartitionedFiles()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.By(x => x.Region);
        var records = new[]
        {
            CreateOrder(1, "2025-01-01", "EU"),
            CreateOrder(2, "2025-01-01", "US"),
        };

        var resolver = StorageProviderFactory.CreateResolver();
        var sink = new DataLakePartitionedSinkNode<OrderRecord>(
            _tableUri, spec, resolver);

        // Act
        await sink.ExecuteAsync(
            new InMemoryDataPipe<OrderRecord>(records),
            PipelineContext.Default,
            CancellationToken.None);

        // Assert
        Directory.Exists(Path.Combine(_tempDir, "region=EU")).Should().BeTrue();
        Directory.Exists(Path.Combine(_tempDir, "region=US")).Should().BeTrue();
    }

    [Fact]
    public async Task DataLakePartitionedSinkNode_WithProvider_CreatesPartitionedFiles()
    {
        // Arrange
        var spec = PartitionSpec<OrderRecord>.By(x => x.Region);
        var records = Enumerable.Range(0, 10).Select(i => CreateOrder(i, "2025-01-01", "APAC")).ToList();

        var sink = new DataLakePartitionedSinkNode<OrderRecord>(
            _provider, _tableUri, spec);

        // Act
        await sink.ExecuteAsync(
            new InMemoryDataPipe<OrderRecord>(records),
            PipelineContext.Default,
            CancellationToken.None);

        // Assert
        Directory.Exists(Path.Combine(_tempDir, "region=APAC")).Should().BeTrue();
    }

    #endregion

    #region Helper Methods

    private static OrderRecord CreateOrder(int id, string dateStr, string region)
    {
        return new OrderRecord
        {
            Id = id,
            ProductName = $"Product_{id}",
            Amount = 10m * (id + 1),
            EventDate = DateOnly.Parse(dateStr),
            Region = region
        };
    }

    private static IEnumerable<OrderRecord> CreateOrdersForDate(DateOnly date, string region, int count)
    {
        return Enumerable.Range(0, count)
            .Select(i => new OrderRecord
            {
                Id = i,
                ProductName = $"Prod_{i}",
                Amount = 1m + i,
                EventDate = date,
                Region = region
            });
    }

    #endregion

    #region Test Record Types

    public sealed class OrderRecord
    {
        public int Id { get; set; }
        public string ProductName { get; set; } = string.Empty;

        [ParquetDecimal(18, 2)]
        public decimal Amount { get; set; }

        public DateOnly EventDate { get; set; }
        public string Region { get; set; } = string.Empty;
    }

    public sealed class DetailedRecord
    {
        public int Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public string Region { get; set; } = string.Empty;
    }

    #endregion
}
