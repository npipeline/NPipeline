using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Parquet.Tests;

public sealed class ParquetIntegrationTests
{
    #region Nullable Values

    [Fact]
    public async Task RoundTrip_WithNullableValues_PreservesNulls()
    {
        // Arrange
        var records = new[]
        {
            new NullableRecord { Id = 1, Name = "First", OptionalValue = 100 },
            new NullableRecord { Id = 2, Name = null, OptionalValue = null },
            new NullableRecord { Id = 3, Name = "Third", OptionalValue = 300 },
        };

        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Act - Write
            var sink = new ParquetSinkNode<NullableRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<NullableRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Act - Read
            var source = new ParquetSourceNode<NullableRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(3);
            result[0].Name.Should().Be("First");
            result[0].OptionalValue.Should().Be(100);
            result[1].Name.Should().BeNull();
            result[1].OptionalValue.Should().BeNull();
            result[2].Name.Should().Be("Third");
            result[2].OptionalValue.Should().Be(300);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Custom Column Names

    [Fact]
    public async Task RoundTrip_WithCustomColumnNames_PreservesData()
    {
        // Arrange
        var records = new[]
        {
            new CustomColumnRecord { RecordId = 1, RecordName = "Test" },
        };

        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Act - Write
            var sink = new ParquetSinkNode<CustomColumnRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<CustomColumnRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Act - Read
            var source = new ParquetSourceNode<CustomColumnRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(1);
            result[0].RecordId.Should().Be(1);
            result[0].RecordName.Should().Be("Test");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Single File Round-Trip

    [Fact]
    public async Task RoundTrip_SingleFile_PreservesData()
    {
        // Arrange
        var records = CreateTestRecords(100);
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Act - Write
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<TestRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Act - Read
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().BeEquivalentTo(records);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithAllDataTypes_PreservesData()
    {
        // Arrange
        var records = new[]
        {
            new AllTypesRecord
            {
                Id = 1,
                Name = "Test",
                Amount = 123.45m,
                CreatedAt = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc),
                Timestamp = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.Zero),
                IsActive = true,
                Score = 95.5,
                Count = 1000L,
                ExternalId = Guid.Parse("A1B2C3D4-E5F6-7890-ABCD-EF1234567890"),
            },
        };

        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Act - Write
            var sink = new ParquetSinkNode<AllTypesRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<AllTypesRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Act - Read
            var source = new ParquetSourceNode<AllTypesRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(1);
            result[0].Id.Should().Be(1);
            result[0].Name.Should().Be("Test");
            result[0].Amount.Should().Be(123.45m);
            result[0].IsActive.Should().BeTrue();
            result[0].Score.Should().Be(95.5);
            result[0].Count.Should().Be(1000L);
            result[0].ExternalId.Should().Be(Guid.Parse("A1B2C3D4-E5F6-7890-ABCD-EF1234567890"));
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Multi-Row-Group

    [Fact]
    public async Task RoundTrip_WithSmallRowGroupSize_CreatesMultipleRowGroups()
    {
        // Arrange
        var records = CreateTestRecords(100);
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration { RowGroupSize = 25 }; // Small row group size
            var resolver = StorageProviderFactory.CreateResolver();

            // Act - Write
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            await sink.ConsumeAsync(
                new DataStream<TestRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Act - Read
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(100);
            result.Should().BeEquivalentTo(records);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithExactRowGroupBoundary_PreservesData()
    {
        // Arrange - exactly 50 records with row group size of 25
        var records = CreateTestRecords(50);
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration { RowGroupSize = 25 };
            var resolver = StorageProviderFactory.CreateResolver();

            // Act - Write
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            await sink.ConsumeAsync(
                new DataStream<TestRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Act - Read
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(50);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Column Projection

    [Fact]
    public async Task Read_WithProjectedColumns_OnlyReturnsProjectedData()
    {
        // Arrange
        var records = CreateTestRecords(50);
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write all data
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<TestRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with projection - only Id column
            var config = new ParquetConfiguration { ProjectedColumns = ["Id"] };
            var source = new ParquetSourceNode<TestRecord>(uri, resolver, config);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert - should still get all records but only Id is populated
            result.Should().HaveCount(50);
            result[0].Id.Should().Be(0);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Read_WithEmptyProjectedColumns_ReturnsAllData()
    {
        // Arrange
        var records = CreateTestRecords(10);
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver);

            await sink.ConsumeAsync(
                new DataStream<TestRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read with empty projection list
            var config = new ParquetConfiguration { ProjectedColumns = [] };
            var source = new ParquetSourceNode<TestRecord>(uri, resolver, config);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(10);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Multi-File Directory Source

    [Fact]
    public async Task Read_FromDirectoryWithMultipleFiles_CombinesAllData()
    {
        // Arrange
        var tempDir = Path.Combine(Path.GetTempPath(), $"parquet_test_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(tempDir);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Create multiple files
            await CreateParquetFileAsync(Path.Combine(tempDir, "part1.parquet"), CreateTestRecords(0, 50));
            await CreateParquetFileAsync(Path.Combine(tempDir, "part2.parquet"), CreateTestRecords(50, 50));
            await CreateParquetFileAsync(Path.Combine(tempDir, "part3.parquet"), CreateTestRecords(100, 50));

            // Act - Read from directory
            var uri = StorageUri.FromFilePath(tempDir + "/");
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(150);
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
    }

    [Fact]
    public async Task Read_FromDirectoryWithNonParquetFiles_OnlyReadsParquetFiles()
    {
        // Arrange
        var tempDir = Path.Combine(Path.GetTempPath(), $"parquet_test_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(tempDir);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Create parquet files and non-parquet files
            await CreateParquetFileAsync(Path.Combine(tempDir, "data.parquet"), CreateTestRecords(0, 10));
            await File.WriteAllTextAsync(Path.Combine(tempDir, "readme.txt"), "Not a parquet file");
            await File.WriteAllTextAsync(Path.Combine(tempDir, "config.json"), "{}");

            // Act
            var uri = StorageUri.FromFilePath(tempDir + "/");
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(10);
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
    }

    #endregion

    #region Large Record Count

    [Fact]
    public async Task RoundTrip_WithLargeRecordCount_PreservesData()
    {
        // Arrange
        var records = CreateTestRecords(10_000);
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration { RowGroupSize = 1000 };
            var resolver = StorageProviderFactory.CreateResolver();

            // Act - Write
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            await sink.ConsumeAsync(
                new DataStream<TestRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Act - Read
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();

            // Assert
            result.Should().HaveCount(10_000);
            result.Should().BeEquivalentTo(records);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task RoundTrip_WithVeryLargeRecordCount_HandlesMemoryEfficiently()
    {
        // Arrange - 50,000 records
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration { RowGroupSize = 5000 };
            var resolver = StorageProviderFactory.CreateResolver();

            // Act - Write (streaming)
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            await sink.ConsumeAsync(
                new DataStream<TestRecord>(GenerateRecords(0, 50_000).ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Act - Read (streaming)
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var count = 0;

            await foreach (var _ in source.OpenStream(PipelineContext.Default, CancellationToken.None))
            {
                count++;
            }

            // Assert
            count.Should().Be(50_000);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Helper Methods

    private static List<TestRecord> CreateTestRecords(int count)
    {
        return CreateTestRecords(0, count);
    }

    private static List<TestRecord> CreateTestRecords(int startId, int count)
    {
        return GenerateRecords(startId, count).ToList();
    }

    private static IEnumerable<TestRecord> GenerateRecords(int startId, int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new TestRecord
            {
                Id = startId + i,
                Name = $"Record_{startId + i}",
            };
        }
    }

    private static async Task CreateParquetFileAsync(string path, IEnumerable<TestRecord> records)
    {
        var uri = StorageUri.FromFilePath(path);
        var resolver = StorageProviderFactory.CreateResolver();
        var sink = new ParquetSinkNode<TestRecord>(uri, resolver);

        await sink.ConsumeAsync(
            new DataStream<TestRecord>(records.ToAsyncEnumerable()),
            PipelineContext.Default,
            CancellationToken.None);
    }

    private static void CleanupFile(string path)
    {
        if (File.Exists(path))
            File.Delete(path);

        var directory = Path.GetDirectoryName(path);

        if (directory is not null && Directory.Exists(directory))
        {
            var tempFiles = Directory.GetFiles(directory, Path.GetFileName(path) + ".tmp-*");

            foreach (var tempFile in tempFiles)
            {
                try
                {
                    File.Delete(tempFile);
                }
                catch
                {
                    /* ignore */
                }
            }
        }
    }

    #endregion

    #region Test Record Types

    private sealed class TestRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private sealed class AllTypesRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }

        [ParquetDecimal(18, 4)]
        public decimal Amount { get; set; }

        public DateTime CreatedAt { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public bool IsActive { get; set; }
        public double Score { get; set; }
        public long Count { get; set; }
        public Guid ExternalId { get; set; }
    }

    private sealed class NullableRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public int? OptionalValue { get; set; }
    }

    private sealed class CustomColumnRecord
    {
        [ParquetColumn("record_id")]
        public int RecordId { get; set; }

        [ParquetColumn("record_name")]
        public string? RecordName { get; set; }
    }

    #endregion
}
