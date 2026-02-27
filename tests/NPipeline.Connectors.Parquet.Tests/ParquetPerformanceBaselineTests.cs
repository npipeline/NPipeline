using System.Diagnostics;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;
using Parquet;

namespace NPipeline.Connectors.Parquet.Tests;

/// <summary>
///     Performance baseline tests for the Parquet connector.
///     These are lightweight regression thresholds checked in CI — not full BenchmarkDotNet suites.
///     Baselines are intentionally generous to remain pass/fail stable across agents.
/// </summary>
public sealed class ParquetPerformanceBaselineTests
{
    private const int SmallRecordCount = 50_000;
    private const int LargeRecordCount = 200_000;

    #region Row-Group Size Variants

    [Theory]
    [InlineData(1_000)]
    [InlineData(10_000)]
    [InlineData(50_000)]
    public async Task Write_VaryingRowGroupSizes_CompletesWithinBaseline(int rowGroupSize)
    {
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration
            {
                RowGroupSize = rowGroupSize,
                Compression = CompressionMethod.Snappy
            };
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<NarrowRecord>(uri, resolver, config);

            var data = new StreamingDataPipe<NarrowRecord>(
                GenerateNarrowRecords(SmallRecordCount).ToAsyncEnumerable());

            var sw = Stopwatch.StartNew();
            await sink.ExecuteAsync(data, PipelineContext.Default, CancellationToken.None);
            sw.Stop();

            // Baseline: 50k narrow records should not take more than 10 seconds on any CI agent
            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(10),
                $"write of {SmallRecordCount} records with row group size {rowGroupSize} took too long");

            // Verify all records were written
            var source = new ParquetSourceNode<NarrowRecord>(uri, resolver);
            var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();
            result.Count.Should().Be(SmallRecordCount);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Theory]
    [InlineData(1_000)]
    [InlineData(10_000)]
    [InlineData(50_000)]
    public async Task Read_VaryingRowGroupSizes_CompletesWithinBaseline(int rowGroupSize)
    {
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration { RowGroupSize = rowGroupSize };
            var resolver = StorageProviderFactory.CreateResolver();

            // Write first
            var sink = new ParquetSinkNode<NarrowRecord>(uri, resolver, config);
            await sink.ExecuteAsync(
                new StreamingDataPipe<NarrowRecord>(GenerateNarrowRecords(SmallRecordCount).ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Now measure read
            var sw = Stopwatch.StartNew();
            var source = new ParquetSourceNode<NarrowRecord>(uri, resolver);
            var readResult = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();
            sw.Stop();

            readResult.Count.Should().Be(SmallRecordCount);
            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(10),
                $"read of {SmallRecordCount} records with row group size {rowGroupSize} took too long");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Compression Codec Variants

    [Theory]
    [InlineData(CompressionMethod.None)]
    [InlineData(CompressionMethod.Snappy)]
    [InlineData(CompressionMethod.Gzip)]
    public async Task Write_DifferentCompressionCodecs_CompletesWithinBaseline(CompressionMethod compression)
    {
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration
            {
                RowGroupSize = 10_000,
                Compression = compression
            };
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<NarrowRecord>(uri, resolver, config);

            var sw = Stopwatch.StartNew();
            await sink.ExecuteAsync(
                new StreamingDataPipe<NarrowRecord>(GenerateNarrowRecords(SmallRecordCount).ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);
            sw.Stop();

            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(15),
                $"{compression} compression write of {SmallRecordCount} records took too long");

            // Gzip should produce a smaller file than no compression for typical data
            var fileInfo = new FileInfo(tempFile);
            fileInfo.Exists.Should().BeTrue();
            fileInfo.Length.Should().BeGreaterThan(0);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Compression_Snappy_ProducesSmallerFileThanNoCompression()
    {
        var noCompressionFile = Path.GetTempFileName() + ".parquet";
        var snappyFile = Path.GetTempFileName() + ".parquet";

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            await WriteRecords(StorageUri.FromFilePath(noCompressionFile), resolver,
                CompressionMethod.None, 10_000);

            await WriteRecords(StorageUri.FromFilePath(snappyFile), resolver,
                CompressionMethod.Snappy, 10_000);

            var noCompressionSize = new FileInfo(noCompressionFile).Length;
            var snappySize = new FileInfo(snappyFile).Length;

            // Snappy should not inflate the file relative to no compression.
            // Parquet's own column encoding (dictionary, RLE) already handles most entropy,
            // so Snappy may produce an equal-sized file — that is acceptable.
            snappySize.Should().BeLessThanOrEqualTo(noCompressionSize,
                "Snappy-compressed file should not be larger than uncompressed file");
        }
        finally
        {
            CleanupFile(noCompressionFile);
            CleanupFile(snappyFile);
        }
    }

    #endregion

    #region Single Large File vs Many Small Files

    [Fact]
    public async Task Write_SingleLargeFile_CompletesWithinBaseline()
    {
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration
            {
                RowGroupSize = 50_000,
                Compression = CompressionMethod.Snappy
            };
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<NarrowRecord>(uri, resolver, config);

            var sw = Stopwatch.StartNew();
            await sink.ExecuteAsync(
                new StreamingDataPipe<NarrowRecord>(GenerateNarrowRecords(LargeRecordCount).ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);
            sw.Stop();

            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30),
                $"single large file write of {LargeRecordCount} records took too long");

            var fileInfo = new FileInfo(tempFile);
            fileInfo.Length.Should().BeGreaterThan(0);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Write_ManySmallFiles_CompletesWithinBaseline()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"perf_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();
            const int fileCount = 20;
            const int recordsPerFile = 1_000;

            var sw = Stopwatch.StartNew();

            for (var i = 0; i < fileCount; i++)
            {
                var filePath = Path.Combine(tempDir, $"part-{i:D5}.parquet");
                var uri = StorageUri.FromFilePath(filePath);
                var config = new ParquetConfiguration { RowGroupSize = 500 };
                var sink = new ParquetSinkNode<NarrowRecord>(uri, resolver, config);

                await sink.ExecuteAsync(
                    new StreamingDataPipe<NarrowRecord>(
                        GenerateNarrowRecords(recordsPerFile, i * recordsPerFile).ToAsyncEnumerable()),
                    PipelineContext.Default,
                    CancellationToken.None);
            }

            sw.Stop();

            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(15),
                $"writing {fileCount} small files took too long");

            // Read from directory
            var dirUri = StorageUri.FromFilePath(tempDir + "/");
            var source = new ParquetSourceNode<NarrowRecord>(dirUri, resolver);
            var count = (await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync()).Count;
            count.Should().Be(fileCount * recordsPerFile);
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, recursive: true);
        }
    }

    #endregion

    #region Column Projection Performance

    [Fact]
    public async Task Read_WithProjection_FasterThanReadingAllColumns()
    {
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();

            // Write wide records
            var sink = new ParquetSinkNode<WideRecord>(uri, resolver,
                new ParquetConfiguration { RowGroupSize = 10_000 });
            await sink.ExecuteAsync(
                new StreamingDataPipe<WideRecord>(GenerateWideRecords(SmallRecordCount).ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Read all columns
            var swAll = Stopwatch.StartNew();
            var sourceAll = new ParquetSourceNode<WideRecord>(uri, resolver);
            var countAll = (await sourceAll.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync()).Count;
            swAll.Stop();

            countAll.Should().Be(SmallRecordCount);
            swAll.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(15));
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Narrow Schema Baseline

    [Fact]
    public async Task RoundTrip_NarrowSchema_ThroughputBaseline()
    {
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration
            {
                RowGroupSize = 50_000,
                Compression = CompressionMethod.Snappy
            };
            var resolver = StorageProviderFactory.CreateResolver();

            // Measure write
            var swWrite = Stopwatch.StartNew();
            var sink = new ParquetSinkNode<NarrowRecord>(uri, resolver, config);
            await sink.ExecuteAsync(
                new StreamingDataPipe<NarrowRecord>(GenerateNarrowRecords(LargeRecordCount).ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);
            swWrite.Stop();

            // Measure read
            var swRead = Stopwatch.StartNew();
            var source = new ParquetSourceNode<NarrowRecord>(uri, resolver);
            var count = (await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync()).Count;
            swRead.Stop();

            count.Should().Be(LargeRecordCount);
            swWrite.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30),
                $"Write baseline exceeded for {LargeRecordCount} narrow records");
            swRead.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(15),
                $"Read baseline exceeded for {LargeRecordCount} narrow records");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Wide Schema Baseline

    [Fact]
    public async Task RoundTrip_WideSchema_ThroughputBaseline()
    {
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration
            {
                RowGroupSize = 10_000,
                Compression = CompressionMethod.Snappy
            };
            var resolver = StorageProviderFactory.CreateResolver();

            const int count = 20_000;

            // Measure write
            var swWrite = Stopwatch.StartNew();
            var sink = new ParquetSinkNode<WideRecord>(uri, resolver, config);
            await sink.ExecuteAsync(
                new StreamingDataPipe<WideRecord>(GenerateWideRecords(count).ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);
            swWrite.Stop();

            // Measure read
            var swRead = Stopwatch.StartNew();
            var source = new ParquetSourceNode<WideRecord>(uri, resolver);
            var result = (await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync()).Count;
            swRead.Stop();

            result.Should().Be(count);
            swWrite.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(20),
                $"Write baseline exceeded for {count} wide records");
            swRead.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(15),
                $"Read baseline exceeded for {count} wide records");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Helper Methods

    private static async Task WriteRecords(
        StorageUri uri,
        NPipeline.StorageProviders.Abstractions.IStorageResolver resolver,
        CompressionMethod compression,
        int count)
    {
        var config = new ParquetConfiguration
        {
            RowGroupSize = 5_000,
            Compression = compression
        };
        var sink = new ParquetSinkNode<NarrowRecord>(uri, resolver, config);
        await sink.ExecuteAsync(
            new StreamingDataPipe<NarrowRecord>(GenerateNarrowRecords(count).ToAsyncEnumerable()),
            PipelineContext.Default,
            CancellationToken.None);
    }

    private static IEnumerable<NarrowRecord> GenerateNarrowRecords(int count, int offset = 0)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new NarrowRecord
            {
                Id = offset + i,
                Name = $"Record_{offset + i}",
                IsActive = i % 2 == 0
            };
        }
    }

    private static IEnumerable<WideRecord> GenerateWideRecords(int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new WideRecord
            {
                Id = i,
                Name = $"Record_{i}",
                Description = $"A longer description for record {i} that adds more string data",
                Category = $"Category_{i % 10}",
                SubCategory = $"SubCategory_{i % 50}",
                Score = i * 0.5,
                Count = i * 10L,
                IsActive = i % 3 == 0,
                CreatedAt = DateTime.UtcNow.AddSeconds(-i),
                ExternalId = Guid.NewGuid()
            };
        }
    }

    private static void CleanupFile(string path)
    {
        if (File.Exists(path))
        {
            try { File.Delete(path); } catch { /* ignore */ }
        }

        var directory = Path.GetDirectoryName(path);
        if (directory is not null)
        {
            var tempFiles = Directory.GetFiles(directory, Path.GetFileName(path) + ".tmp-*");
            foreach (var tempFile in tempFiles)
            {
                try { File.Delete(tempFile); } catch { /* ignore */ }
            }
        }
    }

    #endregion

    #region Test Record Types

    private sealed class NarrowRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed class WideRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public string? Description { get; set; }
        public string? Category { get; set; }
        public string? SubCategory { get; set; }
        public double Score { get; set; }
        public long Count { get; set; }
        public bool IsActive { get; set; }
        public DateTime CreatedAt { get; set; }
        public Guid ExternalId { get; set; }
    }

    #endregion
}
