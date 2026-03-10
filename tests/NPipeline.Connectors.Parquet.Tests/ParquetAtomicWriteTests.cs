using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Parquet.Tests;

public sealed class ParquetAtomicWriteTests
{
    #region Cleanup on Failure

    [Fact]
    public async Task Write_WithExceptionDuringWrite_NoFinalArtifact()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration { UseAtomicWrite = true };
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            // Create data that throws after some items
            var throwingData = new DataStream<TestRecord>(
                GenerateRecordsWithException(100, 50).ToAsyncEnumerable());

            // Act
            var act = () => sink.ConsumeAsync(
                throwingData,
                PipelineContext.Default,
                CancellationToken.None);

            // Assert
            await act.Should().ThrowAsync<InvalidOperationException>();
            File.Exists(tempFile).Should().BeFalse("no file should exist after failed write");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Multiple Row Groups with Atomic Write

    [Fact]
    public async Task Write_WithMultipleRowGroups_CommitsAtomically()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration
            {
                UseAtomicWrite = true,
                RowGroupSize = 100,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            var records = GenerateRecords(500).ToList();

            // Act
            await sink.ConsumeAsync(
                new DataStream<TestRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Assert - all data should be readable
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();
            result.Should().HaveCount(500);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Test Record Types

    private sealed class TestRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    #endregion

    #region Temp-Path Commit on Success

    [Fact]
    public async Task Write_WithAtomicWriteEnabled_CommitsToFinalPath()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration { UseAtomicWrite = true };
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            var records = new[]
            {
                new TestRecord { Id = 1, Name = "Test" },
            };

            // Act
            await sink.ConsumeAsync(
                new DataStream<TestRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Assert - final file should exist
            File.Exists(tempFile).Should().BeTrue();

            // Should be able to read from final path
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();
            result.Should().HaveCount(1);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithAtomicWriteEnabled_NoTempFileRemains()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);
        var directory = Path.GetDirectoryName(tempFile) ?? "";
        var fileName = Path.GetFileName(tempFile);

        try
        {
            var config = new ParquetConfiguration { UseAtomicWrite = true };
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            var records = new[] { new TestRecord { Id = 1, Name = "Test" } };

            // Act
            await sink.ConsumeAsync(
                new DataStream<TestRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Assert - no temp files should remain
            var tempFiles = Directory.GetFiles(directory, fileName + ".tmp-*");
            tempFiles.Should().BeEmpty("no temp files should remain after successful write");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithAtomicWriteDisabled_WritesDirectly()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var config = new ParquetConfiguration { UseAtomicWrite = false };
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            var records = new[] { new TestRecord { Id = 1, Name = "Test" } };

            // Act
            await sink.ConsumeAsync(
                new DataStream<TestRecord>(records.ToAsyncEnumerable()),
                PipelineContext.Default,
                CancellationToken.None);

            // Assert - file should exist
            File.Exists(tempFile).Should().BeTrue();

            // Should be readable
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();
            result.Should().HaveCount(1);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Cancellation Mid-Write

    [Fact]
    public async Task Write_WithCancellation_LeavesNoFinalArtifact()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);
        var cts = new CancellationTokenSource();

        try
        {
            var config = new ParquetConfiguration { UseAtomicWrite = true };
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            // Cancel immediately before writing
            cts.Cancel();

            var data = new DataStream<TestRecord>(
                GenerateRecords(10).ToAsyncEnumerable());

            // Act
            var act = () => sink.ConsumeAsync(
                data,
                PipelineContext.Default,
                cts.Token);

            // Assert
            await act.Should().ThrowAsync<OperationCanceledException>();
            File.Exists(tempFile).Should().BeFalse("no file should exist after cancelled write");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task Write_WithCancellation_CleansUpTempFile()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);
        var directory = Path.GetDirectoryName(tempFile) ?? "";
        var fileName = Path.GetFileName(tempFile);
        var cts = new CancellationTokenSource();

        try
        {
            var config = new ParquetConfiguration { UseAtomicWrite = true };
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver, config);

            // Cancel immediately
            cts.Cancel();

            var data = new DataStream<TestRecord>(
                GenerateRecords(10).ToAsyncEnumerable());

            // Act
            var act = () => sink.ConsumeAsync(
                data,
                PipelineContext.Default,
                cts.Token);

            // Assert
            await act.Should().ThrowAsync<OperationCanceledException>();

            // No temp files should remain
            var tempFiles = Directory.GetFiles(directory, fileName + ".tmp-*");
            tempFiles.Should().BeEmpty("temp files should be cleaned up on cancellation");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Helper Methods

    private static IEnumerable<TestRecord> GenerateRecords(int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new TestRecord { Id = i, Name = $"Record_{i}" };
        }
    }

    private static IEnumerable<TestRecord> GenerateRecordsWithException(int count, int throwAtIndex)
    {
        for (var i = 0; i < count; i++)
        {
            if (i == throwAtIndex)
                throw new InvalidOperationException($"Intentional exception at index {throwAtIndex}");

            yield return new TestRecord { Id = i, Name = $"Record_{i}" };
        }
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
}
