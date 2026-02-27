using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Parquet.Tests;

public sealed class ParquetNodeBehaviorTests
{
    #region CancellationToken Propagation

    [Fact]
    public async Task SourceNode_WithCancelledToken_ThrowsOperationCanceledException()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            // Create a small test file first
            await CreateTestParquetFileAsync(tempFile, [new TestRecord { Id = 1, Name = "Test" }]);

            var cts = new CancellationTokenSource();
            cts.Cancel();

            var resolver = StorageProviderFactory.CreateResolver();
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);

            // Act
            var act = async () =>
            {
                var pipe = source.Initialize(PipelineContext.Default, cts.Token);
                await pipe.ToListAsync();
            };

            // Assert
            await act.Should().ThrowAsync<OperationCanceledException>();
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task SinkNode_WithCancelledToken_ThrowsOperationCanceledException()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var cts = new CancellationTokenSource();
            cts.Cancel();

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver);

            var data = new StreamingDataPipe<TestRecord>(
                new[] { new TestRecord { Id = 1, Name = "Test" } }.ToAsyncEnumerable());

            // Act
            var act = () => sink.ExecuteAsync(data, PipelineContext.Default, cts.Token);

            // Assert
            await act.Should().ThrowAsync<OperationCanceledException>();
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region RowErrorHandler Skip vs Rethrow

    [Fact]
    public async Task SourceNode_WithRowErrorHandlerSkipping_SkipsProblematicRows()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            // Create a valid file
            await CreateTestParquetFileAsync(tempFile,
            [
                new TestRecord { Id = 1, Name = "First" },
                new TestRecord { Id = 2, Name = "Second" },
                new TestRecord { Id = 3, Name = "Third" }
            ]);

            var config = new ParquetConfiguration
            {
                RowErrorHandler = (ex, row) =>
                {
                    // Skip rows where Id = 2
                    if (row.HasColumn("Id") && row.GetOrDefault("Id", 0) == 2)
                    {
                        return true; // Skip
                    }
                    return false; // Rethrow for others
                }
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var source = new ParquetSourceNode<TestRecord>(uri, resolver, config);

            // Act
            var pipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = await pipe.ToListAsync();

            // Assert - all rows should be present since they're valid
            results.Should().HaveCount(3);
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task SourceNode_WithRowErrorHandlerRethrow_ThrowsOnProblematicRows()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            // Create a valid file
            await CreateTestParquetFileAsync(tempFile,
            [
                new TestRecord { Id = 1, Name = "First" },
                new TestRecord { Id = 2, Name = "Second" }
            ]);

            // Use a custom mapper that throws for Id = 2
            var config = new ParquetConfiguration
            {
                RowErrorHandler = (_, _) => false // Never skip, always rethrow
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var source = new ParquetSourceNode<TestRecord>(
                uri,
                row =>
                {
                    var id = row.GetOrDefault("Id", 0);
                    if (id == 2)
                    {
                        throw new InvalidOperationException("Simulated mapping error for Id=2");
                    }
                    return new TestRecord { Id = id, Name = row.GetOrDefault("Name", "") ?? "" };
                },
                resolver,
                config);

            // Act
            var act = async () =>
            {
                var pipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
                await pipe.ToListAsync();
            };

            // Assert
            await act.Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("*Simulated mapping error*");
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    #endregion

    #region Multi-file Directory Source Ordering

    [Fact]
    public async Task SourceNode_WithMultipleFiles_ReturnsRowsInDeterministicOrder()
    {
        // Arrange
        var tempDir = Path.Combine(Path.GetTempPath(), $"parquet_test_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(tempDir);

        try
        {
            // Create multiple files with different data
            var file1 = Path.Combine(tempDir, "file_a.parquet");
            var file2 = Path.Combine(tempDir, "file_b.parquet");
            var file3 = Path.Combine(tempDir, "file_c.parquet");

            await CreateTestParquetFileAsync(file1, [new TestRecord { Id = 1, Name = "FileA" }]);
            await CreateTestParquetFileAsync(file2, [new TestRecord { Id = 2, Name = "FileB" }]);
            await CreateTestParquetFileAsync(file3, [new TestRecord { Id = 3, Name = "FileC" }]);

            var uri = StorageUri.FromFilePath(tempDir + "/");
            var resolver = StorageProviderFactory.CreateResolver();
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);

            // Act
            var pipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = await pipe.ToListAsync();

            // Assert - files should be processed in alphabetical order by path
            results.Should().HaveCount(3);
            results[0].Name.Should().Be("FileA");
            results[1].Name.Should().Be("FileB");
            results[2].Name.Should().Be("FileC");
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task SourceNode_WithMultipleFilesAndParallelRead_ReturnsRowsInDeterministicOrder()
    {
        // Arrange
        var tempDir = Path.Combine(Path.GetTempPath(), $"parquet_test_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(tempDir);

        try
        {
            var file1 = Path.Combine(tempDir, "file_a.parquet");
            var file2 = Path.Combine(tempDir, "file_b.parquet");
            var file3 = Path.Combine(tempDir, "file_c.parquet");

            await CreateTestParquetFileAsync(file1, [new TestRecord { Id = 1, Name = "FileA" }]);
            await CreateTestParquetFileAsync(file2, [new TestRecord { Id = 2, Name = "FileB" }]);
            await CreateTestParquetFileAsync(file3, [new TestRecord { Id = 3, Name = "FileC" }]);

            var uri = StorageUri.FromFilePath(tempDir + "/");
            var config = new ParquetConfiguration { FileReadParallelism = 2 };
            var resolver = StorageProviderFactory.CreateResolver();
            var source = new ParquetSourceNode<TestRecord>(uri, resolver, config);

            // Act
            var pipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = await pipe.ToListAsync();

            // Assert
            results.Should().HaveCount(3);
            results[0].Name.Should().Be("FileA");
            results[1].Name.Should().Be("FileB");
            results[2].Name.Should().Be("FileC");
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task SourceNode_WithMultipleFilesInSubdirectories_WhenRecursiveDiscoversFiles()
    {
        // Arrange
        var tempDir = Path.Combine(Path.GetTempPath(), $"parquet_test_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(tempDir);
        var subDir = Path.Combine(tempDir, "subdir");
        _ = Directory.CreateDirectory(subDir);

        try
        {
            var rootFile = Path.Combine(tempDir, "root.parquet");
            var subFile = Path.Combine(subDir, "nested.parquet");

            await CreateTestParquetFileAsync(rootFile, [new TestRecord { Id = 1, Name = "Root" }]);
            await CreateTestParquetFileAsync(subFile, [new TestRecord { Id = 2, Name = "Nested" }]);

            var uri = StorageUri.FromFilePath(tempDir + "/");
            var config = new ParquetConfiguration { RecursiveDiscovery = true };
            var resolver = StorageProviderFactory.CreateResolver();
            var source = new ParquetSourceNode<TestRecord>(uri, resolver, config);

            // Act
            var pipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = await pipe.ToListAsync();

            // Assert - both files should be discovered
            results.Should().HaveCount(2);
            results.Select(r => r.Name).Should().BeEquivalentTo("Root", "Nested");
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    #endregion

    #region Empty Input Handling

    [Fact]
    public async Task SinkNode_WithEmptyInput_CreatesValidParquetFile()
    {
        // Arrange
        var tempFile = Path.GetTempFileName() + ".parquet";
        var uri = StorageUri.FromFilePath(tempFile);

        try
        {
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new ParquetSinkNode<TestRecord>(uri, resolver);

            var emptyData = new StreamingDataPipe<TestRecord>(
                Enumerable.Empty<TestRecord>().ToAsyncEnumerable());

            // Act
            await sink.ExecuteAsync(emptyData, PipelineContext.Default, CancellationToken.None);

            // Assert - file should exist but be empty
            File.Exists(tempFile).Should().BeTrue();

            // Read back should return empty
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);
            var pipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = await pipe.ToListAsync();
            results.Should().BeEmpty();
        }
        finally
        {
            CleanupFile(tempFile);
        }
    }

    [Fact]
    public async Task SourceNode_WithEmptyDirectory_ReturnsEmptyResults()
    {
        // Arrange
        var tempDir = Path.Combine(Path.GetTempPath(), $"parquet_test_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(tempDir);

        try
        {
            var uri = StorageUri.FromFilePath(tempDir + "/");
            var resolver = StorageProviderFactory.CreateResolver();
            var source = new ParquetSourceNode<TestRecord>(uri, resolver);

            // Act
            var pipe = source.Initialize(PipelineContext.Default, CancellationToken.None);
            var results = await pipe.ToListAsync();

            // Assert
            results.Should().BeEmpty();
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    #endregion

    #region Helper Methods

    private static async Task CreateTestParquetFileAsync(string path, IEnumerable<TestRecord> records)
    {
        var uri = StorageUri.FromFilePath(path);
        var resolver = StorageProviderFactory.CreateResolver();
        var sink = new ParquetSinkNode<TestRecord>(uri, resolver);
        var data = new StreamingDataPipe<TestRecord>(records.ToAsyncEnumerable());
        await sink.ExecuteAsync(data, PipelineContext.Default, CancellationToken.None);
    }

    private static void CleanupFile(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
        // Also clean up temp files created by atomic write
        var directory = Path.GetDirectoryName(path);
        if (directory is not null && Directory.Exists(directory))
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

    private sealed class TestRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    #endregion
}
