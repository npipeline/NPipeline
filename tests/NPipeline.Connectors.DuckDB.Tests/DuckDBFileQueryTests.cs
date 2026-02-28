using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.DuckDB.Tests;

/// <summary>
///     Tests for DuckDB's ability to query and export files (Parquet/CSV).
/// </summary>
public sealed class DuckDBFileQueryTests : IDisposable
{
    private readonly string _tempDir;

    public DuckDBFileQueryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"npipeline_file_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, true);
        }
        catch
        {
            /* Best effort */
        }
    }

    [Fact]
    public async Task ToFile_ExportsCsvFile()
    {
        var csvPath = Path.Combine(_tempDir, "output.csv");

        var records = Enumerable.Range(1, 5).Select(i => new TestRecord
        {
            Id = i, Name = $"Item{i}", Value = i * 2.0,
        }).ToList();

        var config = new DuckDBConfiguration
        {
            FileExportOptions = new DuckDBFileExportOptions
            {
                CsvHeader = true,
            },
        };

        var sink = DuckDBSinkNode<TestRecord>.ToFile(csvPath, config);

        await sink.ExecuteAsync(
            new StreamingDataPipe<TestRecord>(records.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        File.Exists(csvPath).Should().BeTrue();
        var lines = await File.ReadAllLinesAsync(csvPath);
        lines.Length.Should().BeGreaterThan(1); // Header + data rows
    }

    [Fact]
    public async Task ToFile_ExportsParquetFile()
    {
        var parquetPath = Path.Combine(_tempDir, "output.parquet");

        var records = Enumerable.Range(1, 10).Select(i => new TestRecord
        {
            Id = i, Name = $"Item{i}", Value = i,
        }).ToList();

        var sink = DuckDBSinkNode<TestRecord>.ToFile(parquetPath);

        await sink.ExecuteAsync(
            new StreamingDataPipe<TestRecord>(records.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        File.Exists(parquetPath).Should().BeTrue();
        new FileInfo(parquetPath).Length.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task FromFile_ReadsCsvFile()
    {
        // Write a CSV
        var csvPath = Path.Combine(_tempDir, "input.csv");

        await File.WriteAllTextAsync(csvPath,
            "Id,Name,Value\n1,Alice,10.5\n2,Bob,20.3\n3,Charlie,30.1\n");

        var source = DuckDBSourceNode<TestRecord>.FromFile(csvPath);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(3);
        result[0].Name.Should().Be("Alice");
        result[2].Name.Should().Be("Charlie");
    }

    [Fact]
    public async Task FileRoundTrip_WriteCsvThenRead_PreservesData()
    {
        // Write
        var csvPath = Path.Combine(_tempDir, "roundtrip.csv");

        var original = new[]
        {
            new TestRecord { Id = 1, Name = "Alpha", Value = 1.1 },
            new TestRecord { Id = 2, Name = "Beta", Value = 2.2 },
        };

        var sinkConfig = new DuckDBConfiguration
        {
            FileExportOptions = new DuckDBFileExportOptions { CsvHeader = true },
        };

        var sink = DuckDBSinkNode<TestRecord>.ToFile(csvPath, sinkConfig);

        await sink.ExecuteAsync(
            new StreamingDataPipe<TestRecord>(original.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        // Read back
        var source = DuckDBSourceNode<TestRecord>.FromFile(csvPath);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(2);
        result[0].Id.Should().Be(1);
        result[0].Name.Should().Be("Alpha");
        result[1].Id.Should().Be(2);
        result[1].Name.Should().Be("Beta");
    }

    [Fact]
    public void ToFile_EmptyPath_ThrowsArgumentException()
    {
        var act = () => DuckDBSinkNode<TestRecord>.ToFile("");
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void FromFile_EmptyPath_ThrowsArgumentException()
    {
        var act = () => DuckDBSourceNode<TestRecord>.FromFile("");
        act.Should().Throw<ArgumentException>();
    }
}
