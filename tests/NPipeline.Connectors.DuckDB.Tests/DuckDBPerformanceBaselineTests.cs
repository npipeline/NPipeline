using System.Diagnostics;
using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.DuckDB.Tests;

/// <summary>
///     Performance baseline tests for DuckDB connector.
///     These are lightweight regression thresholds for CI, not micro-benchmarks.
/// </summary>
public sealed class DuckDBPerformanceBaselineTests : IDisposable
{
    private const int RecordCount = 100_000;
    private readonly string _dbPath = DuckDBTestHelper.GetTempDatabasePath();

    public void Dispose()
    {
        DuckDBTestHelper.CleanupDatabase(_dbPath);
    }

    [Fact]
    public async Task Read_100K_Rows_CompletesWithinThreshold()
    {
        var records = GenerateRecords(RecordCount).ToList();

        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "perf_read", new DuckDBConfiguration
        {
            AutoCreateTable = true,
            TruncateBeforeWrite = true,
            WriteStrategy = DuckDBWriteStrategy.Appender,
        });

        await sink.ConsumeAsync(
            new DataStream<TestRecord>(records.ToAsyncEnumerable()),
            PipelineContext.Default,
            CancellationToken.None);

        var source = new DuckDBSourceNode<TestRecord>(_dbPath, "SELECT * FROM perf_read ORDER BY \"Id\"");

        var sw = Stopwatch.StartNew();
        var result = await source.OpenStream(PipelineContext.Default, CancellationToken.None).ToListAsync();
        sw.Stop();

        result.Count.Should().Be(RecordCount);

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(20),
            "read baseline exceeded for 100k rows");
    }

    [Fact]
    public async Task Write_100K_Rows_Appender_CompletesWithinThreshold()
    {
        var records = GenerateRecords(RecordCount).ToList();

        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "perf_write_appender", new DuckDBConfiguration
        {
            AutoCreateTable = true,
            TruncateBeforeWrite = true,
            WriteStrategy = DuckDBWriteStrategy.Appender,
        });

        var sw = Stopwatch.StartNew();

        await sink.ConsumeAsync(
            new DataStream<TestRecord>(records.ToAsyncEnumerable()),
            PipelineContext.Default,
            CancellationToken.None);

        sw.Stop();

        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();
        DuckDBTestHelper.GetRowCount(conn, "perf_write_appender").Should().Be(RecordCount);

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30),
            "appender write baseline exceeded for 100k rows");
    }

    [Fact]
    public async Task Write_100K_Rows_Sql_CompletesWithinThreshold()
    {
        var records = GenerateRecords(RecordCount).ToList();

        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "perf_write_sql", new DuckDBConfiguration
        {
            AutoCreateTable = true,
            TruncateBeforeWrite = true,
            WriteStrategy = DuckDBWriteStrategy.Sql,
            BatchSize = 2_000,
        });

        var sw = Stopwatch.StartNew();

        await sink.ConsumeAsync(
            new DataStream<TestRecord>(records.ToAsyncEnumerable()),
            PipelineContext.Default,
            CancellationToken.None);

        sw.Stop();

        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();
        DuckDBTestHelper.GetRowCount(conn, "perf_write_sql").Should().Be(RecordCount);

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(45),
            "SQL write baseline exceeded for 100k rows");
    }

    private static IEnumerable<TestRecord> GenerateRecords(int count)
    {
        for (var index = 1; index <= count; index++)
        {
            yield return new TestRecord
            {
                Id = index,
                Name = $"Record_{index}",
                Value = index * 0.001,
            };
        }
    }
}
