using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Nodes;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.DuckDB.Tests;

public sealed class DuckDBSinkNodeTests : IDisposable
{
    private readonly string _dbPath;

    public DuckDBSinkNodeTests()
    {
        _dbPath = DuckDBTestHelper.GetTempDatabasePath();
    }

    public void Dispose()
    {
        DuckDBTestHelper.CleanupDatabase(_dbPath);
    }

    [Fact]
    public async Task Sink_WritesAllRows_WithAppenderStrategy()
    {
        // Arrange
        var records = Enumerable.Range(1, 10).Select(i => new TestRecord
        {
            Id = i, Name = $"Item{i}", Value = i * 1.5,
        }).ToList();

        var config = new DuckDBConfiguration { WriteStrategy = DuckDBWriteStrategy.Appender };
        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "test_records", config);
        var pipe = new DataStream<TestRecord>(records.ToAsyncEnumerable());

        // Act
        await sink.ConsumeAsync(pipe, PipelineContext.Default, CancellationToken.None);

        // Assert
        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();
        DuckDBTestHelper.GetRowCount(conn, "test_records").Should().Be(10);
    }

    [Fact]
    public async Task Sink_WritesAllRows_WithSqlStrategy()
    {
        var records = Enumerable.Range(1, 5).Select(i => new TestRecord
        {
            Id = i, Name = $"Item{i}", Value = i * 1.1,
        }).ToList();

        var config = new DuckDBConfiguration
        {
            WriteStrategy = DuckDBWriteStrategy.Sql,
            BatchSize = 2,
        };

        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "sql_records", config);
        var pipe = new DataStream<TestRecord>(records.ToAsyncEnumerable());

        await sink.ConsumeAsync(pipe, PipelineContext.Default, CancellationToken.None);

        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();
        DuckDBTestHelper.GetRowCount(conn, "sql_records").Should().Be(5);
    }

    [Fact]
    public async Task Sink_AutoCreateTable_CreatesTableIfNotExists()
    {
        var records = new[] { new TestRecord { Id = 1, Name = "Test", Value = 1.0 } };
        var config = new DuckDBConfiguration { AutoCreateTable = true };
        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "auto_created", config);
        var pipe = new DataStream<TestRecord>(records.ToAsyncEnumerable());

        await sink.ConsumeAsync(pipe, PipelineContext.Default, CancellationToken.None);

        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();
        DuckDBTestHelper.GetRowCount(conn, "auto_created").Should().Be(1);
    }

    [Fact]
    public async Task Sink_TruncateBeforeWrite_ClearsExistingData()
    {
        // First write
        var records1 = Enumerable.Range(1, 5).Select(i => new TestRecord
        {
            Id = i, Name = $"Old{i}", Value = i,
        }).ToList();

        var config1 = new DuckDBConfiguration { AutoCreateTable = true };
        var sink1 = new DuckDBSinkNode<TestRecord>(_dbPath, "truncate_test", config1);

        await sink1.ConsumeAsync(
            new DataStream<TestRecord>(records1.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        // Second write with truncate
        var records2 = Enumerable.Range(1, 3).Select(i => new TestRecord
        {
            Id = i, Name = $"New{i}", Value = i * 10,
        }).ToList();

        var config2 = new DuckDBConfiguration
        {
            AutoCreateTable = true,
            TruncateBeforeWrite = true,
        };

        var sink2 = new DuckDBSinkNode<TestRecord>(_dbPath, "truncate_test", config2);

        await sink2.ConsumeAsync(
            new DataStream<TestRecord>(records2.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        // Assert
        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();
        DuckDBTestHelper.GetRowCount(conn, "truncate_test").Should().Be(3);
    }

    [Fact]
    public async Task Sink_WithObserver_InvokesCallbacks()
    {
        var records = Enumerable.Range(1, 5).Select(i => new TestRecord
        {
            Id = i, Name = $"Item{i}", Value = i,
        }).ToList();

        var observer = new TestDuckDBObserver();
        var config = new DuckDBConfiguration { Observer = observer };
        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "observed_table", config);

        await sink.ConsumeAsync(
            new DataStream<TestRecord>(records.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        observer.RowsWritten.Should().Be(5);
        observer.WriteCompletedCount.Should().Be(5);
    }

    [Fact]
    public async Task Sink_EmptyInput_WritesNothing()
    {
        var config = new DuckDBConfiguration { AutoCreateTable = true };
        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "empty_write", config);
        var pipe = new DataStream<TestRecord>(Array.Empty<TestRecord>().ToAsyncEnumerable());

        await sink.ConsumeAsync(pipe, PipelineContext.Default, CancellationToken.None);

        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();
        DuckDBTestHelper.GetRowCount(conn, "empty_write").Should().Be(0);
    }
}
