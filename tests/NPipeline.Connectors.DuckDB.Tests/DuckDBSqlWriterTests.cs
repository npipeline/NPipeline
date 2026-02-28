using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Writers;

namespace NPipeline.Connectors.DuckDB.Tests;

public sealed class DuckDBSqlWriterTests : IDisposable
{
    private readonly string _dbPath = DuckDBTestHelper.GetTempDatabasePath();

    public void Dispose()
    {
        DuckDBTestHelper.CleanupDatabase(_dbPath);
    }

    [Fact]
    public async Task WriteAsync_BatchesRows_AndFlushWritesRemainder()
    {
        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();

        await using (var create = conn.CreateCommand())
        {
            create.CommandText = "CREATE TABLE writer_sql (\"Id\" INTEGER, \"Name\" VARCHAR, \"Value\" DOUBLE)";
            await create.ExecuteNonQueryAsync();
        }

        var observer = new BatchObserver();
        await using var writer = new DuckDBSqlWriter<TestRecord>(conn, "writer_sql", 2, observer);

        await writer.WriteAsync(new TestRecord { Id = 1, Name = "A", Value = 1.0 }, CancellationToken.None);
        await writer.WriteAsync(new TestRecord { Id = 2, Name = "B", Value = 2.0 }, CancellationToken.None);
        await writer.WriteAsync(new TestRecord { Id = 3, Name = "C", Value = 3.0 }, CancellationToken.None);
        await writer.FlushAsync(CancellationToken.None);

        DuckDBTestHelper.GetRowCount(conn, "writer_sql").Should().Be(3);
        observer.Batches.Should().HaveCount(2);
        observer.Batches[0].BatchSize.Should().Be(2);
        observer.Batches[0].TotalRows.Should().Be(2);
        observer.Batches[1].BatchSize.Should().Be(1);
        observer.Batches[1].TotalRows.Should().Be(3);
    }

    [Fact]
    public async Task WriteAsync_QuotesStringsCorrectly()
    {
        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();

        await using (var create = conn.CreateCommand())
        {
            create.CommandText = "CREATE TABLE writer_sql_quote (\"Id\" INTEGER, \"Name\" VARCHAR, \"Value\" DOUBLE)";
            await create.ExecuteNonQueryAsync();
        }

        await using var writer = new DuckDBSqlWriter<TestRecord>(conn, "writer_sql_quote", 10);
        await writer.WriteAsync(new TestRecord { Id = 7, Name = "O'Reilly", Value = 7.7 }, CancellationToken.None);
        await writer.FlushAsync(CancellationToken.None);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT \"Name\" FROM writer_sql_quote WHERE \"Id\" = 7";
        var name = (string?)await cmd.ExecuteScalarAsync();
        name.Should().Be("O'Reilly");
    }

    private sealed class BatchObserver : IDuckDBConnectorObserver
    {
        public List<(int BatchSize, long TotalRows)> Batches { get; } = [];

        public void OnBatchFlushed(int batchSize, long totalRows)
        {
            Batches.Add((batchSize, totalRows));
        }
    }
}
