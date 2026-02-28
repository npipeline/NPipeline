using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Writers;

namespace NPipeline.Connectors.DuckDB.Tests;

public sealed class DuckDBAppenderWriterTests : IDisposable
{
    private readonly string _dbPath = DuckDBTestHelper.GetTempDatabasePath();

    public void Dispose()
    {
        DuckDBTestHelper.CleanupDatabase(_dbPath);
    }

    [Fact]
    public async Task WriteAsync_AppendsRows_AndFlushPersistsData()
    {
        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();

        await using (var create = conn.CreateCommand())
        {
            create.CommandText = "CREATE TABLE writer_appender (\"Id\" INTEGER, \"Name\" VARCHAR, \"Value\" DOUBLE)";
            await create.ExecuteNonQueryAsync();
        }

        await using var writer = new DuckDBAppenderWriter<TestRecord>(conn, "writer_appender");
        await writer.WriteAsync(new TestRecord { Id = 1, Name = "A", Value = 1.5 }, CancellationToken.None);
        await writer.WriteAsync(new TestRecord { Id = 2, Name = "B", Value = 2.5 }, CancellationToken.None);
        await writer.FlushAsync(CancellationToken.None);

        DuckDBTestHelper.GetRowCount(conn, "writer_appender").Should().Be(2);
    }

    [Fact]
    public async Task WriteAsync_NullValues_ArePersistedAsNull()
    {
        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();

        await using (var create = conn.CreateCommand())
        {
            create.CommandText = "CREATE TABLE writer_nullable (\"Id\" INTEGER, \"Name\" VARCHAR, \"OptionalValue\" DOUBLE, \"CreatedAt\" TIMESTAMP)";
            await create.ExecuteNonQueryAsync();
        }

        await using var writer = new DuckDBAppenderWriter<NullableTestRecord>(conn, "writer_nullable");
        await writer.WriteAsync(new NullableTestRecord { Id = 10, Name = null, OptionalValue = null }, CancellationToken.None);
        await writer.FlushAsync(CancellationToken.None);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT \"Name\" IS NULL, \"OptionalValue\" IS NULL FROM writer_nullable WHERE \"Id\" = 10";
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        reader.GetBoolean(0).Should().BeTrue();
        reader.GetBoolean(1).Should().BeTrue();
    }
}
