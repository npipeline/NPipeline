using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Nodes;
using NPipeline.DataFlow;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.DuckDB.Tests;

public sealed class DuckDBSourceNodeTests : IDisposable
{
    private readonly string _dbPath;

    public DuckDBSourceNodeTests()
    {
        _dbPath = DuckDBTestHelper.GetTempDatabasePath();
    }

    public void Dispose()
    {
        DuckDBTestHelper.CleanupDatabase(_dbPath);
    }

    [Fact]
    public async Task Source_ReadsAllRows_FromInMemoryDatabase()
    {
        // Arrange - Seed into the file-based database
        await SeedFileDatabase(5);

        var source = new DuckDBSourceNode<TestRecord>(
            _dbPath, "SELECT * FROM items ORDER BY \"Id\"");

        // Act
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        // Assert
        result.Should().HaveCount(5);
        result[0].Id.Should().Be(1);
        result[0].Name.Should().Be("Item1");
        result[4].Id.Should().Be(5);
    }

    [Fact]
    public async Task Source_WithCustomRowMapper_AppliesMapper()
    {
        await SeedFileDatabase(3);

        var source = new DuckDBSourceNode<string>(
            _dbPath,
            "SELECT * FROM items",
            row => $"{row.Get<int>("Id")}:{row.Get<string>("Name")}");

        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(3);
        result.Should().Contain("1:Item1");
    }

    [Fact]
    public async Task Source_WithObserver_InvokesCallbacks()
    {
        await SeedFileDatabase(3);

        var observer = new TestDuckDBObserver();
        var config = new DuckDBConfiguration { Observer = observer };

        var source = new DuckDBSourceNode<TestRecord>(_dbPath, "SELECT * FROM items", config);
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        observer.RowsRead.Should().Be(3);
        observer.ReadCompletedCount.Should().Be(3);
    }

    [Fact]
    public async Task Source_EmptyTable_ReturnsEmptyResults()
    {
        // Create an empty table
        using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE empty_table (\"Id\" INTEGER, \"Name\" VARCHAR, \"Value\" DOUBLE)";
        cmd.ExecuteNonQuery();

        var source = new DuckDBSourceNode<TestRecord>(_dbPath, "SELECT * FROM empty_table");
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().BeEmpty();
    }

    [Fact]
    public void Source_NullQuery_ThrowsArgumentException()
    {
        var act = () => new DuckDBSourceNode<TestRecord>(_dbPath, null!);
        act.Should().Throw<ArgumentException>();
    }

    private async Task SeedFileDatabase(int count)
    {
        await using var conn = new DuckDBConnection($"DataSource={_dbPath}");
        await conn.OpenAsync();
        DuckDBTestHelper.SeedTestRecords(conn, "items", count);
    }
}
