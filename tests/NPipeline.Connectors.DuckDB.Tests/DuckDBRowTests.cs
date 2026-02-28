using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Mapping;

namespace NPipeline.Connectors.DuckDB.Tests;

public sealed class DuckDBRowTests : IDisposable
{
    private readonly DuckDBConnection _connection;

    public DuckDBRowTests()
    {
        _connection = DuckDBTestHelper.CreateInMemoryConnection();
    }

    public void Dispose()
    {
        _connection.Dispose();
    }

    [Fact]
    public void Get_IntegerColumn_ReturnsCorrectValue()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 42 AS value";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.Get<int>("value").Should().Be(42);
    }

    [Fact]
    public void Get_StringColumn_ReturnsCorrectValue()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 'hello' AS name";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.Get<string>("name").Should().Be("hello");
    }

    [Fact]
    public void Get_DoubleColumn_ReturnsCorrectValue()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 3.14 AS value";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.Get<double>("value").Should().BeApproximately(3.14, 0.001);
    }

    [Fact]
    public void Get_BoolColumn_ReturnsCorrectValue()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT true AS flag";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.Get<bool>("flag").Should().BeTrue();
    }

    [Fact]
    public void HasColumn_ExistingColumn_ReturnsTrue()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1 AS id, 'test' AS name";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.HasColumn("id").Should().BeTrue();
        row.HasColumn("name").Should().BeTrue();
    }

    [Fact]
    public void HasColumn_NonExistentColumn_ReturnsFalse()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1 AS id";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.HasColumn("missing").Should().BeFalse();
    }

    [Fact]
    public void IsNull_NullColumn_ReturnsTrue()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT NULL AS value";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.IsNull("value").Should().BeTrue();
    }

    [Fact]
    public void IsNull_NonNullColumn_ReturnsFalse()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 42 AS value";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.IsNull("value").Should().BeFalse();
    }

    [Fact]
    public void GetOrDefault_NullColumn_ReturnsDefault()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT NULL AS value";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.GetOrDefault<int>("value").Should().Be(0);
        row.GetOrDefault<string>("value").Should().BeNull();
    }

    [Fact]
    public void TryGet_ExistingColumn_ReturnsTrueWithValue()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 99 AS value";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        var success = row.TryGet<int>("value", out var result);
        success.Should().BeTrue();
        result.Should().Be(99);
    }

    [Fact]
    public void TryGet_NullColumn_ReturnsFalse()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT NULL AS value";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        var success = row.TryGet<int>("value", out _);
        success.Should().BeFalse();
    }

    [Fact]
    public void ColumnNames_ReturnsAllColumns()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1 AS id, 'test' AS name, 3.14 AS value";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.ColumnNames.Should().BeEquivalentTo("id", "name", "value");
    }

    [Fact]
    public void Get_DateTimeColumn_ReturnsCorrectValue()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT TIMESTAMP '2024-01-15 10:30:00' AS ts";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        var dt = row.Get<DateTime>("ts");
        dt.Year.Should().Be(2024);
        dt.Month.Should().Be(1);
        dt.Day.Should().Be(15);
    }

    [Fact]
    public void Get_LongColumn_ReturnsCorrectValue()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 9999999999::BIGINT AS big";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        row.Get<long>("big").Should().Be(9999999999L);
    }
}
