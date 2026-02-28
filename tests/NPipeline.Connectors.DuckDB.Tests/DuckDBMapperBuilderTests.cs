using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Mapping;

namespace NPipeline.Connectors.DuckDB.Tests;

public sealed class DuckDBMapperBuilderTests : IDisposable
{
    private readonly DuckDBConnection _connection;

    public DuckDBMapperBuilderTests()
    {
        _connection = DuckDBTestHelper.CreateInMemoryConnection();
    }

    public void Dispose()
    {
        _connection.Dispose();
    }

    [Fact]
    public void BuildMapper_SimpleClass_MapsAllProperties()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1 AS \"Id\", 'Test' AS \"Name\", 3.14 AS \"Value\"";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        var mapper = DuckDBMapperBuilder.BuildMapper<TestRecord>(row.ColumnNames);
        var result = mapper(row);

        result.Id.Should().Be(1);
        result.Name.Should().Be("Test");
        result.Value.Should().BeApproximately(3.14, 0.001);
    }

    [Fact]
    public void BuildMapper_CaseInsensitive_MapsCorrectly()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1 AS id, 'Test' AS name, 2.0 AS value";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        var mapper = DuckDBMapperBuilder.BuildMapper<TestRecord>(row.ColumnNames);
        var result = mapper(row);

        result.Id.Should().Be(1);
        result.Name.Should().Be("Test");
    }

    [Fact]
    public void BuildMapper_SnakeCaseToPascalCase_MapsCorrectly()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1 AS user_id, 'John' AS first_name, 'Doe' AS last_name";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        var mapper = DuckDBMapperBuilder.BuildMapper<SnakeCaseRecord>(row.ColumnNames);
        var result = mapper(row);

        result.user_id.Should().Be(1);
        result.first_name.Should().Be("John");
        result.last_name.Should().Be("Doe");
    }

    [Fact]
    public void BuildMapper_CustomColumnAttribute_MapsCorrectly()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 42 AS record_id, 'Test' AS record_name";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        var mapper = DuckDBMapperBuilder.BuildMapper<CustomColumnRecord>(row.ColumnNames);
        var result = mapper(row);

        result.RecordId.Should().Be(42);
        result.RecordName.Should().Be("Test");
    }

    [Fact]
    public void BuildMapper_IgnoredColumn_IsSkipped()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1 AS record_id, 'Test' AS record_name, 'ignored' AS ignored";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        var mapper = DuckDBMapperBuilder.BuildMapper<CustomColumnRecord>(row.ColumnNames);
        var result = mapper(row);

        result.Ignored.Should().BeNull();
    }

    [Fact]
    public void BuildMapper_NullableProperties_HandlesNulls()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1 AS \"Id\", NULL AS \"Name\", NULL AS \"OptionalValue\", NULL AS \"CreatedAt\"";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        var mapper = DuckDBMapperBuilder.BuildMapper<NullableTestRecord>(row.ColumnNames);
        var result = mapper(row);

        result.Id.Should().Be(1);
        result.Name.Should().BeNull();
        result.OptionalValue.Should().BeNull();
        result.CreatedAt.Should().BeNull();
    }

    [Fact]
    public void BuildMapper_Mapper_IsCachedBetweenCalls()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1 AS \"Id\", 'A' AS \"Name\", 1.0 AS \"Value\"";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        var row = new DuckDBRow(reader);
        var columns = row.ColumnNames;

        var mapper1 = DuckDBMapperBuilder.BuildMapper<TestRecord>(columns);
        var mapper2 = DuckDBMapperBuilder.BuildMapper<TestRecord>(columns);

        mapper1.Should().BeSameAs(mapper2);
    }
}
