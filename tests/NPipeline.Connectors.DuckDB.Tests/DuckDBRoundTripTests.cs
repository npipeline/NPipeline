using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.DuckDB.Tests;

/// <summary>
///     Round-trip tests: write data, read it back, assert it matches.
///     These are the most important integration tests for a connector.
/// </summary>
public sealed class DuckDBRoundTripTests : IDisposable
{
    private readonly string _dbPath;

    public DuckDBRoundTripTests()
    {
        _dbPath = DuckDBTestHelper.GetTempDatabasePath();
    }

    public void Dispose()
    {
        DuckDBTestHelper.CleanupDatabase(_dbPath);
    }

    [Fact]
    public async Task RoundTrip_SimpleRecords_PreservesData()
    {
        // Arrange
        var original = Enumerable.Range(1, 100).Select(i => new TestRecord
        {
            Id = i,
            Name = $"Item{i}",
            Value = i * 1.23,
        }).ToList();

        // Write
        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "round_trip");

        await sink.ExecuteAsync(
            new StreamingDataPipe<TestRecord>(original.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        // Read
        var source = new DuckDBSourceNode<TestRecord>(_dbPath, "SELECT * FROM round_trip ORDER BY \"Id\"");
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        // Assert
        result.Should().HaveCount(100);

        for (var i = 0; i < 100; i++)
        {
            result[i].Id.Should().Be(original[i].Id);
            result[i].Name.Should().Be(original[i].Name);
            result[i].Value.Should().BeApproximately(original[i].Value, 0.001);
        }
    }

    [Fact]
    public async Task RoundTrip_NullableValues_PreservesNulls()
    {
        var original = new[]
        {
            new NullableTestRecord { Id = 1, Name = "First", OptionalValue = 100.0 },
            new NullableTestRecord { Id = 2, Name = null, OptionalValue = null },
            new NullableTestRecord { Id = 3, Name = "Third", OptionalValue = 300.0 },
        };

        var sink = new DuckDBSinkNode<NullableTestRecord>(_dbPath, "nullable_trip");

        await sink.ExecuteAsync(
            new StreamingDataPipe<NullableTestRecord>(original.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        var source = new DuckDBSourceNode<NullableTestRecord>(
            _dbPath, "SELECT * FROM nullable_trip ORDER BY \"Id\"");

        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(3);
        result[0].Name.Should().Be("First");
        result[0].OptionalValue.Should().Be(100.0);
        result[1].Name.Should().BeNull();
        result[1].OptionalValue.Should().BeNull();
        result[2].Name.Should().Be("Third");
    }

    [Fact]
    public async Task RoundTrip_CustomColumnNames_PreservesData()
    {
        var original = new[]
        {
            new CustomColumnRecord { RecordId = 1, RecordName = "Alpha", Ignored = "should not persist" },
            new CustomColumnRecord { RecordId = 2, RecordName = "Beta" },
        };

        var sink = new DuckDBSinkNode<CustomColumnRecord>(_dbPath, "custom_cols");

        await sink.ExecuteAsync(
            new StreamingDataPipe<CustomColumnRecord>(original.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        var source = new DuckDBSourceNode<CustomColumnRecord>(
            _dbPath, "SELECT * FROM custom_cols ORDER BY record_id");

        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(2);
        result[0].RecordId.Should().Be(1);
        result[0].RecordName.Should().Be("Alpha");
        result[0].Ignored.Should().BeNull(); // Ignored column is not written/read
        result[1].RecordId.Should().Be(2);
        result[1].RecordName.Should().Be("Beta");
    }

    [Fact]
    public async Task RoundTrip_AllTypes_PreservesData()
    {
        var now = new DateTime(2024, 6, 15, 12, 30, 0, DateTimeKind.Utc);

        var original = new[]
        {
            new AllTypesRecord
            {
                BoolValue = true,
                ByteValue = 255,
                ShortValue = 32000,
                IntValue = 1_000_000,
                LongValue = 9_000_000_000L,
                FloatValue = 3.14f,
                DoubleValue = 2.71828,
                DecimalValue = 123.456m,
                StringValue = "Hello DuckDB",
                DateTimeValue = now,
            },
        };

        var sink = new DuckDBSinkNode<AllTypesRecord>(_dbPath, "all_types");

        await sink.ExecuteAsync(
            new StreamingDataPipe<AllTypesRecord>(original.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        var source = new DuckDBSourceNode<AllTypesRecord>(_dbPath, "SELECT * FROM all_types");
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(1);
        var r = result[0];
        r.BoolValue.Should().BeTrue();
        r.ByteValue.Should().Be(255);
        r.ShortValue.Should().Be(32000);
        r.IntValue.Should().Be(1_000_000);
        r.LongValue.Should().Be(9_000_000_000L);
        r.FloatValue.Should().BeApproximately(3.14f, 0.01f);
        r.DoubleValue.Should().BeApproximately(2.71828, 0.0001);
        r.StringValue.Should().Be("Hello DuckDB");
    }

    [Fact]
    public async Task RoundTrip_LargeDataset_HandlesVolume()
    {
        var original = Enumerable.Range(1, 10_000).Select(i => new TestRecord
        {
            Id = i,
            Name = $"Record_{i}",
            Value = i * 0.001,
        }).ToList();

        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "large_dataset");

        await sink.ExecuteAsync(
            new StreamingDataPipe<TestRecord>(original.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        var source = new DuckDBSourceNode<TestRecord>(_dbPath, "SELECT COUNT(*) AS \"Id\", '' AS \"Name\", 0.0 AS \"Value\" FROM large_dataset");
        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(1);
        result[0].Id.Should().Be(10_000);
    }

    [Fact]
    public async Task RoundTrip_SqlWriteStrategy_PreservesData()
    {
        var original = Enumerable.Range(1, 20).Select(i => new TestRecord
        {
            Id = i, Name = $"Sql{i}", Value = i,
        }).ToList();

        var config = new DuckDBConfiguration
        {
            WriteStrategy = DuckDBWriteStrategy.Sql,
            BatchSize = 5,
        };

        var sink = new DuckDBSinkNode<TestRecord>(_dbPath, "sql_trip", config);

        await sink.ExecuteAsync(
            new StreamingDataPipe<TestRecord>(original.ToAsyncEnumerable()),
            PipelineContext.Default, CancellationToken.None);

        var source = new DuckDBSourceNode<TestRecord>(
            _dbPath, "SELECT * FROM sql_trip ORDER BY \"Id\"");

        var result = await source.Initialize(PipelineContext.Default, CancellationToken.None).ToListAsync();

        result.Should().HaveCount(20);
        result[0].Name.Should().Be("Sql1");
        result[19].Name.Should().Be("Sql20");
    }
}
