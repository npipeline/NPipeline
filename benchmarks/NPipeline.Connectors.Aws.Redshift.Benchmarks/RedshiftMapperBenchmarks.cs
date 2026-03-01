using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using NPipeline.Connectors.Aws.Redshift.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Aws.Redshift.Benchmarks;

[MemoryDiagnoser]
[RankColumn]
[SuppressMessage("Performance", "CA1822:Mark members as static")]
public class RedshiftMapperBenchmarks
{
    private readonly RedshiftRow _testRow;
    private Func<RedshiftRow, MapperTestEntity>? _compiledMapper;

    public RedshiftMapperBenchmarks()
    {
        // Create a fake row for benchmarking
        var fakeReader = new FakeDatabaseReader();
        _testRow = new RedshiftRow(fakeReader);
    }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _compiledMapper = RedshiftMapperBuilder.Build<MapperTestEntity>();
    }

    [Benchmark(Description = "Build mapper (first time)")]
    public void BuildMapper_FirstTime()
    {
        RedshiftMapperBuilder.ClearCache();
        _ = RedshiftMapperBuilder.Build<MapperTestEntity>();
    }

    [Benchmark(Description = "Build mapper (cached)")]
    public void BuildMapper_Cached()
    {
        _ = RedshiftMapperBuilder.Build<MapperTestEntity>();
    }

    [Benchmark(Description = "Map row with compiled mapper")]
    public MapperTestEntity MapRow_Compiled()
    {
        return _compiledMapper!(_testRow);
    }

    [Benchmark(Description = "Map row with reflection")]
    public MapperTestEntity MapRow_Reflection()
    {
        return MapWithReflection(_testRow);
    }

    private static MapperTestEntity MapWithReflection(RedshiftRow row)
    {
        var entity = new MapperTestEntity();
        var type = typeof(MapperTestEntity);

        foreach (var prop in type.GetProperties())
        {
            if (row.HasColumn(prop.Name))
            {
                var value = row.Get<object>(prop.Name);
                prop.SetValue(entity, value);
            }
        }

        return entity;
    }
}

public sealed class MapperTestEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime CreatedAt { get; set; }
    public bool IsActive { get; set; }
    public Guid ExternalId { get; set; }
    public double Score { get; set; }
    public int? NullableInt { get; set; }
}

// Minimal fake for benchmarking
file sealed class FakeDatabaseReader : IDatabaseReader
{
    public bool HasRows => true;
    public int FieldCount => 9;

    public string GetName(int ordinal)
    {
        return ordinal switch
        {
            0 => "id",
            1 => "name",
            2 => "email",
            3 => "amount",
            4 => "created_at",
            5 => "is_active",
            6 => "external_id",
            7 => "score",
            8 => "nullable_int",
            _ => throw new IndexOutOfRangeException(),
        };
    }

    public Type GetFieldType(int ordinal)
    {
        return ordinal switch
        {
            0 => typeof(int),
            1 => typeof(string),
            2 => typeof(string),
            3 => typeof(decimal),
            4 => typeof(DateTime),
            5 => typeof(bool),
            6 => typeof(Guid),
            7 => typeof(double),
            8 => typeof(int?),
            _ => typeof(object),
        };
    }

    public T? GetFieldValue<T>(int ordinal)
    {
        var value = ordinal switch
        {
            0 => (object)1,
            1 => "Test",
            2 => "test@example.com",
            3 => 100.50m,
            4 => DateTime.UtcNow,
            5 => true,
            6 => Guid.NewGuid(),
            7 => 99.9,
            8 => (int?)42,
            _ => null,
        };

        return (T?)value;
    }

    public bool IsDBNull(int ordinal)
    {
        return false;
    }

    public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(false);
    }

    public Task<bool> NextResultAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(false);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
