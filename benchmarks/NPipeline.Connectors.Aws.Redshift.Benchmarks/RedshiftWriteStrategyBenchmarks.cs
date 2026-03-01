using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using NPipeline.Connectors.Aws.Redshift.Connection;

namespace NPipeline.Connectors.Aws.Redshift.Benchmarks;

/// <summary>
///     Benchmarks for comparing Redshift write strategies.
///     Note: These benchmarks require a live Redshift connection to run.
///     Set environment variables before running:
///     - NPIPELINE_REDSHIFT_CONNECTION_STRING
/// </summary>
[MemoryDiagnoser]
[RankColumn]
[SuppressMessage("Performance", "CA1822:Mark members as static")]
[SuppressMessage("Design", "CA1001:Types that own disposable fields should be disposable")]
public class RedshiftWriteStrategyBenchmarks
{
    private readonly string _connectionString;
    private readonly List<BenchmarkRow> _rows100k = [];
    private readonly List<BenchmarkRow> _rows10k = [];
    private readonly List<BenchmarkRow> _rows1k = [];
    private IRedshiftConnectionPool? _connectionPool;

    public RedshiftWriteStrategyBenchmarks()
    {
        _connectionString = Environment.GetEnvironmentVariable("NPIPELINE_REDSHIFT_CONNECTION_STRING") ?? "";
    }

    [GlobalSetup]
    public void GlobalSetup()
    {
        // Generate test data
        for (var i = 0; i < 1_000_000; i++)
        {
            var row = new BenchmarkRow
            {
                Id = i,
                Name = $"Customer_{i}",
                Email = $"customer{i}@example.com",
                Amount = i * 1.5m,
                CreatedAt = DateTime.UtcNow.AddMinutes(-i),
                IsActive = i % 2 == 0,
            };

            if (i < 1_000)
                _rows1k.Add(row);

            if (i < 10_000)
                _rows10k.Add(row);

            _rows100k.Add(row);
        }

        // Initialize connection pool if configured
        if (!string.IsNullOrEmpty(_connectionString))
            _connectionPool = new RedshiftConnectionPool(_connectionString);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_connectionPool is not null)
            await _connectionPool.DisposeAsync();
    }

    // Data generation benchmarks (no database required)
    [Benchmark(Description = "Generate 1K test rows")]
    [BenchmarkCategory("DataGeneration")]
    public void GenerateRows_1K()
    {
        var rows = new List<BenchmarkRow>(1000);

        for (var i = 0; i < 1_000; i++)
        {
            rows.Add(new BenchmarkRow
            {
                Id = i,
                Name = $"Customer_{i}",
                Email = $"customer{i}@example.com",
                Amount = i * 1.5m,
                CreatedAt = DateTime.UtcNow.AddMinutes(-i),
                IsActive = i % 2 == 0,
            });
        }
    }

    [Benchmark(Description = "Generate 10K test rows")]
    [BenchmarkCategory("DataGeneration")]
    public void GenerateRows_10K()
    {
        var rows = new List<BenchmarkRow>(10000);

        for (var i = 0; i < 10_000; i++)
        {
            rows.Add(new BenchmarkRow
            {
                Id = i,
                Name = $"Customer_{i}",
                Email = $"customer{i}@example.com",
                Amount = i * 1.5m,
                CreatedAt = DateTime.UtcNow.AddMinutes(-i),
                IsActive = i % 2 == 0,
            });
        }
    }

    [Benchmark(Description = "Generate 100K test rows")]
    [BenchmarkCategory("DataGeneration")]
    public void GenerateRows_100K()
    {
        var rows = new List<BenchmarkRow>(100000);

        for (var i = 0; i < 100_000; i++)
        {
            rows.Add(new BenchmarkRow
            {
                Id = i,
                Name = $"Customer_{i}",
                Email = $"customer{i}@example.com",
                Amount = i * 1.5m,
                CreatedAt = DateTime.UtcNow.AddMinutes(-i),
                IsActive = i % 2 == 0,
            });
        }
    }

    // Connection pool benchmarks
    [Benchmark(Description = "Get connection from pool")]
    [BenchmarkCategory("Connection")]
    public async Task GetConnectionAsync()
    {
        if (_connectionPool is null)
            throw new InvalidOperationException("Connection string not configured");

        await using var connection = await _connectionPool.GetConnectionAsync();

        // Connection is disposed automatically via await using
    }
}

public sealed class BenchmarkRow
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime CreatedAt { get; set; }
    public bool IsActive { get; set; }
}
