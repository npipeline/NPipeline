using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace NPipeline.Benchmarks;

/// <summary>
///     Helper class for generating realistic test data for benchmarks.
/// </summary>
public static class BenchmarkDataGenerators
{
    private static readonly Random Random = new(42); // Fixed seed for reproducible results

    /// <summary>
    ///     Generates a sequence of integers with configurable complexity.
    /// </summary>
    public static IAsyncEnumerable<int> GenerateIntegers(int count, CancellationToken ct = default)
    {
        return GenerateIntegers(count, 0, 1, ct);
    }

    /// <summary>
    ///     Generates a sequence of integers with configurable complexity.
    /// </summary>
    private static async IAsyncEnumerable<int> GenerateIntegers(
        int count,
        int startValue = 0,
        int step = 1,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.Yield(); // Ensure async behavior

        for (var i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();
            yield return startValue + i * step;
        }
    }

    /// <summary>
    ///     Generates realistic CSV-like data structures.
    /// </summary>
    public static async IAsyncEnumerable<CsvRecord> GenerateCsvRecords(
        int count,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.Yield(); // Ensure async behavior

        var firstNames = new[] { "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank" };
        var lastNames = new[] { "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis" };
        var cities = new[] { "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego" };

        for (var i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();

            var record = new CsvRecord
            {
                Id = i + 1,
                FirstName = firstNames[Random.Next(firstNames.Length)],
                LastName = lastNames[Random.Next(lastNames.Length)],
                Age = Random.Next(18, 80),
                City = cities[Random.Next(cities.Length)],
                Salary = Random.Next(30000, 150000),
                JoinDate = DateTime.Now.AddDays(-Random.Next(1, 365 * 10)),
            };

            yield return record;
        }
    }

    /// <summary>
    ///     Generates realistic JSON-like data structures.
    /// </summary>
    public static async IAsyncEnumerable<JsonRecord> GenerateJsonRecords(
        int count,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.Yield(); // Ensure async behavior

        var categories = new[] { "electronics", "clothing", "books", "home", "sports", "toys", "food", "health" };
        var statuses = new[] { "active", "inactive", "pending", "completed", "cancelled" };

        for (var i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();

            var record = new JsonRecord
            {
                Id = Guid.NewGuid(),
                ProductId = $"PROD-{Random.Next(10000, 99999)}",
                Name = $"Product {i + 1}",
                Category = categories[Random.Next(categories.Length)],
                Price = Math.Round(Random.NextDouble() * 1000, 2),
                InStock = Random.Next(0, 1000),
                Status = statuses[Random.Next(statuses.Length)],
                Tags = GenerateRandomTags(Random.Next(1, 5)),
                Metadata = new Dictionary<string, object>
                {
                    ["created"] = DateTime.UtcNow.AddDays(-Random.Next(1, 365)),
                    ["updated"] = DateTime.UtcNow.AddDays(-Random.Next(0, 30)),
                    ["version"] = Random.Next(1, 10),
                    ["priority"] = Random.Next(1, 6),
                },
            };

            yield return record;
        }
    }

    /// <summary>
    ///     Generates data with varying complexity for memory pressure tests.
    /// </summary>
    public static async IAsyncEnumerable<ComplexDataItem> GenerateComplexData(
        int count,
        int complexityFactor = 1,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.Yield(); // Ensure async behavior

        for (var i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();

            var item = new ComplexDataItem
            {
                Id = i,
                Timestamp = DateTime.UtcNow,
                Data = new byte[1024 * complexityFactor], // Variable size based on complexity
                NestedObjects = GenerateNestedObjects(Random.Next(1, 5) * complexityFactor),
                LargeString = new string('A', 100 * complexityFactor),
            };

            // Fill the byte array with some data
            Random.NextBytes(item.Data);

            yield return item;
        }
    }

    /// <summary>
    ///     Generates a sequence with occasional delays for latency testing.
    /// </summary>
    public static async IAsyncEnumerable<int> GenerateWithDelays(
        int count,
        int delayEveryN = 100,
        int delayMs = 10,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.Yield(); // Ensure async behavior

        for (var i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();

            // Add delay every N items
            if (i > 0 && i % delayEveryN == 0)
                await Task.Delay(delayMs, ct);

            yield return i;
        }
    }

    private static List<string> GenerateRandomTags(int count)
    {
        var possibleTags = new[] { "popular", "new", "sale", "featured", "limited", "premium", "basic", "advanced" };
        var tags = new List<string>();

        for (var i = 0; i < count; i++)
        {
            tags.Add(possibleTags[Random.Next(possibleTags.Length)]);
        }

        return tags;
    }

    private static List<NestedObject> GenerateNestedObjects(int count)
    {
        var objects = new List<NestedObject>();

        for (var i = 0; i < count; i++)
        {
            objects.Add(new NestedObject
            {
                Id = Guid.NewGuid(),
                Value = Random.NextDouble(),
                Name = $"Nested_{i}",
                Created = DateTime.UtcNow.AddDays(-Random.Next(1, 100)),
            });
        }

        return objects;
    }
}

/// <summary>
///     Represents a CSV record for benchmark testing.
/// </summary>
public record CsvRecord
{
    public int Id { get; init; }
    public string FirstName { get; init; } = string.Empty;
    public string LastName { get; init; } = string.Empty;
    public int Age { get; init; }
    public string City { get; init; } = string.Empty;
    public decimal Salary { get; init; }
    public DateTime JoinDate { get; init; }
}

/// <summary>
///     Represents a JSON record for benchmark testing.
/// </summary>
public record JsonRecord
{
    public Guid Id { get; init; }
    public string ProductId { get; init; } = string.Empty;
    public string Name { get; init; } = string.Empty;
    public string Category { get; init; } = string.Empty;
    public double Price { get; init; }
    public int InStock { get; init; }
    public string Status { get; init; } = string.Empty;
    public List<string> Tags { get; init; } = [];
    public Dictionary<string, object> Metadata { get; init; } = [];
}

/// <summary>
///     Represents a complex data item for memory pressure testing.
/// </summary>
public record ComplexDataItem
{
    public int Id { get; init; }
    public DateTime Timestamp { get; init; }
    public byte[] Data { get; init; } = [];
    public List<NestedObject> NestedObjects { get; init; } = [];
    public string LargeString { get; init; } = string.Empty;
}

/// <summary>
///     Represents a nested object within complex data.
/// </summary>
public record NestedObject
{
    public Guid Id { get; init; }
    public double Value { get; init; }
    public string Name { get; init; } = string.Empty;
    public DateTime Created { get; init; }
}
