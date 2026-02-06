using System.Diagnostics;
using System.Text.Json;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using Xunit.Abstractions;

namespace NPipeline.Connectors.Json.Tests;

/// <summary>
///     Performance tests for JSON connector operations.
///     Tests serialization/deserialization efficiency, memory usage, and timing benchmarks.
/// </summary>
public class JsonPerformanceTests : IDisposable
{
    private readonly PipelineContext _context;
    private readonly ITestOutputHelper _output;
    private readonly IStorageProvider _provider;
    private readonly string _tempDirectory;

    public JsonPerformanceTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"NPipeline.Json.Performance_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDirectory);

        _provider = StorageProviderFactory.GetProviderOrThrow(
            StorageProviderFactory.CreateResolver(),
            StorageUri.FromFilePath(_tempDirectory));

        _context = new PipelineContext();
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDirectory))
                Directory.Delete(_tempDirectory, true);
        }
        catch
        {
            // Ignore cleanup errors
        }

        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task Performance_SerializeDeserialize_10000Records_CompletesInAcceptableTime()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = Enumerable.Range(1, 10000)
            .Select(i => new TestPerson { Id = i, Name = $"Person{i}", Age = 20 + i % 50 })
            .ToList();

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

        var stopwatch = Stopwatch.StartNew();

        // Act - Read from input
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);
        var readData = await sourcePipe.ToListAsync(CancellationToken.None);

        // Write to output
        var sinkConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        stopwatch.Stop();

        // Assert
        Assert.Equal(10000, readData.Count);
        _output.WriteLine($"Processed 10000 records in {stopwatch.ElapsedMilliseconds}ms");
        Assert.True(stopwatch.ElapsedMilliseconds < 5000, $"Performance test took {stopwatch.ElapsedMilliseconds}ms, expected < 5000ms");
    }

    [Fact]
    public async Task Performance_MemoryUsage_LargeDataset_StaysWithinLimits()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");

        var inputData = Enumerable.Range(1, 5000)
            .Select(i => new TestPerson { Id = i, Name = $"Person{i}", Age = 20 + i % 50 })
            .ToList();

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

        var initialMemory = GC.GetTotalMemory(true);

        // Act - Read from input
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);
        var readData = await sourcePipe.ToListAsync(CancellationToken.None);

        var finalMemory = GC.GetTotalMemory(true);
        var memoryIncrease = finalMemory - initialMemory;

        // Assert
        Assert.Equal(5000, readData.Count);
        _output.WriteLine($"Memory increase: {memoryIncrease / 1024 / 1024}MB for 5000 records");
        Assert.True(memoryIncrease < 100 * 1024 * 1024, $"Memory increase {memoryIncrease / 1024 / 1024}MB exceeds 100MB limit");
    }

    [Fact]
    public async Task Performance_NdjsonFormat_Streaming_HandlesLargeFiles()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.ndjson");
        var outputFile = Path.Combine(_tempDirectory, "output.ndjson");

        var inputData = Enumerable.Range(1, 5000)
            .Select(i => new TestPerson { Id = i, Name = $"Person{i}", Age = 20 + i % 50 })
            .ToList();

        // Write input as NDJSON
        var ndjsonLines = inputData.Select(p => JsonSerializer.Serialize(p));
        await File.WriteAllLinesAsync(inputFile, ndjsonLines);

        var stopwatch = Stopwatch.StartNew();

        // Act - Read from input
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);

        // Write to output
        var sinkConfig = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        stopwatch.Stop();

        // Assert
        _output.WriteLine($"NDJSON processing of 5000 records took {stopwatch.ElapsedMilliseconds}ms");
        Assert.True(stopwatch.ElapsedMilliseconds < 3000, $"NDJSON processing took {stopwatch.ElapsedMilliseconds}ms, expected < 3000ms");
    }

    [Fact]
    public async Task Performance_RoundTrip_1000Records_CompletesQuickly()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = Enumerable.Range(1, 1000)
            .Select(i => new TestPerson { Id = i, Name = $"Person{i}", Age = 20 + i % 50 })
            .ToList();

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

        var stopwatch = Stopwatch.StartNew();

        // Act - Read from input
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);

        // Write to output
        var sinkConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        // Read back from output
        var outputSourceNode = new JsonSourceNode<TestPerson>(_provider, sinkUri, sourceConfig);
        var outputPipe = outputSourceNode.Initialize(_context, CancellationToken.None);
        var outputData = await outputPipe.ToListAsync(CancellationToken.None);

        stopwatch.Stop();

        // Assert
        Assert.Equal(1000, outputData.Count);
        _output.WriteLine($"Round-trip of 1000 records took {stopwatch.ElapsedMilliseconds}ms");
        Assert.True(stopwatch.ElapsedMilliseconds < 1000, $"Round-trip took {stopwatch.ElapsedMilliseconds}ms, expected < 1000ms");
    }

    [Fact]
    public async Task Performance_BufferSize_8192_HandlesLargeFiles()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = Enumerable.Range(1, 5000)
            .Select(i => new TestPerson { Id = i, Name = $"Person{i}", Age = 20 + i % 50 })
            .ToList();

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

        var stopwatch = Stopwatch.StartNew();

        // Act - Read from input with custom buffer size
        var sourceConfig = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            BufferSize = 8192,
        };

        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);

        // Write to output
        var sinkConfig = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            BufferSize = 8192,
        };

        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        stopwatch.Stop();

        // Assert
        _output.WriteLine($"Processing with 8192 buffer took {stopwatch.ElapsedMilliseconds}ms");
        Assert.True(stopwatch.ElapsedMilliseconds < 3000, $"Processing took {stopwatch.ElapsedMilliseconds}ms, expected < 3000ms");
    }

    [Fact]
    public async Task Performance_ConcurrentReads_MultipleFiles_HandlesLoad()
    {
        // Arrange
        var files = new List<string>();

        for (var i = 0; i < 5; i++)
        {
            var inputFile = Path.Combine(_tempDirectory, $"input{i}.json");

            var inputData = Enumerable.Range(1, 1000)
                .Select(j => new TestPerson { Id = j, Name = $"Person{j}", Age = 20 + j % 50 })
                .ToList();

            var jsonData = JsonSerializer.Serialize(inputData);
            await File.WriteAllTextAsync(inputFile, jsonData);
            files.Add(inputFile);
        }

        var stopwatch = Stopwatch.StartNew();

        // Act - Read from multiple files concurrently
        var tasks = files.Select(async file =>
        {
            var sourceConfig = new JsonConfiguration { Format = JsonFormat.Array };
            var sourceUri = StorageUri.FromFilePath(file);
            var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
            var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);
            return await sourcePipe.ToListAsync(CancellationToken.None);
        });

        var results = await Task.WhenAll(tasks);

        stopwatch.Stop();

        // Assert
        Assert.Equal(5, results.Length);

        foreach (var result in results)
        {
            Assert.Equal(1000, result.Count);
        }

        _output.WriteLine($"Concurrent reads of 5 files took {stopwatch.ElapsedMilliseconds}ms");
        Assert.True(stopwatch.ElapsedMilliseconds < 3000, $"Concurrent reads took {stopwatch.ElapsedMilliseconds}ms, expected < 3000ms");
    }

    [Fact]
    public async Task Performance_IndentedFormat_Overhead_IsAcceptable()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = Enumerable.Range(1, 1000)
            .Select(i => new TestPerson { Id = i, Name = $"Person{i}", Age = 20 + i % 50 })
            .ToList();

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

        var stopwatch = Stopwatch.StartNew();

        // Act - Read from input
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);

        // Write to output with indentation
        var sinkConfig = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            WriteIndented = true,
        };

        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        stopwatch.Stop();

        // Assert
        _output.WriteLine($"Indented format processing of 1000 records took {stopwatch.ElapsedMilliseconds}ms");
        Assert.True(stopwatch.ElapsedMilliseconds < 2000, $"Indented processing took {stopwatch.ElapsedMilliseconds}ms, expected < 2000ms");
    }

    [Fact]
    public async Task Performance_ComplexNestedData_HandlesEfficiently()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = Enumerable.Range(1, 500)
            .Select(i => new TestNestedPerson
            {
                Id = i,
                Name = $"Person{i}",
                Address = new TestAddress
                {
                    Street = $"{i} Main St",
                    City = "City",
                    ZipCode = "12345",
                },
                Contacts = Enumerable.Range(1, 3).Select(j => new TestContact
                {
                    Type = $"Type{j}",
                    Value = $"Contact{j}",
                }).ToArray(),
            })
            .ToList();

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

        var stopwatch = Stopwatch.StartNew();

        // Act - Read from input
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestNestedPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);

        // Write to output
        var sinkConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestNestedPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        stopwatch.Stop();

        // Assert
        _output.WriteLine($"Complex nested data processing of 500 records took {stopwatch.ElapsedMilliseconds}ms");
        Assert.True(stopwatch.ElapsedMilliseconds < 3000, $"Complex processing took {stopwatch.ElapsedMilliseconds}ms, expected < 3000ms");
    }

    private sealed class TestPerson
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public int Age { get; set; }
    }

    private sealed class TestNestedPerson
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public TestAddress? Address { get; set; }
        public TestContact[]? Contacts { get; set; }
    }

    private sealed class TestAddress
    {
        public string? Street { get; set; }
        public string? City { get; set; }
        public string? ZipCode { get; set; }
    }

    private sealed class TestContact
    {
        public string? Type { get; set; }
        public string? Value { get; set; }
    }
}
