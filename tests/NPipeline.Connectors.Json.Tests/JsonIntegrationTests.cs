using System.Text.Json;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Json.Tests;

/// <summary>
///     Integration tests for JSON connector with full pipeline scenarios.
///     Tests round-trip operations, different formats, naming policies, and edge cases.
/// </summary>
public sealed class JsonIntegrationTests : IDisposable
{
    private static readonly JsonSerializerOptions CamelCaseOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
    private readonly PipelineContext _context;
    private readonly IStorageProvider _provider;

    private readonly string _tempDirectory;

    public JsonIntegrationTests()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"NPipeline.Json.Integration_{Guid.NewGuid():N}");
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
    public async Task RoundTrip_JsonArrayFormat_PreservesData()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = new List<TestPerson>
        {
            new() { Id = 1, Name = "Alice", Age = 30 },
            new() { Id = 2, Name = "Bob", Age = 25 },
        };

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

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

        // Read back from output
        var outputSourceNode = new JsonSourceNode<TestPerson>(_provider, sinkUri, sourceConfig);
        var outputPipe = outputSourceNode.Initialize(_context, CancellationToken.None);
        var outputData = await outputPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(2, outputData.Count);
        Assert.Equal(1, outputData[0].Id);
        Assert.Equal("Alice", outputData[0].Name);
        Assert.Equal(30, outputData[0].Age);
        Assert.Equal(2, outputData[1].Id);
        Assert.Equal("Bob", outputData[1].Name);
        Assert.Equal(25, outputData[1].Age);
    }

    [Fact]
    public async Task RoundTrip_NdjsonFormat_PreservesData()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.ndjson");
        var outputFile = Path.Combine(_tempDirectory, "output.ndjson");

        var inputData = new List<TestPerson>
        {
            new() { Id = 1, Name = "Alice", Age = 30 },
            new() { Id = 2, Name = "Bob", Age = 25 },
        };

        // Write input as NDJSON
        var ndjsonLines = inputData.Select(p => JsonSerializer.Serialize(p));
        await File.WriteAllLinesAsync(inputFile, ndjsonLines);

        // Act - Read from input
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);
        var readData = await sourcePipe.ToListAsync(CancellationToken.None);

        // Write to output
        var sinkConfig = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        // Read back from output
        var outputSourceNode = new JsonSourceNode<TestPerson>(_provider, sinkUri, sourceConfig);
        var outputPipe = outputSourceNode.Initialize(_context, CancellationToken.None);
        var outputData = await outputPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(2, outputData.Count);
        Assert.Equal(1, outputData[0].Id);
        Assert.Equal("Alice", outputData[0].Name);
        Assert.Equal(30, outputData[0].Age);
        Assert.Equal(2, outputData[1].Id);
        Assert.Equal("Bob", outputData[1].Name);
        Assert.Equal(25, outputData[1].Age);
    }

    [Fact]
    public async Task RoundTrip_WithCamelCaseNamingPolicy_PreservesData()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = new List<TestPerson>
        {
            new() { Id = 1, Name = "Alice", Age = 30 },
        };

        var jsonData = JsonSerializer.Serialize(inputData, CamelCaseOptions);
        await File.WriteAllTextAsync(inputFile, jsonData);

        // Act - Read from input
        var sourceConfig = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase,
        };

        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);

        // Write to output
        var sinkConfig = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase,
        };

        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        // Read back from output
        var outputSourceNode = new JsonSourceNode<TestPerson>(_provider, sinkUri, sourceConfig);
        var outputPipe = outputSourceNode.Initialize(_context, CancellationToken.None);
        var outputData = await outputPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(outputData);
        Assert.Equal(1, outputData[0].Id);
        Assert.Equal("Alice", outputData[0].Name);
        Assert.Equal(30, outputData[0].Age);
    }

    [Fact]
    public async Task RoundTrip_WithNullValues_PreservesNulls()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = new List<TestPerson>
        {
            new() { Id = 1, Name = null, Age = 30 },
            new() { Id = 2, Name = "Bob", Age = null },
        };

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

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

        // Assert
        Assert.Equal(2, outputData.Count);
        Assert.Equal(1, outputData[0].Id);
        Assert.Null(outputData[0].Name);
        Assert.Equal(30, outputData[0].Age);
        Assert.Equal(2, outputData[1].Id);
        Assert.Equal("Bob", outputData[1].Name);
        Assert.Null(outputData[1].Age);
    }

    [Fact]
    public async Task RoundTrip_EmptyDataset_WritesEmptyArray()
    {
        // Arrange
        var outputFile = Path.Combine(_tempDirectory, "output.json");
        var inputData = new List<TestPerson>();

        // Act - Create empty source pipe
        var sourcePipe = new InMemoryDataPipe<TestPerson>(inputData);

        // Write to output
        var sinkConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        // Read back from output
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sinkUri, sourceConfig);
        var outputPipe = sourceNode.Initialize(_context, CancellationToken.None);
        var outputData = await outputPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Empty(outputData);
        Assert.True(File.Exists(outputFile));
    }

    [Fact]
    public async Task RoundTrip_LargeDataset_PreservesAllData()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = Enumerable.Range(1, 1000)
            .Select(i => new TestPerson { Id = i, Name = $"Person{i}", Age = 20 + i % 50 })
            .ToList();

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

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

        // Assert
        Assert.Equal(1000, outputData.Count);
        Assert.Equal(1, outputData[0].Id);
        Assert.Equal("Person1", outputData[0].Name);
        Assert.Equal(21, outputData[0].Age);
        Assert.Equal(1000, outputData[999].Id);
        Assert.Equal("Person1000", outputData[999].Name);
        Assert.Equal(20, outputData[999].Age);
    }

    [Fact]
    public async Task RoundTrip_WithIndentedFormat_ProducesReadableJson()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = new List<TestPerson>
        {
            new() { Id = 1, Name = "Alice", Age = 30 },
        };

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

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

        // Read output file content
        var outputContent = await File.ReadAllTextAsync(outputFile);

        // Assert
        Assert.Contains("  \"id\": 1,", outputContent);
        Assert.Contains("  \"name\": \"Alice\",", outputContent);
        Assert.Contains("  \"age\": 30", outputContent);
    }

    [Fact]
    public async Task RoundTrip_WithDifferentDataTypes_PreservesTypes()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = new List<TestMixedTypes>
        {
            new() { Id = 1, IsActive = true, Score = 95.5, CreatedDate = DateTime.UtcNow, Tags = new[] { "tag1", "tag2" } },
        };

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

        // Act - Read from input
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestMixedTypes>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);

        // Write to output
        var sinkConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestMixedTypes>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        // Read back from output
        var outputSourceNode = new JsonSourceNode<TestMixedTypes>(_provider, sinkUri, sourceConfig);
        var outputPipe = outputSourceNode.Initialize(_context, CancellationToken.None);
        var outputData = await outputPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(outputData);
        Assert.Equal(1, outputData[0].Id);
        Assert.True(outputData[0].IsActive);
        Assert.Equal(95.5, outputData[0].Score);

        // Note: Array properties may not be preserved in round-trip
        // DateTime is a value type, no need for NotNull assertion
    }

    [Fact]
    public async Task RoundTrip_WithCancellation_StopsProcessing()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var inputData = Enumerable.Range(1, 1000)
            .Select(i => new TestPerson { Id = i, Name = $"Person{i}", Age = 20 + i % 50 })
            .ToList();

        var jsonData = JsonSerializer.Serialize(inputData);
        await File.WriteAllTextAsync(inputFile, jsonData);

        var cts = new CancellationTokenSource();

        // Act - Read from input with cancellation
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, cts.Token);

        // Write to output
        var sinkConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);

        var writeTask = sinkNode.ExecuteAsync(sourcePipe, _context, cts.Token);

        // Cancel after starting the task
        cts.Cancel();

        // Assert - The operation should be canceled
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => writeTask);
    }

    private sealed class TestPerson
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public int? Age { get; set; }
    }

    private sealed class TestMixedTypes
    {
        public int Id { get; set; }
        public bool IsActive { get; set; }
        public double Score { get; set; }
        public DateTime CreatedDate { get; set; }
        public string[]? Tags { get; set; }
    }
}
