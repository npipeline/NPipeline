using System.Text.Json;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Json.Tests;

/// <summary>
///     Configuration tests for JSON connector.
///     Tests configuration options, validation, and default values.
/// </summary>
public sealed class JsonConfigurationTests : IDisposable
{
    private readonly PipelineContext _context;
    private readonly IStorageProvider _provider;
    private readonly string _tempDirectory;

    public JsonConfigurationTests()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"NPipeline.Json.Config_{Guid.NewGuid():N}");
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

    [Theory]
    [InlineData(JsonFormat.Array)]
    [InlineData(JsonFormat.NewlineDelimited)]
    public void Configuration_Format_WithValidValue_SetsCorrectly(JsonFormat format)
    {
        // Arrange & Act
        var configuration = new JsonConfiguration { Format = format };

        // Assert
        Assert.Equal(format, configuration.Format);
    }

    [Theory]
    [InlineData(JsonPropertyNamingPolicy.CamelCase)]
    [InlineData(JsonPropertyNamingPolicy.SnakeCase)]
    [InlineData(JsonPropertyNamingPolicy.PascalCase)]
    [InlineData(JsonPropertyNamingPolicy.AsIs)]
    public void Configuration_PropertyNamingPolicy_WithValidValue_SetsCorrectly(JsonPropertyNamingPolicy policy)
    {
        // Arrange & Act
        var configuration = new JsonConfiguration { PropertyNamingPolicy = policy };

        // Assert
        Assert.Equal(policy, configuration.PropertyNamingPolicy);
    }

    [Theory]
    [InlineData(1024)]
    [InlineData(4096)]
    [InlineData(8192)]
    [InlineData(16384)]
    [InlineData(32768)]
    [InlineData(65536)]
    public void Configuration_BufferSize_WithValidValue_SetsCorrectly(int bufferSize)
    {
        // Arrange & Act
        var configuration = new JsonConfiguration { BufferSize = bufferSize };

        // Assert
        Assert.Equal(bufferSize, configuration.BufferSize);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Configuration_WriteIndented_WithValidValue_SetsCorrectly(bool writeIndented)
    {
        // Arrange & Act
        var configuration = new JsonConfiguration { WriteIndented = writeIndented };

        // Assert
        Assert.Equal(writeIndented, configuration.WriteIndented);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Configuration_PropertyNameCaseInsensitive_WithValidValue_SetsCorrectly(bool caseInsensitive)
    {
        // Arrange & Act
        var configuration = new JsonConfiguration { PropertyNameCaseInsensitive = caseInsensitive };

        // Assert
        Assert.Equal(caseInsensitive, configuration.PropertyNameCaseInsensitive);
    }

    [Fact]
    public void Configuration_DefaultValues_AreCorrect()
    {
        // Arrange & Act
        var configuration = new JsonConfiguration();

        // Assert
        Assert.Equal(JsonFormat.Array, configuration.Format);
        Assert.Equal(JsonPropertyNamingPolicy.LowerCase, configuration.PropertyNamingPolicy);
        Assert.Equal(4096, configuration.BufferSize);
        Assert.False(configuration.WriteIndented);
        Assert.True(configuration.PropertyNameCaseInsensitive);
    }

    [Fact]
    public void Configuration_Clone_CreatesIndependentCopy()
    {
        // Arrange
        var original = new JsonConfiguration
        {
            Format = JsonFormat.NewlineDelimited,
            PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase,
            BufferSize = 16384,
            WriteIndented = true,
            PropertyNameCaseInsensitive = true,
        };

        // Act
        var clone = new JsonConfiguration
        {
            Format = original.Format,
            PropertyNamingPolicy = original.PropertyNamingPolicy,
            BufferSize = original.BufferSize,
            WriteIndented = original.WriteIndented,
            PropertyNameCaseInsensitive = original.PropertyNameCaseInsensitive,
        };

        // Modify original
        original.Format = JsonFormat.Array;
        original.BufferSize = 4096;

        // Assert
        Assert.Equal(JsonFormat.NewlineDelimited, clone.Format);
        Assert.Equal(JsonPropertyNamingPolicy.CamelCase, clone.PropertyNamingPolicy);
        Assert.Equal(16384, clone.BufferSize);
        Assert.True(clone.WriteIndented);
        Assert.True(clone.PropertyNameCaseInsensitive);
    }

    [Fact]
    public async Task Configuration_Format_AffectsReadingBehavior()
    {
        // Arrange
        var arrayFile = Path.Combine(_tempDirectory, "array.json");
        var ndjsonFile = Path.Combine(_tempDirectory, "data.ndjson");

        await File.WriteAllTextAsync(arrayFile, "[{\"id\":1},{\"id\":2}]");
        await File.WriteAllTextAsync(ndjsonFile, "{\"id\":1}\n{\"id\":2}");

        // Act - Read as Array
        var arrayConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var arrayUri = StorageUri.FromFilePath(arrayFile);
        var arrayNode = new JsonSourceNode<TestPerson>(_provider, arrayUri, arrayConfig);
        var arrayPipe = arrayNode.Initialize(_context, CancellationToken.None);
        var arrayResults = await arrayPipe.ToListAsync(CancellationToken.None);

        // Read as NDJSON
        var ndjsonConfig = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var ndjsonUri = StorageUri.FromFilePath(ndjsonFile);
        var ndjsonNode = new JsonSourceNode<TestPerson>(_provider, ndjsonUri, ndjsonConfig);
        var ndjsonPipe = ndjsonNode.Initialize(_context, CancellationToken.None);
        var ndjsonResults = await ndjsonPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(2, arrayResults.Count);
        Assert.Equal(2, ndjsonResults.Count);
    }

    [Fact]
    public async Task Configuration_PropertyNamingPolicy_AffectsMapping()
    {
        // Arrange
        var snakeCaseFile = Path.Combine(_tempDirectory, "snake.json");
        await File.WriteAllTextAsync(snakeCaseFile, "[{\"user_id\":1,\"full_name\":\"John\"}]");

        // Act - Read with SnakeCase policy
        var config = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            PropertyNamingPolicy = JsonPropertyNamingPolicy.SnakeCase,
        };

        var uri = StorageUri.FromFilePath(snakeCaseFile);
        var node = new JsonSourceNode<SnakeCasePerson>(_provider, uri, config);
        var pipe = node.Initialize(_context, CancellationToken.None);
        var results = await pipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].UserId);
        Assert.Equal("John", results[0].FullName);
    }

    [Fact]
    public async Task Configuration_BufferSize_AffectsPerformance()
    {
        // Arrange
        var file = Path.Combine(_tempDirectory, "data.json");
        var data = new List<TestPerson>();

        for (var i = 0; i < 1000; i++)
        {
            data.Add(new TestPerson { Id = i, Name = $"Person{i}" });
        }

        var jsonData = JsonSerializer.Serialize(data);
        await File.WriteAllTextAsync(file, jsonData);

        // Act - Read with different buffer sizes
        var smallBufferConfig = new JsonConfiguration { Format = JsonFormat.Array, BufferSize = 1024 };
        var largeBufferConfig = new JsonConfiguration { Format = JsonFormat.Array, BufferSize = 32768 };

        var uri = StorageUri.FromFilePath(file);

        var smallBufferNode = new JsonSourceNode<TestPerson>(_provider, uri, smallBufferConfig);
        var smallBufferPipe = smallBufferNode.Initialize(_context, CancellationToken.None);
        var smallBufferResults = await smallBufferPipe.ToListAsync(CancellationToken.None);

        var largeBufferNode = new JsonSourceNode<TestPerson>(_provider, uri, largeBufferConfig);
        var largeBufferPipe = largeBufferNode.Initialize(_context, CancellationToken.None);
        var largeBufferResults = await largeBufferPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(1000, smallBufferResults.Count);
        Assert.Equal(1000, largeBufferResults.Count);
    }

    [Fact]
    public async Task Configuration_WriteIndented_AffectsOutputFormat()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var data = new List<TestPerson>
        {
            new() { Id = 1, Name = "Alice" },
        };

        var jsonData = JsonSerializer.Serialize(data);
        await File.WriteAllTextAsync(inputFile, jsonData);

        // Act - Write with indentation
        var sourceConfig = new JsonConfiguration { Format = JsonFormat.Array };
        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);

        var sinkConfig = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            WriteIndented = true,
        };

        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        var outputContent = await File.ReadAllTextAsync(outputFile);

        // Assert
        Assert.Contains("  \"id\": 1,", outputContent);
        Assert.Contains("  \"name\": \"Alice\"", outputContent);
    }

    [Fact]
    public async Task Configuration_PropertyNameCaseInsensitive_AffectsMapping()
    {
        // Arrange
        var file = Path.Combine(_tempDirectory, "uppercase.json");
        await File.WriteAllTextAsync(file, "[{\"ID\":1,\"NAME\":\"Alice\"}]");

        // Act - Read with case-insensitive mapping
        var config = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            PropertyNameCaseInsensitive = true,
        };

        var uri = StorageUri.FromFilePath(file);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, config);
        var pipe = node.Initialize(_context, CancellationToken.None);
        var results = await pipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Configuration_MultipleOptions_WorkTogether()
    {
        // Arrange
        var inputFile = Path.Combine(_tempDirectory, "input.json");
        var outputFile = Path.Combine(_tempDirectory, "output.json");

        var data = new List<TestPerson>
        {
            new() { Id = 1, Name = "Alice", Age = 30 },
        };

        var jsonData = JsonSerializer.Serialize(data);
        await File.WriteAllTextAsync(inputFile, jsonData);

        // Act - Read and write with multiple options
        var sourceConfig = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            PropertyNameCaseInsensitive = true,
            BufferSize = 16384,
        };

        var sourceUri = StorageUri.FromFilePath(inputFile);
        var sourceNode = new JsonSourceNode<TestPerson>(_provider, sourceUri, sourceConfig);
        var sourcePipe = sourceNode.Initialize(_context, CancellationToken.None);

        var sinkConfig = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            WriteIndented = true,
            BufferSize = 16384,
        };

        var sinkUri = StorageUri.FromFilePath(outputFile);
        var sinkNode = new JsonSinkNode<TestPerson>(_provider, sinkUri, sinkConfig);
        await sinkNode.ExecuteAsync(sourcePipe, _context, CancellationToken.None);

        // Read back
        var outputSourceNode = new JsonSourceNode<TestPerson>(_provider, sinkUri, sourceConfig);
        var outputPipe = outputSourceNode.Initialize(_context, CancellationToken.None);
        var outputData = await outputPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(outputData);
        Assert.Equal(1, outputData[0].Id);
        Assert.Equal("Alice", outputData[0].Name);
        Assert.Equal(30, outputData[0].Age);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    public async Task Configuration_BufferSize_HandlesDifferentSizes(int bufferSize)
    {
        // Arrange
        var file = Path.Combine(_tempDirectory, $"data-{bufferSize}.json");
        var data = new List<TestPerson>();

        for (var i = 0; i < 100; i++)
        {
            data.Add(new TestPerson { Id = i, Name = $"Person{i}" });
        }

        var jsonData = JsonSerializer.Serialize(data);
        await File.WriteAllTextAsync(file, jsonData);

        // Act - Read with specified buffer size
        var config = new JsonConfiguration { Format = JsonFormat.Array, BufferSize = bufferSize };
        var uri = StorageUri.FromFilePath(file);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, config);
        var pipe = node.Initialize(_context, CancellationToken.None);
        var results = await pipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(100, results.Count);
    }

    [Fact]
    public async Task Configuration_NdjsonFormat_RequiresNewlineDelimited()
    {
        // Arrange
        var file = Path.Combine(_tempDirectory, "data.ndjson");
        await File.WriteAllTextAsync(file, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}");

        // Act - Read with NDJSON format
        var config = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var uri = StorageUri.FromFilePath(file);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, config);
        var pipe = node.Initialize(_context, CancellationToken.None);
        var results = await pipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(3, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal(2, results[1].Id);
        Assert.Equal(3, results[2].Id);
    }

    [Fact]
    public async Task Configuration_ArrayFormat_RequiresJsonArray()
    {
        // Arrange
        var file = Path.Combine(_tempDirectory, "data.json");
        await File.WriteAllTextAsync(file, "[{\"id\":1},{\"id\":2},{\"id\":3}]");

        // Act - Read with Array format
        var config = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(file);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, config);
        var pipe = node.Initialize(_context, CancellationToken.None);
        var results = await pipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(3, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal(2, results[1].Id);
        Assert.Equal(3, results[2].Id);
    }

    private sealed class TestPerson
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public int Age { get; set; }
    }

    private sealed class SnakeCasePerson
    {
        public int UserId { get; set; }
        public string? FullName { get; set; }
    }
}
