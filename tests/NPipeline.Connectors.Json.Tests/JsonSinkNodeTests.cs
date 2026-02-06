using System.Text.Json;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Json.Tests;

/// <summary>
///     Comprehensive unit tests for JsonSinkNode.
///     Tests writing JSON arrays, NDJSON files, indented output, naming policies, null values,
///     cancellation, storage providers, data types, large datasets, buffer sizes, and file overwrite.
/// </summary>
public class JsonSinkNodeTests : IDisposable
{
    private readonly PipelineContext _context;
    private readonly IStorageProvider _provider;
    private readonly string _tempDirectory;

    public JsonSinkNodeTests()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"NPipeline.Json.Tests_{Guid.NewGuid():N}");
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
    public async Task ExecuteAsync_WithJsonArray_WritesCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<Customer>(_provider, uri, configuration);

        var items = new List<Customer>
        {
            new() { Id = 1, Name = "Alice" },
            new() { Id = 2, Name = "Bob" },
        };

        var dataPipe = new InMemoryDataPipe<Customer>(items);

        // Act
        await node.ExecuteAsync(dataPipe, _context, CancellationToken.None);

        // Assert
        var content = await File.ReadAllTextAsync(jsonFile);
        var jsonDoc = JsonDocument.Parse(content);
        var array = jsonDoc.RootElement.EnumerateArray().ToArray();
        Assert.Equal(2, array.Length);
        Assert.Equal(1, array[0].GetProperty("id").GetInt32());
        Assert.Equal("Alice", array[0].GetProperty("name").GetString());
        Assert.Equal(2, array[1].GetProperty("id").GetInt32());
        Assert.Equal("Bob", array[1].GetProperty("name").GetString());
    }

    [Fact]
    public async Task ExecuteAsync_WithNdjson_WritesCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.ndjson");
        var configuration = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<Customer>(_provider, uri, configuration);

        var items = new List<Customer>
        {
            new() { Id = 1, Name = "Alice" },
            new() { Id = 2, Name = "Bob" },
        };

        var dataPipe = new InMemoryDataPipe<Customer>(items);

        // Act
        await node.ExecuteAsync(dataPipe, _context, CancellationToken.None);

        // Assert
        var lines = await File.ReadAllLinesAsync(jsonFile);
        Assert.Equal(2, lines.Length);
        var jsonDoc1 = JsonDocument.Parse(lines[0]);
        var jsonDoc2 = JsonDocument.Parse(lines[1]);
        Assert.Equal(1, jsonDoc1.RootElement.GetProperty("id").GetInt32());
        Assert.Equal("Alice", jsonDoc1.RootElement.GetProperty("name").GetString());
        Assert.Equal(2, jsonDoc2.RootElement.GetProperty("id").GetInt32());
        Assert.Equal("Bob", jsonDoc2.RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public async Task ExecuteAsync_WithIndentedOutput_WritesFormattedJson()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");

        var configuration = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            WriteIndented = true,
        };

        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<Customer>(_provider, uri, configuration);

        var items = new List<Customer>
        {
            new() { Id = 1, Name = "Alice" },
        };

        var dataPipe = new InMemoryDataPipe<Customer>(items);

        // Act
        await node.ExecuteAsync(dataPipe, _context, CancellationToken.None);

        // Assert
        var content = await File.ReadAllTextAsync(jsonFile);
        Assert.Contains("  \"id\": 1", content);
        Assert.Contains("  \"name\": \"Alice\"", content);
    }

    [Fact]
    public async Task ExecuteAsync_WithSnakeCaseNamingPolicy_WritesSnakeCaseProperties()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");

        var configuration = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            PropertyNamingPolicy = JsonPropertyNamingPolicy.SnakeCase,
        };

        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<Customer>(_provider, uri, configuration);

        var items = new List<Customer>
        {
            new() { Id = 1, Name = "Alice" },
        };

        var dataPipe = new InMemoryDataPipe<Customer>(items);

        // Act
        await node.ExecuteAsync(dataPipe, _context, CancellationToken.None);

        // Assert
        var content = await File.ReadAllTextAsync(jsonFile);
        Assert.Contains("\"id\"", content);
        Assert.Contains("\"name\"", content);
    }

    [Fact]
    public async Task ExecuteAsync_WithNullValues_WritesNullValues()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<Customer>(_provider, uri, configuration);

        var items = new List<Customer>
        {
            new() { Id = 1, Name = null },
        };

        var dataPipe = new InMemoryDataPipe<Customer>(items);

        // Act
        await node.ExecuteAsync(dataPipe, _context, CancellationToken.None);

        // Assert
        var content = await File.ReadAllTextAsync(jsonFile);
        Assert.Contains("\"name\":null", content);
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellation_CancelsOperation()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<Customer>(_provider, uri, configuration);

        var items = new List<Customer>
        {
            new() { Id = 1, Name = "Alice" },
            new() { Id = 2, Name = "Bob" },
        };

        var dataPipe = new InMemoryDataPipe<Customer>(items);

        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            node.ExecuteAsync(dataPipe, _context, cts.Token));
    }

    [Fact]
    public async Task ExecuteAsync_WithStorageProvider_WritesCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<Customer>(_provider, uri, configuration);

        var items = new List<Customer>
        {
            new() { Id = 1, Name = "Alice" },
        };

        var dataPipe = new InMemoryDataPipe<Customer>(items);

        // Act
        await node.ExecuteAsync(dataPipe, _context, CancellationToken.None);

        // Assert
        var content = await File.ReadAllTextAsync(jsonFile);
        Assert.Contains("\"id\":1", content);
        Assert.Contains("\"name\":\"Alice\"", content);
    }

    [Fact]
    public async Task ExecuteAsync_WithDifferentTypes_WritesCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<ComplexCustomer>(_provider, uri, configuration);

        var items = new List<ComplexCustomer>
        {
            new()
            {
                Id = 1,
                Age = 30,
                Balance = 100.50m,
                IsActive = true,
            },
        };

        var dataPipe = new InMemoryDataPipe<ComplexCustomer>(items);

        // Act
        await node.ExecuteAsync(dataPipe, _context, CancellationToken.None);

        // Assert
        var content = await File.ReadAllTextAsync(jsonFile);
        Assert.Contains("\"id\":1", content);
        Assert.Contains("\"age\":30", content);
        Assert.Contains("\"balance\":100.50", content);
        Assert.Contains("\"isactive\":true", content);
    }

    [Fact]
    public async Task ExecuteAsync_WithLargeDataset_WritesAllData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<Customer>(_provider, uri, configuration);

        var items = new List<Customer>();

        for (var i = 0; i < 1000; i++)
        {
            items.Add(new Customer { Id = i, Name = $"Customer{i}" });
        }

        var dataPipe = new InMemoryDataPipe<Customer>(items);

        // Act
        await node.ExecuteAsync(dataPipe, _context, CancellationToken.None);

        // Assert
        var content = await File.ReadAllTextAsync(jsonFile);
        var jsonDoc = JsonDocument.Parse(content);
        var array = jsonDoc.RootElement.EnumerateArray().ToArray();
        Assert.Equal(1000, array.Length);
    }

    [Fact]
    public async Task ExecuteAsync_WithBufferSize_WritesCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");

        var configuration = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            BufferSize = 8192,
        };

        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<Customer>(_provider, uri, configuration);

        var items = new List<Customer>
        {
            new() { Id = 1, Name = "Alice" },
        };

        var dataPipe = new InMemoryDataPipe<Customer>(items);

        // Act
        await node.ExecuteAsync(dataPipe, _context, CancellationToken.None);

        // Assert
        var content = await File.ReadAllTextAsync(jsonFile);
        Assert.Contains("\"id\":1", content);
    }

    [Fact]
    public async Task ExecuteAsync_WithFileOverwrite_OverwritesExistingFile()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        await File.WriteAllTextAsync(jsonFile, "[{\"id\":0,\"name\":\"Old\"}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSinkNode<Customer>(_provider, uri, configuration);

        var items = new List<Customer>
        {
            new() { Id = 1, Name = "New" },
        };

        var dataPipe = new InMemoryDataPipe<Customer>(items);

        // Act
        await node.ExecuteAsync(dataPipe, _context, CancellationToken.None);

        // Assert
        var content = await File.ReadAllTextAsync(jsonFile);
        Assert.DoesNotContain("\"id\":0", content);
        Assert.Contains("\"id\":1", content);
    }

    public class Customer
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    public class ComplexCustomer
    {
        public int Id { get; set; }
        public int Age { get; set; }
        public decimal Balance { get; set; }
        public bool IsActive { get; set; }
    }
}
