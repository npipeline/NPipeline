using System.Text.Json;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Json.Tests;

/// <summary>
///     Comprehensive unit tests for JsonSourceNode.
///     Tests reading JSON arrays, NDJSON files, attribute-based mapping, manual mapper functions,
///     error handling (malformed JSON, missing properties), cancellation token propagation,
///     case sensitivity, naming policies, storage providers, null values, and nested properties.
/// </summary>
public class JsonSourceNodeTests : IDisposable
{
    private readonly PipelineContext _context;
    private readonly IStorageProvider _provider;
    private readonly string _tempDirectory;

    public JsonSourceNodeTests()
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
    public async Task Initialize_WithJsonArray_ReturnsCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var jsonData = "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]";
        await File.WriteAllTextAsync(jsonFile, jsonData);

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<Customer>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(2, results[1].Id);
        Assert.Equal("Bob", results[1].Name);
    }

    [Fact]
    public async Task Initialize_WithNdjson_ReturnsCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.ndjson");
        var jsonData = "{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}";
        await File.WriteAllTextAsync(jsonFile, jsonData);

        var configuration = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<Customer>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(2, results[1].Id);
        Assert.Equal("Bob", results[1].Name);
    }

    [Fact]
    public async Task Initialize_WithManualMapper_ReturnsCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var jsonData = "[{\"id\":1,\"name\":\"Alice\"}]";
        await File.WriteAllTextAsync(jsonFile, jsonData);

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);

        var node = new JsonSourceNode<Customer>(_provider, uri, row => new Customer
        {
            Id = row.Get<int>("id"),
            Name = row.Get<string>("name") ?? string.Empty,
        });

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Initialize_WithMalformedJson_ThrowsJsonException()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "invalid.json");
        await File.WriteAllTextAsync(jsonFile, "[{\"id\":1},{\"id\":2,\"name\":\"test\""); // Missing closing bracket

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<Customer>(_provider, uri, configuration);

        // Act & Assert
        await Assert.ThrowsAnyAsync<JsonException>(async () =>
        {
            var dataPipe = node.Initialize(_context, CancellationToken.None);
            await dataPipe.ToListAsync();
        });
    }

    [Fact]
    public async Task Initialize_WithMissingProperty_DoesNotThrow()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var jsonData = "[{\"id\":1}]";
        await File.WriteAllTextAsync(jsonFile, jsonData);

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<Customer>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Null(results[0].Name);
    }

    [Fact]
    public async Task Initialize_WithCancellation_CancelsOperation()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var jsonData = "[{\"id\":1},{\"id\":2},{\"id\":3}]";
        await File.WriteAllTextAsync(jsonFile, jsonData);

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<Customer>(_provider, uri, configuration);

        var cts = new CancellationTokenSource();

        // Act & Assert
        var dataPipe = node.Initialize(_context, cts.Token);
        var results = new List<Customer>();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var item in dataPipe.WithCancellation(cts.Token))
            {
                results.Add(item);

                if (results.Count == 1)
                    cts.Cancel();
            }
        });

        Assert.Single(results);
    }

    [Fact]
    public async Task Initialize_WithCaseInsensitive_ReturnsCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var jsonData = "[{\"ID\":1,\"NAME\":\"Alice\"}]";
        await File.WriteAllTextAsync(jsonFile, jsonData);

        var configuration = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            PropertyNameCaseInsensitive = true,
        };

        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<Customer>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Initialize_WithSnakeCaseNamingPolicy_ReturnsCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "snake_case.json");
        await File.WriteAllTextAsync(jsonFile, "[{\"user_id\":1,\"full_name\":\"John Doe\"}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array, PropertyNamingPolicy = JsonPropertyNamingPolicy.SnakeCase };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<SnakeCaseCustomer>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var items = await dataPipe.ToListAsync();

        // Assert
        Assert.Single(items);
        Assert.Equal(1, items[0].UserId);
        Assert.Equal("John Doe", items[0].FullName);
    }

    [Fact]
    public async Task Initialize_WithStorageProvider_ReturnsCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var jsonData = "[{\"id\":1,\"name\":\"Alice\"}]";
        await File.WriteAllTextAsync(jsonFile, jsonData);

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<Customer>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Initialize_WithNullValues_ReturnsCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var jsonData = "[{\"id\":1,\"name\":null}]";
        await File.WriteAllTextAsync(jsonFile, jsonData);

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<Customer>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Null(results[0].Name);
    }

    [Fact]
    public async Task Initialize_WithNestedProperties_ReturnsCorrectData()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "test.json");
        var jsonData = "[{\"id\":1,\"address\":{\"street\":\"Main St\",\"city\":\"NYC\"}}]";
        await File.WriteAllTextAsync(jsonFile, jsonData);

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);

        var node = new JsonSourceNode<CustomerWithAddress>(_provider, uri, row => new CustomerWithAddress
        {
            Id = row.Get<int>("id"),
            Address = row.GetNested<JsonRow>("address"),
        });

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.NotNull(results[0].Address);
    }

    public class Customer
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    public class CustomerWithAddress
    {
        public int Id { get; set; }
        public JsonRow? Address { get; set; }
    }

    private sealed class TestCustomer
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
    }

    private sealed class SnakeCaseCustomer
    {
        public int UserId { get; set; }
        public string FullName { get; set; } = string.Empty;
    }
}
