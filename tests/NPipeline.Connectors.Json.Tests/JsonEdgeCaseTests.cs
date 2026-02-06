using System.Text.Json;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Json.Tests;

/// <summary>
///     Edge case tests for JSON connector operations.
///     Tests extreme values, boundary conditions, and unusual data patterns.
/// </summary>
public sealed class JsonEdgeCaseTests : IDisposable
{
    private readonly PipelineContext _context;
    private readonly IStorageProvider _provider;
    private readonly string _tempDirectory;

    public JsonEdgeCaseTests()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"NPipeline.Json.EdgeCase_{Guid.NewGuid():N}");
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
    public async Task EdgeCase_EmptyArray_ReturnsEmptyList()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "empty.json");
        await File.WriteAllTextAsync(jsonFile, "[]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public async Task EdgeCase_EmptyObject_ReturnsSingleEmptyRecord()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "empty-obj.json");
        await File.WriteAllTextAsync(jsonFile, "[{}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(0, results[0].Id);
        Assert.Null(results[0].Name);
        Assert.Equal(0, results[0].Age);
    }

    [Fact]
    public async Task EdgeCase_MaxIntValue_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "max-int.json");
        await File.WriteAllTextAsync(jsonFile, $"[{{\"id\":{int.MaxValue},\"name\":\"Test\"}}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(int.MaxValue, results[0].Id);
    }

    [Fact]
    public async Task EdgeCase_MinIntValue_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "min-int.json");
        await File.WriteAllTextAsync(jsonFile, $"[{{\"id\":{int.MinValue},\"name\":\"Test\"}}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(int.MinValue, results[0].Id);
    }

    [Fact]
    public async Task EdgeCase_MaxLongValue_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "max-long.json");
        await File.WriteAllTextAsync(jsonFile, $"[{{\"id\":{long.MaxValue},\"name\":\"Test\"}}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPersonLong>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(long.MaxValue, results[0].Id);
    }

    [Fact]
    public async Task EdgeCase_VeryLongString_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "long-string.json");
        var longString = new string('A', 10000);
        await File.WriteAllTextAsync(jsonFile, $"[{{\"id\":1,\"name\":\"{longString}\"}}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal(10000, results[0].Name?.Length);
    }

    [Fact]
    public async Task EdgeCase_SpecialCharacters_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "special-chars.json");
        var specialChars = "Test\"with\\special/chars\t\n\r";
        var data = new List<TestPerson> { new() { Id = 1, Name = specialChars } };
        await File.WriteAllTextAsync(jsonFile, JsonSerializer.Serialize(data));

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal(specialChars, results[0].Name);
    }

    [Fact]
    public async Task EdgeCase_UnicodeCharacters_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "unicode.json");
        var unicode = "Hello世界🌍";
        var data = new List<TestPerson> { new() { Id = 1, Name = unicode } };
        await File.WriteAllTextAsync(jsonFile, JsonSerializer.Serialize(data));

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal(unicode, results[0].Name);
    }

    [Fact]
    public async Task EdgeCase_DeepNesting_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "deep-nesting.json");
        var nestedJson = CreateNestedJson(10);
        await File.WriteAllTextAsync(jsonFile, nestedJson);

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
    }

    [Fact]
    public async Task EdgeCase_NullsInArray_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "nulls-array.json");
        await File.WriteAllTextAsync(jsonFile, "[{\"id\":1,\"name\":null},{\"id\":2,\"name\":\"Test\"},{\"id\":3,\"name\":null}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(3, results.Count);
        Assert.Null(results[0].Name);
        Assert.Equal("Test", results[1].Name);
        Assert.Null(results[2].Name);
    }

    [Fact]
    public async Task EdgeCase_MixedTypesInArray_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "mixed-types.json");
        await File.WriteAllTextAsync(jsonFile, "[{\"id\":1,\"name\":\"Test\"},{\"id\":2},{\"id\":3,\"name\":\"Another\"}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(3, results.Count);
        Assert.Equal("Test", results[0].Name);
        Assert.Null(results[1].Name);
        Assert.Equal("Another", results[2].Name);
    }

    [Fact]
    public async Task EdgeCase_ArrayInArray_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "array-in-array.json");
        await File.WriteAllTextAsync(jsonFile, "[{\"id\":1,\"tags\":[\"a\",\"b\",\"c\"]}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPersonWithTags>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.NotNull(results[0].Tags);
        Assert.Equal(3, results[0].Tags!.Length);
    }

    [Fact]
    public async Task EdgeCase_VeryLargeArray_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "large-array.json");
        var items = new List<string>();

        for (var i = 0; i < 1000; i++)
        {
            items.Add($"{{\"id\":{i},\"name\":\"Person{i}\"}}");
        }

        await File.WriteAllTextAsync(jsonFile, $"[{string.Join(",", items)}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(1000, results.Count);
        Assert.Equal(0, results[0].Id);
        Assert.Equal(999, results[999].Id);
    }

    [Fact]
    public async Task EdgeCase_WhitespaceOnly_ReturnsEmptyList()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "whitespace.json");
        await File.WriteAllTextAsync(jsonFile, "   \n\t   ");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act & Assert
        await Assert.ThrowsAnyAsync<JsonException>(async () =>
        {
            var dataPipe = node.Initialize(_context, CancellationToken.None);
            await dataPipe.ToListAsync(CancellationToken.None);
        });
    }

    [Fact]
    public async Task EdgeCase_InvalidJson_ThrowsException()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "invalid.json");
        await File.WriteAllTextAsync(jsonFile, "{invalid json}");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act & Assert
        await Assert.ThrowsAnyAsync<JsonException>(async () =>
        {
            var dataPipe = node.Initialize(_context, CancellationToken.None);
            await dataPipe.ToListAsync(CancellationToken.None);
        });
    }

    [Fact]
    public async Task EdgeCase_DuplicatePropertyNames_UsesFirstValue()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "duplicate-props.json");
        await File.WriteAllTextAsync(jsonFile, "[{\"id\":1,\"name\":\"First\",\"name\":\"Second\"}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);

        // System.Text.Json uses the first value for duplicate properties
        Assert.Equal("First", results[0].Name);
    }

    [Fact]
    public async Task EdgeCase_EmptyNdjson_ReturnsEmptyList()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "empty.ndjson");
        await File.WriteAllTextAsync(jsonFile, "");

        var configuration = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public async Task EdgeCase_NdjsonWithTrailingNewline_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "trailing-newline.ndjson");
        await File.WriteAllTextAsync(jsonFile, "{\"id\":1,\"name\":\"Test\"}\n");

        var configuration = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Test", results[0].Name);
    }

    [Fact]
    public async Task EdgeCase_NdjsonWithEmptyLines_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "empty-lines.ndjson");
        await File.WriteAllTextAsync(jsonFile, "{\"id\":1,\"name\":\"Test\"}\n\n{\"id\":2,\"name\":\"Another\"}\n");

        var configuration = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal(2, results[1].Id);
    }

    [Fact]
    public async Task EdgeCase_VeryDeepNesting_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "very-deep-nesting.json");
        var nestedJson = CreateNestedJson(20);
        await File.WriteAllTextAsync(jsonFile, nestedJson);

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
    }

    [Fact]
    public async Task EdgeCase_AllNullProperties_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "all-nulls.json");
        await File.WriteAllTextAsync(jsonFile, "[{\"id\":null,\"name\":null,\"age\":null}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(0, results[0].Id);
        Assert.Null(results[0].Name);
        Assert.Equal(0, results[0].Age);
    }

    [Fact]
    public async Task EdgeCase_ZeroAndFalseValues_HandlesCorrectly()
    {
        // Arrange
        var jsonFile = Path.Combine(_tempDirectory, "zero-false.json");
        await File.WriteAllTextAsync(jsonFile, "[{\"id\":0,\"name\":\"\",\"age\":0}]");

        var configuration = new JsonConfiguration { Format = JsonFormat.Array };
        var uri = StorageUri.FromFilePath(jsonFile);
        var node = new JsonSourceNode<TestPerson>(_provider, uri, configuration);

        // Act
        var dataPipe = node.Initialize(_context, CancellationToken.None);
        var results = await dataPipe.ToListAsync(CancellationToken.None);

        // Assert
        Assert.Single(results);
        Assert.Equal(0, results[0].Id);
        Assert.Equal("", results[0].Name);
        Assert.Equal(0, results[0].Age);
    }

    private string CreateNestedJson(int depth)
    {
        if (depth == 0)
            return "[{\"id\":1,\"name\":\"Test\"}]";

        return $"[{{\"id\":1,\"name\":\"Test\",\"nested\":{CreateNestedJson(depth - 1)}}}]";
    }

    private sealed class TestPerson
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public int Age { get; set; }
    }

    private sealed class TestPersonLong
    {
        public long Id { get; set; }
        public string? Name { get; set; }
    }

    private sealed class TestPersonWithTags
    {
        public int Id { get; set; }
        public string[]? Tags { get; set; }
    }
}
