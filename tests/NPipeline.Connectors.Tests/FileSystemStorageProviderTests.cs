using AwesomeAssertions;

namespace NPipeline.Connectors.Tests;

public sealed class FileSystemStorageProviderTests : IAsyncLifetime
{
    private readonly FileSystemStorageProvider _provider = new();
    private string _testDirectory = null!;

    public async Task InitializeAsync()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"npipeline_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);
        await Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (Directory.Exists(_testDirectory))
            Directory.Delete(_testDirectory, true);

        await Task.CompletedTask;
    }

    [Fact]
    public async Task DeleteAsync_WithExistingFile_DeletesFile()
    {
        // Arrange
        var filePath = Path.Combine(_testDirectory, "test.txt");
        await File.WriteAllTextAsync(filePath, "test content");
        var uri = StorageUri.FromFilePath(filePath);

        File.Exists(filePath).Should().BeTrue();

        // Act
        await _provider.DeleteAsync(uri);

        // Assert
        File.Exists(filePath).Should().BeFalse();
    }

    [Fact]
    public async Task DeleteAsync_WithExistingDirectory_DeletesDirectoryRecursively()
    {
        // Arrange
        var subDir = Path.Combine(_testDirectory, "subdir");
        Directory.CreateDirectory(subDir);
        var filePath = Path.Combine(subDir, "file.txt");
        await File.WriteAllTextAsync(filePath, "content");
        var uri = StorageUri.FromFilePath(subDir);

        Directory.Exists(subDir).Should().BeTrue();

        // Act
        await _provider.DeleteAsync(uri);

        // Assert
        Directory.Exists(subDir).Should().BeFalse();
    }

    [Fact]
    public async Task DeleteAsync_WithNonExistentPath_DoesNotThrow()
    {
        // Arrange
        var nonExistentPath = Path.Combine(_testDirectory, "nonexistent.txt");
        var uri = StorageUri.FromFilePath(nonExistentPath);

        // Act
        var act = async () => await _provider.DeleteAsync(uri);

        // Assert
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ListAsync_WithDirectoryContainingFiles_ReturnsOnlyTopLevelItems()
    {
        // Arrange - default recursive=false should only list top-level items
        var file1 = Path.Combine(_testDirectory, "file1.txt");
        var file2 = Path.Combine(_testDirectory, "file2.txt");
        var subDir = Path.Combine(_testDirectory, "subdir");
        var file3 = Path.Combine(subDir, "file3.txt");

        await File.WriteAllTextAsync(file1, "content1");
        await File.WriteAllTextAsync(file2, "content2");
        Directory.CreateDirectory(subDir);
        await File.WriteAllTextAsync(file3, "content3");

        var uri = StorageUri.FromFilePath(_testDirectory);

        // Act
        var items = new List<StorageItem>();

        await foreach (var item in _provider.ListAsync(uri))
        {
            items.Add(item);
        }

        // Assert - should include top-level files and subdir, but not file3.txt
        items.Should().HaveCount(3); // file1.txt, file2.txt, subdir
        items.Should().Contain(i => i.Uri.Path.Contains("file1.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("file2.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("subdir") && i.IsDirectory);
        items.Should().NotContain(i => i.Uri.Path.Contains("file3.txt"));

        var fileItem = items.FirstOrDefault(i => i.Uri.Path.Contains("file1.txt"));
        fileItem.Should().NotBeNull();
        fileItem!.IsDirectory.Should().BeFalse();
        fileItem.Size.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ListAsync_WithRecursiveTrue_ReturnsAllNestedItems()
    {
        // Arrange - recursive=true should list all items including nested ones
        var file1 = Path.Combine(_testDirectory, "file1.txt");
        var file2 = Path.Combine(_testDirectory, "file2.txt");
        var subDir = Path.Combine(_testDirectory, "subdir");
        var file3 = Path.Combine(subDir, "file3.txt");

        await File.WriteAllTextAsync(file1, "content1");
        await File.WriteAllTextAsync(file2, "content2");
        Directory.CreateDirectory(subDir);
        await File.WriteAllTextAsync(file3, "content3");

        var uri = StorageUri.FromFilePath(_testDirectory);

        // Act
        var items = new List<StorageItem>();

        await foreach (var item in _provider.ListAsync(uri, true))
        {
            items.Add(item);
        }

        // Assert - should include all items including nested file3.txt
        items.Should().HaveCountGreaterThanOrEqualTo(4); // file1.txt, file2.txt, subdir, file3.txt
        items.Should().Contain(i => i.Uri.Path.Contains("file1.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("file2.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("subdir") && i.IsDirectory);
        items.Should().Contain(i => i.Uri.Path.Contains("file3.txt"));

        var dirItem = items.FirstOrDefault(i => i.Uri.Path.Contains("subdir"));
        dirItem.Should().NotBeNull();
        dirItem!.IsDirectory.Should().BeTrue();
    }

    [Fact]
    public async Task ListAsync_WithNonExistentDirectory_ReturnsEmpty()
    {
        // Arrange
        var nonExistentPath = Path.Combine(_testDirectory, "nonexistent");
        var uri = StorageUri.FromFilePath(nonExistentPath);

        // Act
        var items = new List<StorageItem>();

        await foreach (var item in _provider.ListAsync(uri))
        {
            items.Add(item);
        }

        // Assert
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task GetMetadataAsync_WithExistingFile_ReturnsMetadata()
    {
        // Arrange
        var filePath = Path.Combine(_testDirectory, "test.csv");
        await File.WriteAllTextAsync(filePath, "header,value\n1,2");
        var uri = StorageUri.FromFilePath(filePath);

        // Act
        var metadata = await _provider.GetMetadataAsync(uri);

        // Assert
        metadata.Should().NotBeNull();
        metadata!.Size.Should().BeGreaterThan(0);
        metadata.LastModified.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(1));
        metadata.ContentType.Should().Be("text/csv");
        metadata.IsDirectory.Should().BeFalse();
    }

    [Fact]
    public async Task GetMetadataAsync_WithExistingDirectory_ReturnsMetadata()
    {
        // Arrange
        var subDir = Path.Combine(_testDirectory, "mydir");
        Directory.CreateDirectory(subDir);
        var uri = StorageUri.FromFilePath(subDir);

        // Act
        var metadata = await _provider.GetMetadataAsync(uri);

        // Assert
        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(0); // Directories typically report size 0
        metadata.LastModified.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(1));
        metadata.ContentType.Should().BeNull();
        metadata.IsDirectory.Should().BeTrue();
    }

    [Fact]
    public async Task GetMetadataAsync_WithNonExistentPath_ReturnsNull()
    {
        // Arrange
        var nonExistentPath = Path.Combine(_testDirectory, "nonexistent.txt");
        var uri = StorageUri.FromFilePath(nonExistentPath);

        // Act
        var metadata = await _provider.GetMetadataAsync(uri);

        // Assert
        metadata.Should().BeNull();
    }

    [Fact]
    public async Task GetMetadataAsync_WithJsonFile_ReturnCorrectContentType()
    {
        // Arrange
        var filePath = Path.Combine(_testDirectory, "data.json");
        await File.WriteAllTextAsync(filePath, "{}");
        var uri = StorageUri.FromFilePath(filePath);

        // Act
        var metadata = await _provider.GetMetadataAsync(uri);

        // Assert
        metadata.Should().NotBeNull();
        metadata!.ContentType.Should().Be("application/json");
    }
}
