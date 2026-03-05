using System.Text;
using System.Text.Json;
using Azure.Storage.Blobs.Models;
using AwesomeAssertions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

/// <summary>
///     Integration tests for AdlsGen2StorageProvider using TestContainers with Azurite.
///     Tests all IStorageProvider operations against a real Azurite instance.
///     Note: Azurite has partial ADLS Gen2 support - some behaviors may differ from real ADLS.
/// </summary>
public sealed class AdlsGen2StorageProviderIntegrationTests : IClassFixture<AzuriteAdlsFixture>
{
    private const string AzuriteAccountName = AzuriteAdlsFixture.AccountName;
    private const string AzuriteAccountKey = AzuriteAdlsFixture.AccountKey;
    private const int LargeFileSizeBytes = 2 * 1024 * 1024;
    private readonly AzuriteAdlsFixture _fixture;

    public AdlsGen2StorageProviderIntegrationTests(AzuriteAdlsFixture fixture)
    {
        _fixture = fixture;
    }

    private AdlsGen2StorageProvider Provider => _fixture.Provider;
    private AdlsGen2StorageProvider _provider => _fixture.Provider;
    private AdlsGen2StorageProviderOptions _options => _fixture.Options;

    /// <summary>
    ///     Collects all items from an async enumerable into a list.
    /// </summary>
    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source, CancellationToken ct = default)
    {
        var list = new List<T>();

        await foreach (var item in source.WithCancellation(ct))
        {
            list.Add(item);
        }

        return list;
    }

    /// <summary>
    ///     Generates a unique filesystem name for each test.
    /// </summary>
    private string GetUniqueFilesystemName()
    {
        return $"testfs{Guid.NewGuid():N}".Substring(0, 20); // Filesystem names max 63 chars
    }

    /// <summary>
    ///     Creates a test file using the Blob API (compatible with Azurite which does not support
    ///     the ADLS Gen2 DFS path-create endpoint used by DataLakeFileClient.UploadAsync).
    /// </summary>
    private async Task CreateTestFileAsync(string filesystemName, string path, string content, string? contentType = null)
    {
        var containerClient = _fixture.BlobServiceClient.GetBlobContainerClient(filesystemName);
        await containerClient.CreateIfNotExistsAsync();

        var blobClient = containerClient.GetBlobClient(path);
        var options = new BlobUploadOptions();

        if (contentType != null)
            options.HttpHeaders = new BlobHttpHeaders { ContentType = contentType };

        await blobClient.UploadAsync(BinaryData.FromString(content), options);
    }

    /// <summary>
    ///     Creates a directory placeholder using the Blob API.
    ///     Azurite does not support the ADLS Gen2 DFS directory-create endpoint; a stand-in blob
    ///     at the given path makes ExistsAsync return true for the directory entry.
    /// </summary>
    private async Task CreateDirectoryAsync(string filesystemName, string path)
    {
        var containerClient = _fixture.BlobServiceClient.GetBlobContainerClient(filesystemName);
        await containerClient.CreateIfNotExistsAsync();

        // Upload a zero-byte placeholder at the exact path so provider.ExistsAsync returns true.
        var blobClient = containerClient.GetBlobClient(path);
        await blobClient.UploadAsync(BinaryData.Empty, true);
    }

    #region Read Operations Tests

    [Fact]
    public async Task OpenReadAsync_ExistingFile_ReturnsContent()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "test-file.txt";
        var expectedContent = "Hello, ADLS Gen2 Storage!";
        await CreateTestFileAsync(filesystemName, path, expectedContent);

        var uri = StorageUri.Parse(
            $"adls://{filesystemName}/{path}?accountName={AzuriteAdlsFixture.AccountName}&accountKey={AzuriteAdlsFixture.AccountKey}");

        // Act
        using var stream = await Provider.OpenReadAsync(uri);
        using var reader = new StreamReader(stream);
        var actualContent = await reader.ReadToEndAsync();

        // Assert
        actualContent.Should().Be(expectedContent);
    }

    [Fact]
    public async Task OpenReadAsync_NonExistentFile_ThrowsFileNotFoundException()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "non-existent-file.txt";

        var uri = StorageUri.Parse(
            $"adls://{filesystemName}/{path}?accountName={AzuriteAdlsFixture.AccountName}&accountKey={AzuriteAdlsFixture.AccountKey}");

        // Act
        var act = async () => await Provider.OpenReadAsync(uri);

        // Assert
        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task OpenReadAsync_LargeFile_UsesStreaming()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "large-file.bin";
        var largeContent = new byte[LargeFileSizeBytes];
        new Random().NextBytes(largeContent);

        var containerClient = _fixture.BlobServiceClient.GetBlobContainerClient(filesystemName);
        await containerClient.CreateIfNotExistsAsync();

        var blobClient = containerClient.GetBlobClient(path);
        using var uploadStream = new MemoryStream(largeContent);
        await blobClient.UploadAsync(uploadStream, true);

        var uri = StorageUri.Parse(
            $"adls://{filesystemName}/{path}?accountName={AzuriteAdlsFixture.AccountName}&accountKey={AzuriteAdlsFixture.AccountKey}");

        // Act
        using var stream = await Provider.OpenReadAsync(uri);
        var actualContent = new byte[largeContent.Length];
        var bytesRead = 0;
        int readResult;

        do
        {
            readResult = await stream.ReadAsync(new Memory<byte>(actualContent, bytesRead, actualContent.Length - bytesRead));
            bytesRead += readResult;
        } while (readResult > 0);

        // Assert
        bytesRead.Should().Be(largeContent.Length);
        actualContent.Should().BeEquivalentTo(largeContent);
    }

    [Fact]
    public async Task OpenReadAsync_DifferentContentTypes_ReturnsCorrectContent()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "test-file.json";
        var expectedContent = JsonSerializer.Serialize(new { name = "test", value = 123 });
        await CreateTestFileAsync(filesystemName, path, expectedContent, "application/json");

        var uri = StorageUri.Parse(
            $"adls://{filesystemName}/{path}?accountName={AzuriteAdlsFixture.AccountName}&accountKey={AzuriteAdlsFixture.AccountKey}");

        // Act
        using var stream = await Provider.OpenReadAsync(uri);
        using var reader = new StreamReader(stream);
        var actualContent = await reader.ReadToEndAsync();

        // Assert
        actualContent.Should().Be(expectedContent);
    }

    #endregion

    #region Write Operations Tests

    [Fact]
    public async Task OpenWriteAsync_NewFile_UploadsSuccessfully()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "new-file.txt";
        var content = "New file content";

        var uri = StorageUri.Parse(
            $"adls://{filesystemName}/{path}?accountName={AzuriteAdlsFixture.AccountName}&accountKey={AzuriteAdlsFixture.AccountKey}");

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            await writeStream.WriteAsync(buffer);
        }

        // Assert
        using var readStream = await _provider.OpenReadAsync(uri);
        using var reader = new StreamReader(readStream);
        var actualContent = await reader.ReadToEndAsync();
        actualContent.Should().Be(content);
    }

    [Fact]
    public async Task OpenWriteAsync_OverwritesExistingFile()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "overwrite-file.txt";
        var originalContent = "Original content";
        var newContent = "New content";
        await CreateTestFileAsync(filesystemName, path, originalContent);

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(newContent);
            await writeStream.WriteAsync(buffer);
        }

        // Assert
        using var readStream = await _provider.OpenReadAsync(uri);
        using var reader = new StreamReader(readStream);
        var actualContent = await reader.ReadToEndAsync();
        actualContent.Should().Be(newContent);
    }

    [Fact]
    public async Task OpenWriteAsync_WithContentType_SetsContentType()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "json-file.json";
        var content = JsonSerializer.Serialize(new { test = "data" });

        var uri = StorageUri.Parse(
            $"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}&contentType=application/json");

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            await writeStream.WriteAsync(buffer);
        }

        // Assert
        var metadata = await _provider.GetMetadataAsync(uri);
        metadata.Should().NotBeNull();
        metadata!.ContentType.Should().Be("application/json");
    }

    [Fact]
    public async Task OpenWriteAsync_SmallFile_UploadsSuccessfully()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "small-file.txt";
        var content = "Small content";
        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            await writeStream.WriteAsync(buffer);
        }

        // Assert
        var exists = await _provider.ExistsAsync(uri);
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task OpenWriteAsync_LargeFile_UsesUpload()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "large-file.bin";
        var largeContent = new byte[LargeFileSizeBytes];
        new Random().NextBytes(largeContent);
        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            await writeStream.WriteAsync(largeContent);
        }

        // Assert
        using var readStream = await _provider.OpenReadAsync(uri);
        var actualContent = new byte[largeContent.Length];
        var bytesRead = 0;
        int readResult;

        do
        {
            readResult = await readStream.ReadAsync(new Memory<byte>(actualContent, bytesRead, actualContent.Length - bytesRead));
            bytesRead += readResult;
        } while (readResult > 0);

        bytesRead.Should().Be(largeContent.Length);
        actualContent.Should().BeEquivalentTo(largeContent);
    }

    [Fact]
    public async Task OpenWriteAsync_WithCustomConcurrencyAndTransferSize_UploadsSuccessfully()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "custom-upload.bin";
        var largeContent = new byte[LargeFileSizeBytes];
        new Random().NextBytes(largeContent);

        // Create provider with custom settings; use DefaultConnectionString so the Azure SDK
        // routes requests through Azurite's blob endpoint (ServiceUrl alone causes DFS 400s).
        var customOptions = new AdlsGen2StorageProviderOptions
        {
            DefaultConnectionString = _fixture.GetConnectionString(),
            ServiceVersion = _options!.ServiceVersion,
            UploadThresholdBytes = LargeFileSizeBytes / 2,
            UploadMaximumConcurrency = 4,
            UploadMaximumTransferSizeBytes = 256 * 1024,
            UseDefaultCredentialChain = false,
        };

        var clientFactory = new AdlsGen2ClientFactory(customOptions);
        var customProvider = new AdlsGen2StorageProvider(clientFactory, customOptions);

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        using (var writeStream = await customProvider.OpenWriteAsync(uri))
        {
            await writeStream.WriteAsync(largeContent);
        }

        // Assert
        using var readStream = await customProvider.OpenReadAsync(uri);
        var actualContent = new byte[largeContent.Length];
        var bytesRead = 0;
        int readResult;

        do
        {
            readResult = await readStream.ReadAsync(new Memory<byte>(actualContent, bytesRead, actualContent.Length - bytesRead));
            bytesRead += readResult;
        } while (readResult > 0);

        bytesRead.Should().Be(largeContent.Length);
        actualContent.Should().BeEquivalentTo(largeContent);
    }

    #endregion

    #region Exists Operations Tests

    [Fact]
    public async Task ExistsAsync_ExistingFile_ReturnsTrue()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "existing-file.txt";
        await CreateTestFileAsync(filesystemName, path, "Test content");

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var exists = await _provider!.ExistsAsync(uri);

        // Assert
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_NonExistentFile_ReturnsFalse()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "non-existent-file.txt";
        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var exists = await _provider!.ExistsAsync(uri);

        // Assert
        exists.Should().BeFalse();
    }

    [Fact]
    public async Task ExistsAsync_ExistingDirectory_ReturnsTrue()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "existing-directory";
        await CreateDirectoryAsync(filesystemName, path);

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var exists = await _provider!.ExistsAsync(uri);

        // Assert
        exists.Should().BeTrue();
    }

    #endregion

    #region List Operations Tests

    [Fact]
    public async Task ListAsync_RecursiveTrue_ReturnsAllFiles()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        await CreateTestFileAsync(filesystemName, "file1.txt", "Content 1");
        await CreateTestFileAsync(filesystemName, "folder/file2.txt", "Content 2");
        await CreateTestFileAsync(filesystemName, "folder/subfolder/file3.txt", "Content 3");

        var prefixUri = StorageUri.Parse($"adls://{filesystemName}/?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var items = await CollectAsync(_provider!.ListAsync(prefixUri, true));

        // Assert
        items.Should().HaveCount(5); // 3 files + 2 directories
        items.Count(i => !i.IsDirectory).Should().Be(3); // 3 files
        items.Count(i => i.IsDirectory).Should().Be(2); // 2 directories (folder, folder/subfolder)
    }

    [Fact]
    public async Task ListAsync_RecursiveFalse_ReturnsImmediateItems()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        await CreateTestFileAsync(filesystemName, "file1.txt", "Content 1");
        await CreateTestFileAsync(filesystemName, "folder/file2.txt", "Content 2");

        var prefixUri = StorageUri.Parse($"adls://{filesystemName}/?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var items = await CollectAsync(_provider!.ListAsync(prefixUri));

        // Assert - Non-recursive listing returns files and directories at the root level
        items.Should().HaveCount(2);
        items.Should().Contain(i => i.Uri.Path.Contains("file1.txt") && !i.IsDirectory);
        items.Should().Contain(i => i.Uri.Path.Contains("folder") && i.IsDirectory);
    }

    [Fact]
    public async Task ListAsync_WithPrefix_FiltersCorrectly()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        await CreateTestFileAsync(filesystemName, "data/file1.txt", "Content 1");
        await CreateTestFileAsync(filesystemName, "data/file2.txt", "Content 2");
        await CreateTestFileAsync(filesystemName, "logs/file3.txt", "Content 3");

        var prefixUri = StorageUri.Parse($"adls://{filesystemName}/data/?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var items = await CollectAsync(_provider!.ListAsync(prefixUri, true));

        // Assert
        items.Should().HaveCount(2);
        items.Should().Contain(i => i.Uri.Path.Contains("data/file1.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("data/file2.txt"));
        items.Should().NotContain(i => i.Uri.Path.Contains("logs/file3.txt"));
    }

    [Fact]
    public async Task ListAsync_EmptyFilesystem_ReturnsEmpty()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();

        // Create the filesystem but don't add any files
        var containerClient = _fixture.BlobServiceClient.GetBlobContainerClient(filesystemName);
        await containerClient.CreateIfNotExistsAsync();

        var prefixUri = StorageUri.Parse($"adls://{filesystemName}/?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var items = await CollectAsync(_provider!.ListAsync(prefixUri, true));

        // Assert
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_HierarchicalStructure_ReturnsCorrectStructure()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        await CreateTestFileAsync(filesystemName, "root.txt", "Root content");
        await CreateTestFileAsync(filesystemName, "level1/level2/level3/deep.txt", "Deep content");
        await CreateTestFileAsync(filesystemName, "level1/level2/medium.txt", "Medium content");
        await CreateTestFileAsync(filesystemName, "level1/shallow.txt", "Shallow content");

        var prefixUri = StorageUri.Parse($"adls://{filesystemName}/?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var items = await CollectAsync(_provider!.ListAsync(prefixUri, true));

        // Assert
        items.Count(i => !i.IsDirectory).Should().Be(4); // 4 files
        items.Should().Contain(i => i.Uri.Path.Contains("root.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("deep.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("medium.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("shallow.txt"));
    }

    #endregion

    #region Metadata Operations Tests

    [Fact]
    public async Task GetMetadataAsync_ExistingFile_ReturnsFileProperties()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "metadata-file.txt";
        var content = "Test content for metadata";
        await CreateTestFileAsync(filesystemName, path, content, "text/plain");

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var metadata = await _provider!.GetMetadataAsync(uri);

        // Assert
        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(content.Length);
        metadata.ContentType.Should().Be("text/plain");
        metadata.ETag.Should().NotBeNullOrEmpty();
        metadata.IsDirectory.Should().BeFalse();
    }

    [Fact]
    public async Task GetMetadataAsync_NonExistentFile_ReturnsNull()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "non-existent-file.txt";
        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var metadata = await _provider!.GetMetadataAsync(uri);

        // Assert
        metadata.Should().BeNull();
    }

    [Fact]
    public async Task GetMetadataAsync_WithCustomMetadata_IncludesCustomMetadata()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "custom-metadata-file.txt";

        var containerClient = _fixture.BlobServiceClient.GetBlobContainerClient(filesystemName);
        await containerClient.CreateIfNotExistsAsync();

        var blobClient = containerClient.GetBlobClient(path);
        await blobClient.UploadAsync(BinaryData.FromString("Content"), true);

        await blobClient.SetMetadataAsync(new Dictionary<string, string>
        {
            ["custom_key_1"] = "custom-value-1",
            ["custom_key_2"] = "custom-value-2",
        });

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var metadata = await _provider!.GetMetadataAsync(uri);

        // Assert
        metadata.Should().NotBeNull();
        metadata!.CustomMetadata.Should().HaveCount(2);
        metadata.CustomMetadata.Should().ContainKey("custom_key_1");
        metadata.CustomMetadata["custom_key_1"].Should().Be("custom-value-1");
        metadata.CustomMetadata.Should().ContainKey("custom_key_2");
        metadata.CustomMetadata["custom_key_2"].Should().Be("custom-value-2");
    }

    [Fact]
    public async Task GetMetadataAsync_IncludesAllRequiredFields()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "full-metadata-file.txt";
        var content = "Full metadata test content";
        await CreateTestFileAsync(filesystemName, path, content, "application/json");

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var metadata = await _provider!.GetMetadataAsync(uri);

        // Assert
        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(content.Length);
        metadata.LastModified.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(10));
        metadata.ContentType.Should().Be("application/json");
        metadata.ETag.Should().NotBeNullOrEmpty();
        metadata.IsDirectory.Should().BeFalse();
    }

    #endregion

    #region Delete Operations Tests

    [Fact]
    public async Task DeleteAsync_ExistingFile_DeletesSuccessfully()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "delete-file.txt";
        await CreateTestFileAsync(filesystemName, path, "Content to delete");

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        await _provider!.DeleteAsync(uri);

        // Assert
        var exists = await _provider.ExistsAsync(uri);
        exists.Should().BeFalse();
    }

    [Fact]
    public async Task DeleteAsync_NonExistentFile_SilentlySucceeds()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "non-existent-file.txt";
        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act & Assert - should not throw
        await _provider!.DeleteAsync(uri);
    }

    #endregion

    #region Move Operations Tests

    [Fact]
    public async Task MoveAsync_WithinSameFilesystem_MovesSuccessfully()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var sourcePath = "source-file.txt";
        var destinationPath = "destination-file.txt";
        var content = "Content to move";
        await CreateTestFileAsync(filesystemName, sourcePath, content);

        var sourceUri = StorageUri.Parse($"adls://{filesystemName}/{sourcePath}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");
        var destinationUri = StorageUri.Parse($"adls://{filesystemName}/{destinationPath}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        await _provider!.MoveAsync(sourceUri, destinationUri);

        // Assert
        var sourceExists = await _provider.ExistsAsync(sourceUri);
        sourceExists.Should().BeFalse();

        var destExists = await _provider.ExistsAsync(destinationUri);
        destExists.Should().BeTrue();

        using var readStream = await _provider.OpenReadAsync(destinationUri);
        using var reader = new StreamReader(readStream);
        var actualContent = await reader.ReadToEndAsync();
        actualContent.Should().Be(content);
    }

    [Fact]
    public async Task MoveAsync_ToDifferentDirectory_MovesSuccessfully()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var sourcePath = "source-dir/file.txt";
        var destinationPath = "dest-dir/file.txt";
        var content = "Content to move across directories";
        await CreateTestFileAsync(filesystemName, sourcePath, content);
        await CreateDirectoryAsync(filesystemName, "dest-dir");

        var sourceUri = StorageUri.Parse($"adls://{filesystemName}/{sourcePath}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");
        var destinationUri = StorageUri.Parse($"adls://{filesystemName}/{destinationPath}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        await _provider!.MoveAsync(sourceUri, destinationUri);

        // Assert
        var sourceExists = await _provider.ExistsAsync(sourceUri);
        sourceExists.Should().BeFalse();

        var destExists = await _provider.ExistsAsync(destinationUri);
        destExists.Should().BeTrue();
    }

    #endregion

    #region Authentication Methods Tests

    [Fact]
    public async Task WithConnectionString_AuthenticatesSuccessfully()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "connection-string-test.txt";

        var options = new AdlsGen2StorageProviderOptions
        {
            DefaultConnectionString = _fixture.GetConnectionString(),
            ServiceVersion = _options!.ServiceVersion,
            UseDefaultCredentialChain = false,
        };

        var clientFactory = new AdlsGen2ClientFactory(options);
        var provider = new AdlsGen2StorageProvider(clientFactory, options);

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}");
        var content = "Connection string test";

        // Act
        using (var writeStream = await provider.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            await writeStream.WriteAsync(buffer);
        }

        using var readStream = await provider.OpenReadAsync(uri);
        using var reader = new StreamReader(readStream);
        var actualContent = await reader.ReadToEndAsync();

        // Assert
        actualContent.Should().Be(content);
    }

    [Fact]
    public async Task WithAccountNameAndKey_AuthenticatesSuccessfully()
    {
        // Arrange
        // Build a connection-string from the known account key so the factory's DefaultConnectionString
        // is derived from accountKey credentials rather than the shared fixture string. This exercises
        // a distinct client-cache entry while keeping DFS calls compatible with Azurite (which requires
        // a connection string; DataLakeServiceClient constructed from a plain ServiceUrl + key does not
        // route DFS path operations correctly through Azurite's blob endpoint).
        var filesystemName = GetUniqueFilesystemName();
        var path = "account-key-test.txt";
        var content = "Account key test";

        var blobEndpoint = _fixture.GetBlobServiceUri();

        var keyConnectionString =
            $"DefaultEndpointsProtocol=http;AccountName={AzuriteAccountName};AccountKey={AzuriteAccountKey}" +
            $";BlobEndpoint={blobEndpoint}";

        var options = new AdlsGen2StorageProviderOptions
        {
            DefaultConnectionString = keyConnectionString,
            ServiceVersion = _options!.ServiceVersion,
            UploadThresholdBytes = _options.UploadThresholdBytes,
            UseDefaultCredentialChain = false,
        };

        var provider = new AdlsGen2StorageProvider(new AdlsGen2ClientFactory(options), options);

        // URI still carries accountName + accountKey; DefaultConnectionString (built from the same key)
        // takes auth precedence, verifying that the provider handles this URI shape end-to-end.
        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        using (var writeStream = await provider.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            await writeStream.WriteAsync(buffer);
        }

        using var readStream = await provider.OpenReadAsync(uri);
        using var reader = new StreamReader(readStream);
        var actualContent = await reader.ReadToEndAsync();

        // Assert
        actualContent.Should().Be(content);
    }

    #endregion

    #region Error Scenarios Tests

    [Fact]
    public async Task InvalidCredentials_ThrowsUnauthorizedAccessException()
    {
        // Arrange — create a provider with NO DefaultConnectionString so the per-URI accountKey is
        // actually sent to Azurite.  A provider that has DefaultConnectionString will use valid auth
        // regardless of what key the URI carries.
        var filesystemName = GetUniqueFilesystemName();
        var path = "unauthorized-file.txt";

        // First create the file using the valid fixture provider so the container exists.
        await CreateTestFileAsync(filesystemName, path, "content");

        // Use a valid base64 string that represents invalid credentials
        var invalidKey =
            "dGVzdGtleWZvcmF6dXJpdGVzdGluZ3VudGhvcml6ZWRhY2Nlc3NleGNlcHRpb250ZXN0aW5nd2l0aHZhbGlkYmFzZTY0ZW5jb2Rpbmd0aGF0cGFzc2VzdmFsaWRhdGlvbididXRmYWlsc2F1dGhlbnRpY2F0aW9u";

        // Build a provider that uses only the Azurite service URL (no DefaultConnectionString), so
        // that the per-URI accountKey credential is actually forwarded to Azurite.
        var blobEndpoint = _fixture.GetBlobServiceUri();

        var noConnectionStringOptions = new AdlsGen2StorageProviderOptions
        {
            ServiceUrl = blobEndpoint,
            ServiceVersion = _options!.ServiceVersion,
            UseDefaultCredentialChain = false,
        };

        var provider = new AdlsGen2StorageProvider(new AdlsGen2ClientFactory(noConnectionStringOptions), noConnectionStringOptions);

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={invalidKey}");

        // Act
        var act = async () => await provider.OpenReadAsync(uri);

        // Assert
        await act.Should().ThrowAsync<UnauthorizedAccessException>();
    }

    [Fact]
    public async Task InvalidFilesystemName_ThrowsArgumentException()
    {
        // Arrange
        var invalidFilesystemName = "Invalid Filesystem Name!"; // Contains spaces and special chars
        var path = "test-file.txt";
        var uri = StorageUri.Parse($"adls://{invalidFilesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var act = async () => await _provider!.OpenReadAsync(uri);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>();
    }

    #endregion

    #region Concurrency Tests

    [Fact]
    public async Task MultipleConcurrentReads_Succeeds()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "concurrent-read-file.txt";
        var content = "Concurrent read test content";
        await CreateTestFileAsync(filesystemName, path, content);

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        // Act
        var tasks = Enumerable.Range(0, 10).Select(async _ =>
        {
            using var stream = await _provider!.OpenReadAsync(uri);
            using var reader = new StreamReader(stream);
            return await reader.ReadToEndAsync();
        });

        var results = await Task.WhenAll(tasks);

        // Assert
        results.Should().HaveCount(10);
        results.Should().AllBeEquivalentTo(content);
    }

    [Fact]
    public async Task MultipleConcurrentWritesToDifferentFiles_Succeeds()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();

        var uris = Enumerable.Range(0, 10)
            .Select(i => StorageUri.Parse($"adls://{filesystemName}/file-{i}.txt?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}"))
            .ToArray();

        // Act
        var tasks = uris.Select(async (uri, index) =>
        {
            using var writeStream = await _provider!.OpenWriteAsync(uri);
            var buffer = Encoding.UTF8.GetBytes($"Content {index}");
            await writeStream.WriteAsync(buffer);
        });

        await Task.WhenAll(tasks);

        // Assert
        foreach (var uri in uris)
        {
            var exists = await _provider!.ExistsAsync(uri);
            exists.Should().BeTrue();
        }
    }

    #endregion

    #region Provider Metadata Tests

    [Fact]
    public void GetMetadata_ReturnsCorrectProviderMetadata()
    {
        // Act
        var metadata = _provider!.GetMetadata();

        // Assert
        metadata.Should().NotBeNull();
        metadata.Name.Should().Be("Azure Data Lake Storage Gen2");
        metadata.SupportedSchemes.Should().Contain("adls");
        metadata.SupportsRead.Should().BeTrue();
        metadata.SupportsWrite.Should().BeTrue();
        metadata.SupportsListing.Should().BeTrue();
        metadata.SupportsMetadata.Should().BeTrue();
        metadata.SupportsHierarchy.Should().BeTrue(); // Key differentiator from Azure Blob
    }

    [Fact]
    public void CanHandle_WithAdlsScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("adls://filesystem/path/file.txt");

        // Act
        var canHandle = _provider!.CanHandle(uri);

        // Assert
        canHandle.Should().BeTrue();
    }

    [Fact]
    public void CanHandle_WithOtherScheme_ReturnsFalse()
    {
        // Arrange
        var azureUri = StorageUri.Parse("azure://container/blob.txt");
        var fileUri = StorageUri.FromFilePath("/path/to/file.txt");

        // Act
        var canHandleAzure = _provider!.CanHandle(azureUri);
        var canHandleFile = _provider.CanHandle(fileUri);

        // Assert
        canHandleAzure.Should().BeFalse();
        canHandleFile.Should().BeFalse();
    }

    #endregion

    #region Cancellation Tests

    [Fact]
    public async Task OpenReadAsync_WithCancellation_ThrowsOperationCanceledException()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "cancellation-test.txt";
        await CreateTestFileAsync(filesystemName, path, "Content");

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() => _provider!.OpenReadAsync(uri, cts.Token));
    }

    [Fact]
    public async Task ExistsAsync_WithCancellation_ThrowsOperationCanceledException()
    {
        // Arrange
        var filesystemName = GetUniqueFilesystemName();
        var path = "cancellation-exists-test.txt";
        await CreateTestFileAsync(filesystemName, path, "Content");

        var uri = StorageUri.Parse($"adls://{filesystemName}/{path}?accountName={AzuriteAccountName}&accountKey={AzuriteAccountKey}");

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() => _provider!.ExistsAsync(uri, cts.Token));
    }

    #endregion
}
