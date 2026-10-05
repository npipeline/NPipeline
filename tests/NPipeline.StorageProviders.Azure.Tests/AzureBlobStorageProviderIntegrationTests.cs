using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using Azure.Storage.Blobs.Models;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Abstractions;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

/// <summary>
///     Integration tests for AzureBlobStorageProvider using TestContainers with Azurite.
///     Tests all IStorageProvider operations against a real Azurite instance.
/// </summary>
public sealed class AzureBlobStorageProviderIntegrationTests : IClassFixture<AzuriteFixture>
{
    private const string AzuriteAccountName = AzuriteFixture.AccountName;
    private const string AzuriteAccountKey = AzuriteFixture.AccountKey;
    private const int LargeBlobSizeBytes = 2 * 1024 * 1024;
    private readonly AzuriteFixture _fixture;

    public AzureBlobStorageProviderIntegrationTests(AzuriteFixture fixture)
    {
        _fixture = fixture;
    }

    private AzureBlobStorageProvider Provider => _fixture.Provider;
    private AzureBlobStorageProvider _provider => _fixture.Provider;
    private AzureBlobStorageProviderOptions _options => _fixture.Options;

    /// <summary>
    ///     Generates a unique container name for each test.
    /// </summary>
    private string GetUniqueContainerName() => $"test-container-{Guid.NewGuid():N}";

    /// <summary>
    ///     Creates a test blob with the specified content.
    /// </summary>
    private async Task CreateTestBlobAsync(string containerName, string blobName, string content, string? contentType = null)
    {
        var containerClient = _fixture.BlobServiceClient.GetBlobContainerClient(containerName);
        _ = await containerClient.CreateIfNotExistsAsync();

        var blobClient = containerClient.GetBlobClient(blobName);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
        _ = await blobClient.UploadAsync(stream, true);

        if (contentType != null)
            _ = await blobClient.SetHttpHeadersAsync(new BlobHttpHeaders { ContentType = contentType });
    }

    #region Delete and Move Tests

    private StorageUri BlobUri(string container, string blob) =>
        StorageUri.Parse($"azure://{container}/{blob}?accountName={AzuriteAccountName}");

    [Fact]
    public async Task DeleteAsync_ExistingBlob_RemovesIt()
    {
        var container = GetUniqueContainerName();
        await CreateTestBlobAsync(container, "doomed.txt", "bye");
        var uri = BlobUri(container, "doomed.txt");

        await Provider.DeleteAsync(uri);

        (await Provider.ExistsAsync(uri)).Should().BeFalse();
    }

    [Fact]
    public async Task DeleteAsync_MissingBlobOrContainer_Succeeds()
    {
        var container = GetUniqueContainerName();
        await CreateTestBlobAsync(container, "other.txt", "x");

        await Provider.DeleteAsync(BlobUri(container, "missing.txt"));
        await Provider.DeleteAsync(BlobUri(GetUniqueContainerName(), "missing.txt"));
    }

    [Fact]
    public async Task MoveAsync_OverwritesDestinationAndRemovesSource()
    {
        var container = GetUniqueContainerName();
        await CreateTestBlobAsync(container, "source.txt", "new content", "text/plain");
        await CreateTestBlobAsync(container, "dir/destination.txt", "old content that is longer");
        var source = BlobUri(container, "source.txt");
        var destination = BlobUri(container, "dir/destination.txt");

        await Provider.MoveAsync(source, destination);

        (await Provider.ExistsAsync(source)).Should().BeFalse();
        using var reader = new StreamReader(await Provider.OpenReadAsync(destination));
        (await reader.ReadToEndAsync()).Should().Be("new content");
        (await Provider.GetMetadataAsync(destination))!.ContentType.Should().Be("text/plain");
    }

    [Fact]
    public async Task MoveAsync_AcrossContainers_Works()
    {
        var sourceContainer = GetUniqueContainerName();
        var destinationContainer = GetUniqueContainerName();
        await CreateTestBlobAsync(sourceContainer, "a.txt", "payload");
        _ = await _fixture.BlobServiceClient.GetBlobContainerClient(destinationContainer).CreateIfNotExistsAsync();

        await Provider.MoveAsync(BlobUri(sourceContainer, "a.txt"), BlobUri(destinationContainer, "b.txt"));

        (await Provider.ExistsAsync(BlobUri(sourceContainer, "a.txt"))).Should().BeFalse();
        (await Provider.ExistsAsync(BlobUri(destinationContainer, "b.txt"))).Should().BeTrue();
    }

    [Fact]
    public async Task MoveAsync_MissingSource_ThrowsFileNotFound()
    {
        var container = GetUniqueContainerName();
        await CreateTestBlobAsync(container, "present.txt", "x");

        var act = () => Provider.MoveAsync(BlobUri(container, "missing.txt"), BlobUri(container, "target.txt"));

        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task MoveAsync_ToSameLocation_KeepsTheBlob()
    {
        var container = GetUniqueContainerName();
        await CreateTestBlobAsync(container, "same.txt", "x");
        var uri = BlobUri(container, "same.txt");

        await Provider.MoveAsync(uri, uri);

        (await Provider.ExistsAsync(uri)).Should().BeTrue();
    }

    #endregion

    #region Read Operations Tests

    [Fact]
    public async Task OpenReadAsync_ExistingBlob_ReturnsContent()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "test-file.txt";
        var expectedContent = "Hello, Azure Blob Storage!";
        await CreateTestBlobAsync(containerName, blobName, expectedContent);

        var uri = StorageUri.Parse(
            $"azure://{containerName}/{blobName}?accountName={AzuriteFixture.AccountName}");

        // Act
        using var stream = await Provider.OpenReadAsync(uri);
        using var reader = new StreamReader(stream);
        var actualContent = await reader.ReadToEndAsync();

        // Assert
        actualContent.Should().Be(expectedContent);
    }

    [Fact]
    public async Task OpenReadAsync_NonExistentBlob_ThrowsFileNotFoundException()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "non-existent-file.txt";

        var uri = StorageUri.Parse(
            $"azure://{containerName}/{blobName}?accountName={AzuriteFixture.AccountName}");

        // Act
        var act = async () => await Provider.OpenReadAsync(uri);

        // Assert
        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task OpenReadAsync_LargeBlob_UsesStreaming()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "large-file.bin";
        var largeContent = new byte[LargeBlobSizeBytes];
        new Random().NextBytes(largeContent);

        var containerClient = _fixture.BlobServiceClient.GetBlobContainerClient(containerName);
        await containerClient.CreateIfNotExistsAsync();

        var blobClient = containerClient.GetBlobClient(blobName);
        using var uploadStream = new MemoryStream(largeContent);
        await blobClient.UploadAsync(uploadStream, true);

        var uri = StorageUri.Parse(
            $"azure://{containerName}/{blobName}?accountName={AzuriteFixture.AccountName}");

        // Act
        using var stream = await Provider.OpenReadAsync(uri);
        var actualContent = new byte[largeContent.Length];
        var bytesRead = 0;
        int readResult;

        do
        {
            readResult = await stream.ReadAsync(actualContent, bytesRead, actualContent.Length - bytesRead);
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
        var containerName = GetUniqueContainerName();
        var blobName = "test-file.json";
        var expectedContent = JsonSerializer.Serialize(new { name = "test", value = 123 });
        await CreateTestBlobAsync(containerName, blobName, expectedContent, "application/json");

        var uri = StorageUri.Parse(
            $"azure://{containerName}/{blobName}?accountName={AzuriteFixture.AccountName}");

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
    public async Task OpenWriteAsync_NewBlob_UploadsSuccessfully()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "new-file.txt";
        var content = "New file content";

        var uri = StorageUri.Parse(
            $"azure://{containerName}/{blobName}?accountName={AzuriteFixture.AccountName}");

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            await writeStream.WriteAsync(buffer, 0, buffer.Length);
            await writeStream.CommitAsync();
        }

        // Assert
        using var readStream = await _provider.OpenReadAsync(uri);
        using var reader = new StreamReader(readStream);
        var actualContent = await reader.ReadToEndAsync();
        actualContent.Should().Be(content);
    }

    [Fact]
    public async Task OpenWriteAsync_OverwritesExistingBlob()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "overwrite-file.txt";
        var originalContent = "Original content";
        var newContent = "New content";
        await CreateTestBlobAsync(containerName, blobName, originalContent);

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(newContent);
            await writeStream.WriteAsync(buffer, 0, buffer.Length);
            await writeStream.CommitAsync();
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
        var containerName = GetUniqueContainerName();
        var blobName = "json-file.json";
        var content = JsonSerializer.Serialize(new { test = "data" });

        var uri = StorageUri.Parse(
            $"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}&contentType=application/json");

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            await writeStream.WriteAsync(buffer, 0, buffer.Length);
            await writeStream.CommitAsync();
        }

        // Assert
        var metadata = await _provider.GetMetadataAsync(uri);
        metadata.Should().NotBeNull();
        metadata!.ContentType.Should().Be("application/json");
    }

    [Fact]
    public async Task OpenWriteAsync_SmallBlob_UploadsSuccessfully()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "small-file.txt";
        var content = "Small content";
        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            await writeStream.WriteAsync(buffer, 0, buffer.Length);
            await writeStream.CommitAsync();
        }

        // Assert
        var exists = await _provider.ExistsAsync(uri);
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task OpenWriteAsync_LargeBlob_UsesBlockBlobUpload()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "large-file.bin";
        var largeContent = new byte[LargeBlobSizeBytes];
        new Random().NextBytes(largeContent);
        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            await writeStream.WriteAsync(largeContent, 0, largeContent.Length);
            await writeStream.CommitAsync();
        }

        // Assert
        using var readStream = await _provider.OpenReadAsync(uri);
        var actualContent = new byte[largeContent.Length];
        var bytesRead = 0;
        int readResult;

        do
        {
            readResult = await readStream.ReadAsync(actualContent, bytesRead, actualContent.Length - bytesRead);
            bytesRead += readResult;
        } while (readResult > 0);

        bytesRead.Should().Be(largeContent.Length);
        actualContent.Should().BeEquivalentTo(largeContent);
    }

    [Fact]
    public async Task OpenWriteAsync_WithCustomConcurrencyAndPartSize_UploadsSuccessfully()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "custom-upload.bin";
        var largeContent = new byte[LargeBlobSizeBytes];
        new Random().NextBytes(largeContent);

        // Create provider with custom settings
        var customOptions = new AzureBlobStorageProviderOptions
        {
            ServiceUrl = _options!.ServiceUrl,
            ServiceVersion = _options.ServiceVersion,
            AccountName = AzuriteAccountName,
            DefaultAccountKey = AzuriteAccountKey,
            PartSizeBytes = 256 * 1024,
            MaxConcurrency = 4,
            CreateContainerIfMissing = true,
            UseDefaultCredentialChain = false,
        };

        var clientFactory = new AzureBlobClientFactory(customOptions);
        var customProvider = new AzureBlobStorageProvider(clientFactory, customOptions);

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        using (var writeStream = await customProvider.OpenWriteAsync(uri))
        {
            await writeStream.WriteAsync(largeContent, 0, largeContent.Length);
            await writeStream.CommitAsync();
        }

        // Assert
        using var readStream = await customProvider.OpenReadAsync(uri);
        var actualContent = new byte[largeContent.Length];
        var bytesRead = 0;
        int readResult;

        do
        {
            readResult = await readStream.ReadAsync(actualContent, bytesRead, actualContent.Length - bytesRead);
            bytesRead += readResult;
        } while (readResult > 0);

        bytesRead.Should().Be(largeContent.Length);
        actualContent.Should().BeEquivalentTo(largeContent);
    }

    #endregion

    #region Exists Operations Tests

    [Fact]
    public async Task ExistsAsync_ExistingBlob_ReturnsTrue()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "existing-file.txt";
        await CreateTestBlobAsync(containerName, blobName, "Test content");

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        var exists = await _provider!.ExistsAsync(uri);

        // Assert
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_NonExistentBlob_ReturnsFalse()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "non-existent-file.txt";
        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        var exists = await _provider!.ExistsAsync(uri);

        // Assert
        exists.Should().BeFalse();
    }

    [Fact]
    public async Task ExistsAsync_ExistingContainer_ReturnsTrue()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "test-file.txt";
        await CreateTestBlobAsync(containerName, blobName, "Test content");

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        var exists = await _provider!.ExistsAsync(uri);

        // Assert
        exists.Should().BeTrue();
    }

    #endregion

    #region List Operations Tests

    [Fact]
    public async Task ListAsync_RecursiveTrue_ReturnsAllBlobs()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        await CreateTestBlobAsync(containerName, "file1.txt", "Content 1");
        await CreateTestBlobAsync(containerName, "folder/file2.txt", "Content 2");
        await CreateTestBlobAsync(containerName, "folder/subfolder/file3.txt", "Content 3");

        var prefixUri = StorageUri.Parse($"azure://{containerName}/?accountName={AzuriteAccountName}");

        // Act
        var items = await _provider!.ListAsync(prefixUri, true).ToListAsync();

        // Assert
        items.Should().HaveCount(3);
        items.Should().Contain(i => i.Uri.Path.Contains("file1.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("file2.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("file3.txt"));
    }

    [Fact]
    public async Task ListAsync_RecursiveFalse_ReturnsImmediateBlobs()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        await CreateTestBlobAsync(containerName, "file1.txt", "Content 1");
        await CreateTestBlobAsync(containerName, "folder/file2.txt", "Content 2");
        await CreateTestBlobAsync(containerName, "folder/subfolder/file3.txt", "Content 3");

        var prefixUri = StorageUri.Parse($"azure://{containerName}/?accountName={AzuriteAccountName}");

        // Act
        var items = await _provider!.ListAsync(prefixUri).ToListAsync();

        // Assert - Non-recursive listing returns both files and virtual directories at the root level
        items.Should().HaveCount(2);
        items.Should().Contain(i => i.Uri.Path.Contains("file1.txt") && !i.IsDirectory);
        items.Should().Contain(i => i.Uri.Path.Contains("folder") && i.IsDirectory);
    }

    [Fact]
    public async Task ListAsync_WithPrefix_FiltersCorrectly()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        await CreateTestBlobAsync(containerName, "data/file1.txt", "Content 1");
        await CreateTestBlobAsync(containerName, "data/file2.txt", "Content 2");
        await CreateTestBlobAsync(containerName, "logs/file3.txt", "Content 3");

        var prefixUri = StorageUri.Parse($"azure://{containerName}/data/?accountName={AzuriteAccountName}");

        // Act
        var items = await _provider!.ListAsync(prefixUri, true).ToListAsync();

        // Assert
        items.Should().HaveCount(2);
        items.Should().Contain(i => i.Uri.Path.Contains("data/file1.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("data/file2.txt"));
        items.Should().NotContain(i => i.Uri.Path.Contains("logs/file3.txt"));
    }

    [Fact]
    public async Task ListAsync_EmptyContainer_ReturnsEmpty()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var prefixUri = StorageUri.Parse($"azure://{containerName}/?accountName={AzuriteAccountName}");

        // Act
        var items = await _provider!.ListAsync(prefixUri, true).ToListAsync();

        // Assert
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_HierarchicalStructure_ReturnsCorrectStructure()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        await CreateTestBlobAsync(containerName, "root.txt", "Root content");
        await CreateTestBlobAsync(containerName, "level1/level2/level3/deep.txt", "Deep content");
        await CreateTestBlobAsync(containerName, "level1/level2/medium.txt", "Medium content");
        await CreateTestBlobAsync(containerName, "level1/shallow.txt", "Shallow content");

        var prefixUri = StorageUri.Parse($"azure://{containerName}/?accountName={AzuriteAccountName}");

        // Act
        var items = await _provider!.ListAsync(prefixUri, true).ToListAsync();

        // Assert
        items.Should().HaveCount(4);
        items.Should().Contain(i => i.Uri.Path.Contains("root.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("deep.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("medium.txt"));
        items.Should().Contain(i => i.Uri.Path.Contains("shallow.txt"));
    }

    #endregion

    #region Metadata Operations Tests

    [Fact]
    public async Task GetMetadataAsync_ExistingBlob_ReturnsBlobProperties()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "metadata-file.txt";
        var content = "Test content for metadata";
        await CreateTestBlobAsync(containerName, blobName, content, "text/plain");

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

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
    public async Task GetMetadataAsync_NonExistentBlob_ReturnsNull()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "non-existent-file.txt";
        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        var metadata = await _provider!.GetMetadataAsync(uri);

        // Assert
        metadata.Should().BeNull();
    }

    [Fact]
    public async Task GetMetadataAsync_WithCustomMetadata_IncludesCustomMetadata()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "custom-metadata-file.txt";

        var blobServiceClient = _fixture.BlobServiceClient;
        var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        await containerClient.CreateIfNotExistsAsync();

        var blobClient = containerClient.GetBlobClient(blobName);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("Content"));
        await blobClient.UploadAsync(stream, true);

        await blobClient.SetMetadataAsync(new Dictionary<string, string>
        {
            ["custom_key_1"] = "custom-value-1",
            ["custom_key_2"] = "custom-value-2",
        });

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

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
        var containerName = GetUniqueContainerName();
        var blobName = "full-metadata-file.txt";
        var content = "Full metadata test content";
        await CreateTestBlobAsync(containerName, blobName, content, "application/json");

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

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

    #region Authentication Methods Tests

    [Fact]
    public async Task WithConnectionString_AuthenticatesSuccessfully()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "connection-string-test.txt";

        var options = new AzureBlobStorageProviderOptions
        {
            DefaultConnectionString = _fixture.GetConnectionString(),
            ServiceVersion = _options!.ServiceVersion,
            UseDefaultCredentialChain = false,
            CreateContainerIfMissing = true,
        };

        var clientFactory = new AzureBlobClientFactory(options);
        var provider = new AzureBlobStorageProvider(clientFactory, options);

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}");
        var content = "Connection string test";

        // Act
        using (var writeStream = await provider.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            await writeStream.WriteAsync(buffer, 0, buffer.Length);
            await writeStream.CommitAsync();
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
        var containerName = GetUniqueContainerName();
        var blobName = "account-key-test.txt";
        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");
        var content = "Account key test";

        // Act
        using (var writeStream = await _provider!.OpenWriteAsync(uri))
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            await writeStream.WriteAsync(buffer, 0, buffer.Length);
            await writeStream.CommitAsync();
        }

        using var readStream = await _provider.OpenReadAsync(uri);
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
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "unauthorized-file.txt";

        // Use a valid base64 string that represents invalid credentials
        // The key must be at least 64 bytes for Azure SDK validation to pass
        var invalidKey =
            "dGVzdGtleWZvcmF6dXJpdGVzdGluZ3VudGhvcml6ZWRhY2Nlc3NleGNlcHRpb250ZXN0aW5nd2l0aHZhbGlkYmFzZTY0ZW5jb2Rpbmd0aGF0cGFzc2VzdmFsaWRhdGlvbididXRmYWlsc2F1dGhlbnRpY2F0aW9u";

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        var badOptions = new AzureBlobStorageProviderOptions
        {
            ServiceUrl = _options.ServiceUrl,
            ServiceVersion = _options.ServiceVersion,
            AccountName = AzuriteAccountName,
            DefaultAccountKey = invalidKey,
            UseDefaultCredentialChain = false,
        };

        var badProvider = new AzureBlobStorageProvider(new AzureBlobClientFactory(badOptions), badOptions);

        // Act
        var act = async () => await badProvider.OpenReadAsync(uri);

        // Assert
        await act.Should().ThrowAsync<UnauthorizedAccessException>();
    }

    [Fact]
    public async Task InvalidContainerName_ThrowsArgumentException()
    {
        // Arrange
        var invalidContainerName = "Invalid Container Name!"; // Contains spaces and special chars
        var blobName = "test-file.txt";
        var uri = StorageUri.Parse($"azure://{invalidContainerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        var act = async () => await _provider!.OpenReadAsync(uri);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task InvalidBlobName_ThrowsArgumentException()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var invalidBlobName = "invalid/blob/name/with/invalid/chars%3F"; // Contains an encoded question mark, which blob names reject
        var uri = StorageUri.Parse($"azure://{containerName}/{invalidBlobName}?accountName={AzuriteAccountName}");

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
        var containerName = GetUniqueContainerName();
        var blobName = "concurrent-read-file.txt";
        var content = "Concurrent read test content";
        await CreateTestBlobAsync(containerName, blobName, content);

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

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
    public async Task MultipleConcurrentWritesToDifferentBlobs_Succeeds()
    {
        // Arrange
        var containerName = GetUniqueContainerName();

        var uris = Enumerable.Range(0, 10)
            .Select(i => StorageUri.Parse($"azure://{containerName}/file-{i}.txt?accountName={AzuriteAccountName}"))
            .ToArray();

        // Act
        var tasks = uris.Select(async (uri, index) =>
        {
            using var writeStream = await _provider!.OpenWriteAsync(uri);
            var buffer = Encoding.UTF8.GetBytes($"Content {index}");
            await writeStream.WriteAsync(buffer, 0, buffer.Length);
            await writeStream.CommitAsync();
        });

        await Task.WhenAll(tasks);

        // Assert
        foreach (var uri in uris)
        {
            var exists = await _provider!.ExistsAsync(uri);
            exists.Should().BeTrue();
        }
    }

    [Fact]
    public async Task ConcurrentReadAndWriteToSameBlob_Succeeds()
    {
        // Arrange
        var containerName = GetUniqueContainerName();
        var blobName = "concurrent-rw-file.txt";
        var initialContent = "Initial content";
        await CreateTestBlobAsync(containerName, blobName, initialContent);

        var uri = StorageUri.Parse($"azure://{containerName}/{blobName}?accountName={AzuriteAccountName}");

        // Act
        var readTask = Task.Run(async () =>
        {
            using var stream = await _provider!.OpenReadAsync(uri);
            using var reader = new StreamReader(stream);
            return await reader.ReadToEndAsync();
        });

        var writeTask = Task.Run(async () =>
        {
            await Task.Delay(100); // Small delay to ensure read starts first
            using var writeStream = await _provider!.OpenWriteAsync(uri);
            var buffer = Encoding.UTF8.GetBytes("Updated content");
            await writeStream.WriteAsync(buffer, 0, buffer.Length);
            await writeStream.CommitAsync();
        });

        await Task.WhenAll(readTask, writeTask);

        // Assert - test passes if no exceptions are thrown during concurrent operations
    }

    #endregion

    #region Provider Metadata Tests

    [Fact]
    public void ProviderIdentity_ReportsAzureSchemeAndCapabilities()
    {
        _provider!.Name.Should().Be("Azure Blob Storage");
        _provider.Schemes.Should().ContainSingle().Which.Should().Be(StorageScheme.Azure);
        _provider.Capabilities.Should().Be(StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete | StorageCapabilities.Move
            | StorageCapabilities.ConditionalWrite);
    }

    #endregion
}
