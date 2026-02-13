#pragma warning disable IDE0005 // Unnecessary using directives
#pragma warning disable IDE0058 // Expression value is never used (assertion helpers return values)
#pragma warning disable IDE0060 // Unused parameter (test helper signatures)
#pragma warning disable IDE0160 // Convert to block-scoped namespace
#pragma warning disable IDE0161 // Convert to file-scoped namespace
#pragma warning disable IDE0290 // Use primary constructor
#pragma warning disable IDE0300 // Method can be made synchronous
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using FakeItEasy;
using FluentAssertions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

// Test AsyncPageable implementation for testing
internal sealed class TestAsyncPageable<T> : AsyncPageable<T> where T : notnull
{
    private readonly IAsyncEnumerable<T> _items;

    public TestAsyncPageable(IAsyncEnumerable<T> items)
    {
        _items = items;
    }

    public override IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = null, int? pageSizeHint = null)
    {
        return GetPagesAsync(continuationToken, pageSizeHint);
    }

    private async IAsyncEnumerable<Page<T>> GetPagesAsync(string? continuationToken, int? pageSizeHint)
    {
        _ = continuationToken;
        _ = pageSizeHint;
        var items = new List<T>();

        await foreach (var item in _items)
        {
            items.Add(item);
        }

        yield return Page<T>.FromValues(items, null, A.Fake<Response>());
    }

    public override IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return _items.GetAsyncEnumerator(cancellationToken);
    }
}

/// <summary>
///     Simple async enumerable that yields items from a list
/// </summary>
internal sealed class SimpleAsyncEnumerable<T> : IAsyncEnumerable<T>
{
    private readonly List<T> _items;

    public SimpleAsyncEnumerable(List<T> items)
    {
        _items = items;
    }

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return new SimpleAsyncEnumerator<T>(_items, cancellationToken);
    }
}

/// <summary>
///     Simple async enumerator that yields items from a list
/// </summary>
internal sealed class SimpleAsyncEnumerator<T> : IAsyncEnumerator<T>
{
    private readonly CancellationToken _cancellationToken;
    private readonly List<T> _items;
    private int _index = -1;

    public SimpleAsyncEnumerator(List<T> items, CancellationToken cancellationToken)
    {
        _items = items;
        _cancellationToken = cancellationToken;
    }

    public T Current => _items[_index];

    public ValueTask<bool> MoveNextAsync()
    {
        _cancellationToken.ThrowIfCancellationRequested();
        _index++;
        return new ValueTask<bool>(_index < _items.Count);
    }

    public ValueTask DisposeAsync()
    {
        _items.Clear();
        return default;
    }
}

/// <summary>
///     Simple wrapper to make a list enumerable as IAsyncEnumerable for testing
/// </summary>
internal sealed class AsyncEnumerableWrapper<T> : IAsyncEnumerable<T>
{
    private readonly IEnumerable<T> _items;

    public AsyncEnumerableWrapper(IEnumerable<T> items)
    {
        _items = items;
    }

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return new AsyncEnumeratorWrapper<T>(_items.GetEnumerator(), cancellationToken);
    }
}

/// <summary>
///     Simple wrapper to make a synchronous enumerator work as async enumerator for testing
/// </summary>
internal sealed class AsyncEnumeratorWrapper<T> : IAsyncEnumerator<T>
{
    private readonly CancellationToken _cancellationToken;
    private readonly IEnumerator<T> _enumerator;

    public AsyncEnumeratorWrapper(IEnumerator<T> enumerator, CancellationToken cancellationToken)
    {
        _enumerator = enumerator;
        _cancellationToken = cancellationToken;
    }

    public T Current => _enumerator.Current;

    public ValueTask<bool> MoveNextAsync()
    {
        _cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<bool>(_enumerator.MoveNext());
    }

    public ValueTask DisposeAsync()
    {
        _enumerator.Dispose();
        return default;
    }
}

/// <summary>
///     Custom async enumerable that yields a single page with items
/// </summary>
internal sealed class PageAsyncEnumerable<T> : IAsyncEnumerable<Page<T>> where T : notnull
{
    private readonly List<T> _items;

    public PageAsyncEnumerable(List<T> items)
    {
        _items = items;
    }

    public IAsyncEnumerator<Page<T>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return new PageAsyncEnumerator<T>(_items, cancellationToken);
    }
}

/// <summary>
///     Custom async enumerator that yields a single page
/// </summary>
internal sealed class PageAsyncEnumerator<T> : IAsyncEnumerator<Page<T>> where T : notnull
{
    private readonly CancellationToken _cancellationToken;
    private readonly List<T> _items;
    private bool _hasYielded;

    public PageAsyncEnumerator(List<T> items, CancellationToken cancellationToken)
    {
        _items = items;
        _cancellationToken = cancellationToken;
    }

    public Page<T> Current => !_hasYielded
        ? throw new InvalidOperationException()
        : Page<T>.FromValues(_items, null, A.Fake<Response>());

    public ValueTask<bool> MoveNextAsync()
    {
        _cancellationToken.ThrowIfCancellationRequested();

        if (_hasYielded)
            return new ValueTask<bool>(false);

        _hasYielded = true;
        return new ValueTask<bool>(true);
    }

    public ValueTask DisposeAsync()
    {
        return default;
    }
}

public class AzureBlobStorageProviderTests
{
    private readonly BlobClient _fakeBlobClient;
    private readonly BlobServiceClient _fakeBlobServiceClient;
    private readonly AzureBlobClientFactory _fakeClientFactory;
    private readonly BlobContainerClient _fakeContainerClient;
    private readonly AzureBlobStorageProviderOptions _options;
    private readonly AzureBlobStorageProvider _provider;

    public AzureBlobStorageProviderTests()
    {
        _fakeClientFactory = A.Fake<AzureBlobClientFactory>(c => c
            .WithArgumentsForConstructor([new AzureBlobStorageProviderOptions()])
            .CallsBaseMethods());

        _fakeBlobServiceClient = A.Fake<BlobServiceClient>();
        _fakeContainerClient = A.Fake<BlobContainerClient>();
        _fakeBlobClient = A.Fake<BlobClient>();
        _options = new AzureBlobStorageProviderOptions();
        _provider = new AzureBlobStorageProvider(_fakeClientFactory, _options);
    }

    [Fact]
    public void Constructor_WithNullClientFactory_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new AzureBlobStorageProvider(null!, _options));
        exception.ParamName.Should().Be("clientFactory");
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new AzureBlobStorageProvider(_fakeClientFactory, null!));
        exception.ParamName.Should().Be("options");
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesProvider()
    {
        // Act & Assert
        _provider.Should().NotBeNull();
    }

    [Fact]
    public void Scheme_ReturnsAzure()
    {
        // Act & Assert
        _provider.Scheme.Should().Be(StorageScheme.Azure);
    }

    [Fact]
    public void CanHandle_WithAzureScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://container/blob");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void CanHandle_WithFileScheme_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("file:///path/to/file");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void CanHandle_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => _provider.CanHandle(null!));
    }

    [Fact]
    public void CanHandle_WithS3Scheme_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://bucket/key");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public async Task OpenReadAsync_WithValidUri_ReturnsStream()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var responseStream = new MemoryStream([1, 2, 3, 4, 5]);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock the instance method
        A.CallTo(() => _fakeBlobClient.OpenReadAsync(A<BlobOpenReadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(call => Task.FromResult<Stream>(responseStream));

        // Act
        var stream = await _provider.OpenReadAsync(uri);

        // Assert
        stream.Should().NotBeNull();
        stream.Should().BeAssignableTo<Stream>();
    }

    [Fact]
    public async Task OpenReadAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.OpenReadAsync(null!));
    }

    [Fact]
    public async Task OpenReadAsync_WithNotFound_ThrowsFileNotFoundException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(404, "BlobNotFound", "The specified blob does not exist.", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock the instance method
        A.CallTo(() => _fakeBlobClient.OpenReadAsync(A<BlobOpenReadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithAuthenticationFailed_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(403, "AuthenticationFailed", "Server failed to authenticate the request.", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock the instance method
        A.CallTo(() => _fakeBlobClient.OpenReadAsync(A<BlobOpenReadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithAuthorizationFailed_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(403, "AuthorizationFailed", "This request is not authorized to perform this operation.", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock the instance method
        A.CallTo(() => _fakeBlobClient.OpenReadAsync(A<BlobOpenReadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithInvalidQueryParameterValue_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");

        var exception = new RequestFailedException(400, "InvalidQueryParameterValue",
            "Value for one of the query parameters specified in the request URI is invalid.", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock the instance method
        A.CallTo(() => _fakeBlobClient.OpenReadAsync(A<BlobOpenReadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithInvalidResourceName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(400, "InvalidResourceName", "The specified resource name contains invalid characters.", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock the instance method
        A.CallTo(() => _fakeBlobClient.OpenReadAsync(A<BlobOpenReadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithGenericError_ThrowsIOException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(500, "InternalError", "The server encountered an internal error.", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock the instance method
        A.CallTo(() => _fakeBlobClient.OpenReadAsync(A<BlobOpenReadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenWriteAsync_WithValidUri_ReturnsAzureBlobWriteStream()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        // Act
        var stream = await _provider.OpenWriteAsync(uri);

        // Assert
        stream.Should().NotBeNull();
        stream.Should().BeOfType<AzureBlobWriteStream>();
    }

    [Fact]
    public async Task OpenWriteAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.OpenWriteAsync(null!));
    }

    [Fact]
    public async Task OpenWriteAsync_WithContentTypeInUri_PassesContentTypeToStream()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob?contentType=application/json");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        // Act
        var stream = await _provider.OpenWriteAsync(uri);

        // Assert
        stream.Should().NotBeNull();
        stream.Should().BeOfType<AzureBlobWriteStream>();
    }

    [Fact]
    public async Task ExistsAsync_WithExistingBlob_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var response = A.Fake<Response<bool>>();
        A.CallTo(() => response.Value).Returns(true);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        A.CallTo(() => _fakeBlobClient.ExistsAsync(A<CancellationToken>._))
            .Returns(Task.FromResult(response));

        // Act
        var result = await _provider.ExistsAsync(uri);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_WithNonExistingBlob_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(404, "Blob not found", "BlobNotFound", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        A.CallTo(() => _fakeBlobClient.ExistsAsync(A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act
        var result = await _provider.ExistsAsync(uri);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public async Task ExistsAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.ExistsAsync(null!));
    }

    [Fact]
    public async Task ExistsAsync_WithAuthenticationFailed_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(403, "Authentication failed", "AuthenticationFailed", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        A.CallTo(() => _fakeBlobClient.ExistsAsync(A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.ExistsAsync(uri));
    }

    [Fact]
    public async Task ExistsAsync_WithInvalidResourceName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(400, "Invalid resource name", "InvalidResourceName", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        A.CallTo(() => _fakeBlobClient.ExistsAsync(A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.ExistsAsync(uri));
    }

    [Fact]
    public async Task DeleteAsync_WithExistingBlob_DeletesSuccessfully()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var response = A.Fake<Response<bool>>();
        A.CallTo(() => response.Value).Returns(true);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock of instance method (FakeItEasy cannot intercept extension methods)
        A.CallTo(() => _fakeBlobClient.DeleteIfExistsAsync(A<DeleteSnapshotsOption>._, A<BlobRequestConditions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(response));

        // Act
        await _provider.DeleteAsync(uri);

        // Assert
        A.CallTo(() => _fakeBlobClient.DeleteIfExistsAsync(A<DeleteSnapshotsOption>._, A<BlobRequestConditions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DeleteAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.DeleteAsync(null!));
    }

    [Fact]
    public async Task DeleteAsync_WithAuthenticationFailed_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(403, "Authentication failed", "AuthenticationFailed", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock of instance method (FakeItEasy cannot intercept extension methods)
        A.CallTo(() => _fakeBlobClient.DeleteIfExistsAsync(A<DeleteSnapshotsOption>._, A<BlobRequestConditions>._, A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.DeleteAsync(uri));
    }

    [Fact]
    public async Task DeleteAsync_WithContainerNotFound_ThrowsFileNotFoundException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(404, "Container not found", "ContainerNotFound", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock of instance method (FakeItEasy cannot intercept extension methods)
        A.CallTo(() => _fakeBlobClient.DeleteIfExistsAsync(A<DeleteSnapshotsOption>._, A<BlobRequestConditions>._, A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(() => _provider.DeleteAsync(uri));
    }

    [Fact]
    public async Task ListAsync_WithRecursiveTrue_ReturnsAllBlobs()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/prefix/");

        var blobItems = new List<BlobItem>
        {
            BlobItemBuilder("prefix/file1.txt", 100),
            BlobItemBuilder("prefix/subdir/file2.txt", 200),
        };

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.ExistsAsync(A<CancellationToken>._))
            .Returns(Task.FromResult(Response.FromValue(true, A.Fake<Response>())));

        // Create a custom AsyncPageable that properly yields items
        var asyncPageable = new TestBlobItemAsyncPageable(blobItems);

        A.CallTo(() => _fakeContainerClient.GetBlobsAsync(
                A<BlobTraits>._,
                A<BlobStates>._,
                "prefix/",
                A<CancellationToken>._))
            .Returns(asyncPageable);

        // Act
        var items = await _provider.ListAsync(uri, true).ToListAsync();

        // Assert
        items.Should().HaveCount(2);
        items.All(i => !i.IsDirectory).Should().BeTrue();
    }

    [Fact]
    public async Task ListAsync_WithRecursiveFalse_ReturnsDirectChildrenOnly()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/prefix/");
        var blobItem = BlobItemBuilder("prefix/file1.txt", 100);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.ExistsAsync(A<CancellationToken>._))
            .Returns(Task.FromResult(Response.FromValue(true, A.Fake<Response>())));

        // Create a BlobHierarchyItem with a blob (not a prefix)
        var blobHierarchyItem = BlobsModelFactory.BlobHierarchyItem(
            null,
            blobItem);

        // Create a custom AsyncPageable that properly yields items
        var asyncPageable = new TestBlobHierarchyItemAsyncPageable([blobHierarchyItem]);

        // Use more permissive matcher for call
        A.CallTo(() => _fakeContainerClient.GetBlobsByHierarchyAsync(
                A<BlobTraits>._,
                A<BlobStates>._,
                A<string>._,
                A<string>._,
                A<CancellationToken>._))
            .Returns(asyncPageable);

        // Act
        var items = await _provider.ListAsync(uri).ToListAsync();

        // Assert
        items.Should().HaveCount(1);
        items.All(i => !i.IsDirectory).Should().BeTrue();
    }

    [Fact]
    public async Task ListAsync_WithRecursiveFalse_ReturnsVirtualDirectoriesAsDirectoryItems()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/prefix/");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.ExistsAsync(A<CancellationToken>._))
            .Returns(Task.FromResult(Response.FromValue(true, A.Fake<Response>())));

        // Create a BlobHierarchyItem with a prefix (virtual directory)
        var prefixHierarchyItem = BlobsModelFactory.BlobHierarchyItem(
            "prefix/subdir/",
            null);

        // Create a BlobHierarchyItem with a blob
        var blobItem = BlobItemBuilder("prefix/file1.txt", 100);

        var blobHierarchyItem = BlobsModelFactory.BlobHierarchyItem(
            null,
            blobItem);

        // Create a custom AsyncPageable that properly yields items
        var asyncPageable = new TestBlobHierarchyItemAsyncPageable([prefixHierarchyItem, blobHierarchyItem]);

        // Use more permissive matcher for call
        A.CallTo(() => _fakeContainerClient.GetBlobsByHierarchyAsync(
                A<BlobTraits>._,
                A<BlobStates>._,
                A<string>._,
                A<string>._,
                A<CancellationToken>._))
            .Returns(asyncPageable);

        // Act
        var items = await _provider.ListAsync(uri).ToListAsync();

        // Assert
        items.Should().HaveCount(2);
        items.Count(i => i.IsDirectory).Should().Be(1);
        items.Count(i => !i.IsDirectory).Should().Be(1);

        var directory = items.FirstOrDefault(i => i.IsDirectory);
        directory.Should().NotBeNull();
        directory!.Uri.Path.Should().Be("/prefix/subdir");
        directory.Size.Should().Be(0);

        var file = items.FirstOrDefault(i => !i.IsDirectory);
        file.Should().NotBeNull();
        file!.Uri.Path.Should().Be("/prefix/file1.txt");
        file.Size.Should().Be(100);
    }

    [Fact]
    public async Task ListAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await _provider.ListAsync(null!).ToListAsync());
    }

    [Fact]
    public async Task ListAsync_WithNonExistentContainer_ReturnsEmpty()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/prefix/");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.ExistsAsync(A<CancellationToken>._))
            .Returns(Task.FromResult(Response.FromValue(false, A.Fake<Response>())));

        // Act
        var items = await _provider.ListAsync(uri, true).ToListAsync();

        // Assert
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_WithAuthenticationFailed_ThrowsRequestFailedException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/prefix/");
        var exception = new RequestFailedException(401, "Authentication failed");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.ExistsAsync(A<CancellationToken>._))
            .Returns(Task.FromResult(Response.FromValue(true, A.Fake<Response>())));

        A.CallTo(() => _fakeContainerClient.GetBlobsAsync(
                A<BlobTraits>._,
                A<BlobStates>._,
                "prefix/",
                A<CancellationToken>._))
            .Throws(exception);

        // Act & Assert
        await Assert.ThrowsAsync<RequestFailedException>(async () => await _provider.ListAsync(uri, true).ToListAsync());
    }

    [Fact]
    public async Task GetMetadataAsync_WithExistingBlob_ReturnsMetadata()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");

        var properties = BlobsModelFactory.BlobProperties(
            contentLength: 1024,
            lastModified: DateTimeOffset.UtcNow,
            contentType: "application/json",
            metadata: new Dictionary<string, string>
            {
                ["custom-key"] = "custom-value",
            });

        var response = A.Fake<Response<BlobProperties>>();
        A.CallTo(() => response.Value).Returns(properties);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock of instance method (FakeItEasy cannot intercept extension methods)
        A.CallTo(() => _fakeBlobClient.GetPropertiesAsync(A<BlobRequestConditions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(response));

        // Act
        var metadata = await _provider.GetMetadataAsync(uri);

        // Assert
        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(1024);
        metadata.ContentType.Should().Be("application/json");
        metadata.IsDirectory.Should().BeFalse();
        metadata.CustomMetadata.Should().ContainKey("custom-key").WhoseValue.Should().Be("custom-value");
    }

    [Fact]
    public async Task GetMetadataAsync_WithNonExistingBlob_ReturnsNull()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(404, "Blob not found", "BlobNotFound", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock of instance method (FakeItEasy cannot intercept extension methods)
        A.CallTo(() => _fakeBlobClient.GetPropertiesAsync(A<BlobRequestConditions>._, A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act
        var metadata = await _provider.GetMetadataAsync(uri);

        // Assert
        metadata.Should().BeNull();
    }

    [Fact]
    public async Task GetMetadataAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.GetMetadataAsync(null!));
    }

    [Fact]
    public async Task GetMetadataAsync_WithAuthenticationFailed_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://test-container/test-blob");
        var exception = new RequestFailedException(403, "Authentication failed", "AuthenticationFailed", null);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient("test-container"))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient("test-blob"))
            .Returns(_fakeBlobClient);

        // Mock of instance method (FakeItEasy cannot intercept extension methods)
        A.CallTo(() => _fakeBlobClient.GetPropertiesAsync(A<BlobRequestConditions>._, A<CancellationToken>._))
            .ThrowsAsync(exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.GetMetadataAsync(uri));
    }

    [Fact]
    public void GetMetadata_ReturnsCorrectProviderMetadata()
    {
        // Act
        var metadata = _provider.GetMetadata();

        // Assert
        metadata.Should().NotBeNull();
        metadata.Name.Should().Be("Azure Blob Storage");
        metadata.SupportedSchemes.Should().Contain("azure");
        metadata.SupportsRead.Should().BeTrue();
        metadata.SupportsWrite.Should().BeTrue();
        metadata.SupportsDelete.Should().BeTrue();
        metadata.SupportsListing.Should().BeTrue();
        metadata.SupportsMetadata.Should().BeTrue();
        metadata.SupportsHierarchy.Should().BeFalse();
        metadata.Capabilities["blockBlobUploadThresholdBytes"].Should().Be(64 * 1024 * 1024);
        metadata.Capabilities["supportsServiceUrl"].Should().Be(true);
        metadata.Capabilities["supportsConnectionString"].Should().Be(true);
        metadata.Capabilities["supportsSasToken"].Should().Be(true);
        metadata.Capabilities["supportsAccountKey"].Should().Be(true);
        metadata.Capabilities["supportsDefaultCredentialChain"].Should().Be(true);
    }

    [Fact]
    public void GetMetadata_WithCustomOptions_ReturnsCorrectCapabilities()
    {
        // Arrange
        var customOptions = new AzureBlobStorageProviderOptions
        {
            BlockBlobUploadThresholdBytes = 128 * 1024 * 1024,
        };

        var customProvider = new AzureBlobStorageProvider(_fakeClientFactory, customOptions);

        // Act
        var metadata = customProvider.GetMetadata();

        // Assert
        metadata.Capabilities["blockBlobUploadThresholdBytes"].Should().Be(128 * 1024 * 1024);
    }

    [Fact]
    public async Task OpenReadAsync_WithEmptyContainerName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure:///test-blob");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenWriteAsync_WithEmptyContainerName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure:///test-blob");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenWriteAsync(uri));
    }

    [Fact]
    public async Task ExistsAsync_WithEmptyContainerName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure:///test-blob");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.ExistsAsync(uri));
    }

    [Fact]
    public async Task GetMetadataAsync_WithEmptyContainerName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure:///test-blob");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.GetMetadataAsync(uri));
    }

    [Fact]
    public async Task ListAsync_WithEmptyContainerName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure:///prefix/");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () => await _provider.ListAsync(uri).ToListAsync());
    }

    [Theory]
    [InlineData("azure://container/blob", "container", "blob")]
    [InlineData("azure://container/path/to/blob", "container", "path/to/blob")]
    public async Task OpenReadAsync_ParsesContainerAndBlobCorrectly(string uriString, string expectedContainer, string expectedBlob)
    {
        // Arrange
        var uri = StorageUri.Parse(uriString);
        var responseStream = new MemoryStream([1, 2, 3, 4, 5]);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(expectedContainer))
            .Returns(_fakeContainerClient);

        A.CallTo(() => _fakeContainerClient.GetBlobClient(expectedBlob))
            .Returns(_fakeBlobClient);

        // Mock the instance method with null options (FakeItEasy cannot intercept extension methods)
        A.CallTo(() => _fakeBlobClient.OpenReadAsync(null, A<CancellationToken>._))
            .ReturnsLazily(call => Task.FromResult<Stream>(responseStream));

        // Act
        var stream = await _provider.OpenReadAsync(uri);

        // Assert
        stream.Should().NotBeNull();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(expectedContainer))
            .MustHaveHappenedOnceExactly();

        A.CallTo(() => _fakeContainerClient.GetBlobClient(expectedBlob))
            .MustHaveHappenedOnceExactly();
    }

    [Theory]
    [InlineData("azure://container/")]
    [InlineData("azure://container")]
    public async Task OpenReadAsync_EmptyBlobPath_ThrowsArgumentException(string uriString)
    {
        var uri = StorageUri.Parse(uriString);

        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    private static BlobItem BlobItemBuilder(string name, long contentLength)
    {
        var properties = BlobsModelFactory.BlobItemProperties(
            false,
            contentLength: contentLength);

        return BlobsModelFactory.BlobItem(
            name,
            properties: properties);
    }

    /// <summary>
    ///     Custom AsyncPageable for BlobItem that properly yields items
    /// </summary>
    internal sealed class TestBlobItemAsyncPageable : AsyncPageable<BlobItem>
    {
        private readonly IEnumerable<BlobItem> _items;

        public TestBlobItemAsyncPageable(IEnumerable<BlobItem> items)
        {
            _items = items;
        }

        public override IAsyncEnumerable<Page<BlobItem>> AsPages(string? continuationToken = null, int? pageSizeHint = null)
        {
            return GetPagesAsync(continuationToken, pageSizeHint);
        }

        private async IAsyncEnumerable<Page<BlobItem>> GetPagesAsync(string? continuationToken, int? pageSizeHint)
        {
            _ = continuationToken;
            _ = pageSizeHint;
            await Task.CompletedTask;
            var items = _items.ToList();
            yield return Page<BlobItem>.FromValues(items, null, A.Fake<Response>());
        }

        public override IAsyncEnumerator<BlobItem> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new TestBlobItemAsyncEnumerator(_items, cancellationToken);
        }
    }

    /// <summary>
    ///     Custom async enumerator for BlobItem
    /// </summary>
    internal sealed class TestBlobItemAsyncEnumerator : IAsyncEnumerator<BlobItem>
    {
        private readonly CancellationToken _cancellationToken;
        private readonly IEnumerator<BlobItem> _enumerator;

        public TestBlobItemAsyncEnumerator(IEnumerable<BlobItem> items, CancellationToken cancellationToken)
        {
            _enumerator = items.GetEnumerator();
            _cancellationToken = cancellationToken;
        }

        public BlobItem Current => _enumerator.Current;

        public ValueTask<bool> MoveNextAsync()
        {
            _cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<bool>(_enumerator.MoveNext());
        }

        public ValueTask DisposeAsync()
        {
            _enumerator.Dispose();
            return default;
        }
    }

    /// <summary>
    ///     Custom AsyncPageable for BlobHierarchyItem that properly yields items
    /// </summary>
    internal sealed class TestBlobHierarchyItemAsyncPageable : AsyncPageable<BlobHierarchyItem>
    {
        private readonly IEnumerable<BlobHierarchyItem> _items;

        public TestBlobHierarchyItemAsyncPageable(IEnumerable<BlobHierarchyItem> items)
        {
            _items = items;
        }

        public override IAsyncEnumerable<Page<BlobHierarchyItem>> AsPages(string? continuationToken = null, int? pageSizeHint = null)
        {
            return GetPagesAsync(continuationToken, pageSizeHint);
        }

        private async IAsyncEnumerable<Page<BlobHierarchyItem>> GetPagesAsync(string? continuationToken, int? pageSizeHint)
        {
            _ = continuationToken;
            _ = pageSizeHint;
            await Task.CompletedTask;
            var items = _items.ToList();
            yield return Page<BlobHierarchyItem>.FromValues(items, null, A.Fake<Response>());
        }

        public override IAsyncEnumerator<BlobHierarchyItem> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new TestBlobHierarchyItemAsyncEnumerator(_items, cancellationToken);
        }
    }

    /// <summary>
    ///     Custom async enumerator for BlobHierarchyItem
    /// </summary>
    internal sealed class TestBlobHierarchyItemAsyncEnumerator : IAsyncEnumerator<BlobHierarchyItem>
    {
        private readonly CancellationToken _cancellationToken;
        private readonly IEnumerator<BlobHierarchyItem> _enumerator;

        public TestBlobHierarchyItemAsyncEnumerator(IEnumerable<BlobHierarchyItem> items, CancellationToken cancellationToken)
        {
            _enumerator = items.GetEnumerator();
            _cancellationToken = cancellationToken;
        }

        public BlobHierarchyItem Current => _enumerator.Current;

        public ValueTask<bool> MoveNextAsync()
        {
            _cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<bool>(_enumerator.MoveNext());
        }

        public ValueTask DisposeAsync()
        {
            _enumerator.Dispose();
            return default;
        }
    }
}
