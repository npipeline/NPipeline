using AwesomeAssertions;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using FakeItEasy;
using NPipeline.StorageProviders.Exceptions;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

public class AdlsGen2WriteStreamTests
{
    private readonly BlobClient _blobClient;
    private readonly BlobContainerClient _containerClient;
    private readonly BlobServiceClient _serviceClient;

    public AdlsGen2WriteStreamTests()
    {
        _serviceClient = A.Fake<BlobServiceClient>();
        _containerClient = A.Fake<BlobContainerClient>();
        _blobClient = A.Fake<BlobClient>();

        A.CallTo(() => _serviceClient.GetBlobContainerClient("filesystem")).Returns(_containerClient);
        A.CallTo(() => _containerClient.GetBlobClient("path/file.txt")).Returns(_blobClient);
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesStream()
    {
        // Act
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        // Assert
        stream.Should().NotBeNull();
        stream.CanWrite.Should().BeTrue();
    }

    [Fact]
    public void Constructor_WithNullServiceClient_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AdlsGen2WriteStream(
            null!,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null));
    }

    [Fact]
    public void Constructor_WithNullFilesystem_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AdlsGen2WriteStream(
            _serviceClient,
            null!,
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null));
    }

    [Fact]
    public void Constructor_WithNullPath_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            null!,
            "text/plain",
            64 * 1024 * 1024,
            null,
            null));
    }

    [Fact]
    public void Write_WritesToTempStream()
    {
        // Arrange
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        var data = "Hello, World!"u8.ToArray();

        // Act
        stream.Write(data, 0, data.Length);

        // Assert
        stream.Length.Should().Be(data.Length);
    }

    [Fact]
    public async Task WriteAsync_WritesToTempStream()
    {
        // Arrange
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        var data = "Hello, World!"u8.ToArray();

        // Act
        await stream.WriteAsync(data);

        // Assert
        stream.Length.Should().Be(data.Length);
    }

    [Fact]
    public void Write_WithNullBuffer_ThrowsArgumentNullException()
    {
        // Arrange
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => stream.Write(null!, 0, 0));
    }

    [Fact]
    public void Seek_ThrowsNotSupportedException()
    {
        // Arrange
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Seek(0, SeekOrigin.Begin));
    }

    [Fact]
    public void SetLength_ThrowsNotSupportedException()
    {
        // Arrange
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.SetLength(100));
    }

    [Fact]
    public void Read_ThrowsNotSupportedException()
    {
        // Arrange
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        var buffer = new byte[100];

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Read(buffer, 0, 100));
    }

    [Fact]
    public void CanRead_ReturnsFalse()
    {
        // Arrange
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        // Assert
        stream.CanRead.Should().BeFalse();
    }

    [Fact]
    public void CanSeek_ReturnsFalse()
    {
        // Arrange
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        // Assert
        stream.CanSeek.Should().BeFalse();
    }

    [Fact]
    public void Flush_DoesNotThrow()
    {
        // Arrange
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        // Act & Assert - should not throw
        stream.Flush();
    }

    [Fact]
    public async Task FlushAsync_DoesNotThrow()
    {
        // Arrange
        using var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        // Act & Assert - should not throw
        await stream.FlushAsync();
    }

    [Fact]
    public async Task CommitAsync_UploadsFile()
    {
        // Arrange
        A.CallTo(() => _containerClient.CreateIfNotExistsAsync(A<PublicAccessType>._, A<IDictionary<string, string>?>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<Response<BlobContainerInfo>>()));

        A.CallTo(() => _blobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions?>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        var data = "Hello, World!"u8.ToArray();
        await stream.WriteAsync(data);

        // Act
        await stream.CommitAsync();
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _blobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions?>._, A<CancellationToken>._))
            .MustHaveHappened();
    }

    [Fact]
    public void Dispose_WithoutCommit_UploadsNothing()
    {
        // Arrange
        A.CallTo(() => _containerClient.CreateIfNotExistsAsync(A<PublicAccessType>._, A<IDictionary<string, string>?>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<Response<BlobContainerInfo>>()));

        A.CallTo(() => _blobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions?>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        var data = "Hello, World!"u8.ToArray();
        stream.Write(data, 0, data.Length);

        // Act
        stream.Dispose();

        // Assert
        A.CallTo(() => _blobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions?>._, A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task Dispose_CalledMultipleTimes_DoesNotThrow()
    {
        // Arrange
        A.CallTo(() => _containerClient.CreateIfNotExistsAsync(A<PublicAccessType>._, A<IDictionary<string, string>?>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<Response<BlobContainerInfo>>()));

        A.CallTo(() => _blobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions?>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AdlsGen2WriteStream(
            _serviceClient,
            "filesystem",
            "path/file.txt",
            "text/plain",
            64 * 1024 * 1024,
            null,
            null);

        // Act & Assert - should not throw
        stream.Dispose();
        stream.Dispose();
        await stream.DisposeAsync();
    }

    [Fact]
    public async Task CommitAsync_WithoutCallerToken_UploadHasNoTimeout()
    {
        // The upload used to run under a hard-coded 5-minute CancelAfter, which cut off large uploads.
        CancellationToken uploadToken = default;

        A.CallTo(() => _containerClient.CreateIfNotExistsAsync(A<PublicAccessType>._, A<IDictionary<string, string>?>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<Response<BlobContainerInfo>>()));

        A.CallTo(() => _blobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions?>._, A<CancellationToken>._))
            .Invokes((Stream _, BlobUploadOptions? _, CancellationToken token) => uploadToken = token)
            .Returns(Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AdlsGen2WriteStream(_serviceClient, "filesystem", "path/file.txt");
        await stream.WriteAsync("x"u8.ToArray());

        await stream.CommitAsync();
        await stream.DisposeAsync();

        uploadToken.CanBeCanceled.Should().BeFalse();
    }

    [Fact]
    public async Task CommitAsync_WithIfMatch_SendsTheETagAsACondition()
    {
        A.CallTo(() => _blobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions?>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AdlsGen2WriteStream(_serviceClient, "filesystem", "path/file.txt", ifMatch: "\"0x8DC\"");
        await stream.WriteAsync(new byte[] { 1 });

        await stream.CommitAsync();

        A.CallTo(() => _blobClient.UploadAsync(
                A<Stream>._,
                A<BlobUploadOptions?>.That.Matches(o => o!.Conditions != null && o.Conditions.IfMatch == new global::Azure.ETag("\"0x8DC\"")),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task CommitAsync_WithOverwriteFalse_SendsIfNoneMatchStar()
    {
        A.CallTo(() => _blobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions?>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AdlsGen2WriteStream(_serviceClient, "filesystem", "path/file.txt", overwrite: false);
        await stream.WriteAsync(new byte[] { 1 });

        await stream.CommitAsync();

        A.CallTo(() => _blobClient.UploadAsync(
                A<Stream>._,
                A<BlobUploadOptions?>.That.Matches(o => o!.Conditions != null && o.Conditions.IfNoneMatch == global::Azure.ETag.All),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task CommitAsync_WhenTheConditionFails_ThrowsStoragePreconditionFailedException()
    {
        A.CallTo(() => _blobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions?>._, A<CancellationToken>._))
            .Throws(new RequestFailedException(412, "failed", "ConditionNotMet", null));

        var stream = new AdlsGen2WriteStream(_serviceClient, "filesystem", "path/file.txt", ifMatch: "\"stale\"");
        await stream.WriteAsync(new byte[] { 1 });

        await Assert.ThrowsAsync<StoragePreconditionFailedException>(async () => await stream.CommitAsync());
    }
}
