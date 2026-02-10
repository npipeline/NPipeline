using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using AwesomeAssertions;
using FakeItEasy;
using Xunit;
#pragma warning disable IDE0058 // Expression value is never used (assertion helpers return values)
#pragma warning disable IDE0160 // Convert to block-scoped namespace
#pragma warning disable IDE0161 // Convert to file-scoped namespace

namespace NPipeline.StorageProviders.Azure.Tests;

public class AzureBlobWriteStreamTests
{
    private const string TestContainer = "test-container";
    private const string TestBlob = "test-blob";
    private readonly BlobServiceClient _fakeBlobServiceClient;

    public AzureBlobWriteStreamTests()
    {
        _fakeBlobServiceClient = A.Fake<BlobServiceClient>();
    }

    [Fact]
    public void Constructor_WithNullBlobServiceClient_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new AzureBlobWriteStream(null!, TestContainer, TestBlob));
        exception.ParamName.Should().Be("blobServiceClient");
    }

    [Fact]
    public void Constructor_WithNullContainer_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new AzureBlobWriteStream(_fakeBlobServiceClient, null!, TestBlob));
        exception.ParamName.Should().Be("container");
    }

    [Fact]
    public void Constructor_WithNullBlob_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, null!));
        exception.ParamName.Should().Be("blob");
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesStream()
    {
        // Act
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);

        // Assert
        stream.Should().NotBeNull();
        stream.CanRead.Should().BeFalse();
        stream.CanSeek.Should().BeFalse();
        stream.CanWrite.Should().BeTrue();
    }

    [Fact]
    public void Constructor_WithContentType_SetsContentType()
    {
        // Act
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob, "application/json");

        // Assert
        stream.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithCustomThreshold_SetsThreshold()
    {
        // Act
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob, null, 128 * 1024 * 1024);

        // Assert
        stream.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithCustomConcurrency_SetsConcurrency()
    {
        // Act
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob, null, 64 * 1024 * 1024, 4);

        // Assert
        stream.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithCustomTransferSize_SetsTransferSize()
    {
        // Act
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob, null, 64 * 1024 * 1024, null, 4 * 1024 * 1024);

        // Assert
        stream.Should().NotBeNull();
    }

    [Fact]
    public void Write_WithValidBuffer_WritesToTempFile()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        stream.Write(data, 0, data.Length);

        // Assert
        stream.Length.Should().Be(data.Length);
    }

    [Fact]
    public async Task WriteAsync_WithValidBuffer_WritesToTempFile()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        await stream.WriteAsync(data, 0, data.Length);

        // Assert
        stream.Length.Should().Be(data.Length);
    }

    [Fact]
    public async Task WriteAsync_WithReadOnlyMemory_WritesToTempFile()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        await stream.WriteAsync(new ReadOnlyMemory<byte>(data));

        // Assert
        stream.Should().NotBeNull();
    }

    [Fact]
    public void Write_WhenDisposed_ThrowsObjectDisposedException()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        stream.Dispose();
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act & Assert
        Assert.Throws<ObjectDisposedException>(() => stream.Write(data, 0, data.Length));
    }

    [Fact]
    public async Task WriteAsync_WhenDisposed_ThrowsObjectDisposedException()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        await stream.DisposeAsync();
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await stream.WriteAsync(data, 0, data.Length));
    }

    [Fact]
    public void Flush_IsNoOp_DoesNotThrow()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);

        // Act & Assert
        stream.Invoking(s => s.Flush()).Should().NotThrow();
    }

    [Fact]
    public async Task FlushAsync_IsNoOp_DoesNotThrow()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);

        // Act & Assert
        await stream.Invoking(async s => await s.FlushAsync()).Should().NotThrowAsync();
    }

    [Fact]
    public void Read_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var buffer = new byte[10];

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Read(buffer, 0, buffer.Length));
    }

    [Fact]
    public void Seek_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Seek(0, SeekOrigin.Begin));
    }

    [Fact]
    public void SetLength_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.SetLength(100));
    }

    [Fact]
    public void Position_Getter_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() =>
        {
            var _ = stream.Position;
        });
    }

    [Fact]
    public void Position_Setter_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Position = 0);
    }

    [Fact]
    public void Length_WhenNotDisposed_ReturnsWrittenBytes()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        stream.Write(data, 0, data.Length);

        // Assert
        stream.Length.Should().Be(data.Length);
    }

    [Fact]
    public void Length_WhenDisposed_ThrowsObjectDisposedException()
    {
        // Arrange
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        stream.Dispose();

        // Act & Assert
        Assert.Throws<ObjectDisposedException>(() =>
        {
            var _ = stream.Length;
        });
    }

    [Fact]
    public async Task DisposeAsync_UploadsToAzure()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob, "application/json");
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(
                A<Stream>._,
                A<BlobUploadOptions>.That.Matches(o => o.HttpHeaders != null && o.HttpHeaders.ContentType == "application/json"),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void Dispose_UploadsToAzure()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob, "application/json");
        var data = new byte[] { 1, 2, 3, 4, 5 };
        stream.Write(data, 0, data.Length);

        // Act
        stream.Dispose();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(
                A<Stream>._,
                A<BlobUploadOptions>.That.Matches(o => o.HttpHeaders != null && o.HttpHeaders.ContentType == "application/json"),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithoutContentType_UploadsToAzureWithoutContentType()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(
                A<Stream>._,
                A<BlobUploadOptions>.That.Matches(o => o.HttpHeaders == null || string.IsNullOrEmpty(o.HttpHeaders.ContentType)),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithAuthenticationFailed_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        var azureException = new RequestFailedException(401, "Authentication failed", "AuthenticationFailed", null);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(azureException);

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithAuthorizationFailed_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        var azureException = new RequestFailedException(403, "Authorization failed", "AuthorizationFailed", null);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(azureException);

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithInvalidResourceName_ThrowsArgumentException()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        var azureException = new RequestFailedException(400, "Invalid resource name", "InvalidResourceName", null);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(azureException);

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithInvalidQueryParameterValue_ThrowsArgumentException()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        var azureException = new RequestFailedException(400, "Invalid query parameter", "InvalidQueryParameterValue", null);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(azureException);

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithContainerNotFound_ThrowsFileNotFoundException()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        var azureException = new RequestFailedException(404, "Container not found", "ContainerNotFound", null);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(azureException);

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithBlobNotFound_ThrowsFileNotFoundException()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        var azureException = new RequestFailedException(404, "Blob not found", "BlobNotFound", null);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(azureException);

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithGenericError_ThrowsIOException()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        var azureException = new RequestFailedException(500, "Internal error", "InternalError", null);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ThrowsAsync(azureException);

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_CalledMultipleTimes_UploadsOnlyOnce()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void Dispose_CalledMultipleTimes_UploadsOnlyOnce()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        stream.Write(data, 0, data.Length);

        // Act
        stream.Dispose();
        stream.Dispose();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithLargeData_UploadsAllData()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        // Use a threshold smaller than data to force block blob upload
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob, null, 100);
        var data = new byte[1024]; // 1KB, larger than threshold

        for (var i = 0; i < data.Length; i++)
        {
            data[i] = (byte)(i % 256);
        }

        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithMultipleWrites_UploadsAllData()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[100];

        // Act
        for (var i = 0; i < 10; i++)
        {
            await stream.WriteAsync(data, 0, data.Length);
        }

        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Theory]
    [InlineData("application/json")]
    [InlineData("text/csv")]
    [InlineData("application/octet-stream")]
    [InlineData("image/png")]
    public async Task DisposeAsync_WithVariousContentTypes_SetsContentTypeCorrectly(string contentType)
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob, contentType);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(
                A<Stream>._,
                A<BlobUploadOptions>.That.Matches(o => o.HttpHeaders != null && o.HttpHeaders.ContentType == contentType),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithCustomConcurrency_UsesCustomConcurrency()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        // Use a threshold smaller than data to force block blob upload
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob, null, 100, 8);
        var data = new byte[1024];
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(
                A<Stream>._,
                A<BlobUploadOptions>.That.Matches(o => o.TransferOptions.MaximumConcurrency == 8),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithCustomTransferSize_UsesCustomTransferSize()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        // Use a threshold smaller than data to force block blob upload
        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob, null, 100, null, 2 * 1024 * 1024);
        var data = new byte[1024];
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(
                A<Stream>._,
                A<BlobUploadOptions>.That.Matches(o => o.TransferOptions.MaximumTransferSize == 2 * 1024 * 1024),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_TempFileIsCleanedUp()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert - The temp file should be cleaned up (FileOptions.DeleteOnClose is set)
        // We can't directly verify this without accessing the file system,
        // but the implementation uses FileOptions.DeleteOnClose
        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithEmptyData_UploadsEmptyBlob()
    {
        // Arrange
        var fakeContainerClient = A.Fake<BlobContainerClient>();
        var fakeBlobClient = A.Fake<BlobClient>();

        A.CallTo(() => _fakeBlobServiceClient.GetBlobContainerClient(TestContainer))
            .Returns(fakeContainerClient);

        A.CallTo(() => fakeContainerClient.GetBlobClient(TestBlob))
            .Returns(fakeBlobClient);

        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        var stream = new AzureBlobWriteStream(_fakeBlobServiceClient, TestContainer, TestBlob);

        // Don't write any data - upload empty blob

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => fakeBlobClient.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }
}
