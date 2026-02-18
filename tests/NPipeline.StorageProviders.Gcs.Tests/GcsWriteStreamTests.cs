using System.Net;
using AwesomeAssertions;
using FakeItEasy;
using Google;
using Google.Cloud.Storage.V1;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace NPipeline.StorageProviders.Gcs.Tests;

public class GcsWriteStreamTests
{
    private const string TestBucket = "test-bucket";
    private const string TestObjectName = "test-object.txt";
    private readonly StorageClient _fakeStorageClient;

    public GcsWriteStreamTests()
    {
        _fakeStorageClient = A.Fake<StorageClient>();
    }

    [Fact]
    public void Constructor_WithNullStorageClient_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new GcsWriteStream(null!, TestBucket, TestObjectName));
        exception.ParamName.Should().Be("storageClient");
    }

    [Fact]
    public void Constructor_WithNullBucket_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new GcsWriteStream(_fakeStorageClient, null!, TestObjectName));
        exception.ParamName.Should().Be("bucket");
    }

    [Fact]
    public void Constructor_WithNullObjectName_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new GcsWriteStream(_fakeStorageClient, TestBucket, null!));
        exception.ParamName.Should().Be("objectName");
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesStream()
    {
        // Act
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);

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
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName, "application/json");

        // Assert
        stream.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithCustomChunkSize_SetsChunkSize()
    {
        // Act
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName, null, 32 * 1024 * 1024);

        // Assert
        stream.Should().NotBeNull();
    }

    [Fact]
    public void Write_WithValidBuffer_WritesToTempFile()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
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
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
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
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
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
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        stream.Dispose();
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act & Assert
        Assert.Throws<ObjectDisposedException>(() => stream.Write(data, 0, data.Length));
    }

    [Fact]
    public async Task WriteAsync_WhenDisposed_ThrowsObjectDisposedException()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        await stream.DisposeAsync();
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await stream.WriteAsync(data, 0, data.Length));
    }

    [Fact]
    public void Flush_IsNoOp_DoesNotThrow()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);

        // Act & Assert
        stream.Invoking(s => s.Flush()).Should().NotThrow();
    }

    [Fact]
    public async Task FlushAsync_IsNoOp_DoesNotThrow()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);

        // Act & Assert
        await stream.Invoking(async s => await s.FlushAsync()).Should().NotThrowAsync();
    }

    [Fact]
    public void Read_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var buffer = new byte[10];

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Read(buffer, 0, buffer.Length));
    }

    [Fact]
    public void Seek_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Seek(0, SeekOrigin.Begin));
    }

    [Fact]
    public void SetLength_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.SetLength(100));
    }

    [Fact]
    public void Position_Getter_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);

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
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Position = 0);
    }

    [Fact]
    public void Length_WhenNotDisposed_ReturnsWrittenBytes()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
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
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        stream.Dispose();

        // Act & Assert
        Assert.Throws<ObjectDisposedException>(() =>
        {
            var _ = stream.Length;
        });
    }

    [Fact]
    public async Task DisposeAsync_UploadsToGcs()
    {
        // Arrange
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName, "application/json");
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>.That.Matches(o =>
                    o.Bucket == TestBucket &&
                    o.Name == TestObjectName &&
                    o.ContentType == "application/json"),
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void Dispose_UploadsToGcs()
    {
        // Arrange
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName, "application/json");
        var data = new byte[] { 1, 2, 3, 4, 5 };
        stream.Write(data, 0, data.Length);

        // Act
        stream.Dispose();

        // Assert
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>.That.Matches(o =>
                    o.Bucket == TestBucket &&
                    o.Name == TestObjectName &&
                    o.ContentType == "application/json"),
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithoutContentType_UploadsToGcsWithoutContentType()
    {
        // Arrange
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>.That.Matches(o =>
                    o.Bucket == TestBucket &&
                    o.Name == TestObjectName &&
                    string.IsNullOrEmpty(o.ContentType)),
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithUnauthorized_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var gcsException = new GoogleApiException("storage", "Unauthorized")
        {
            HttpStatusCode = HttpStatusCode.Unauthorized,
        };

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithForbidden_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var gcsException = new GoogleApiException("storage", "Forbidden")
        {
            HttpStatusCode = HttpStatusCode.Forbidden,
        };

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithNotFound_ThrowsFileNotFoundException()
    {
        // Arrange
        var gcsException = new GoogleApiException("storage", "Not found")
        {
            HttpStatusCode = HttpStatusCode.NotFound,
        };

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithBadRequest_ThrowsArgumentException()
    {
        // Arrange
        var gcsException = new GoogleApiException("storage", "Bad request")
        {
            HttpStatusCode = HttpStatusCode.BadRequest,
        };

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithConflict_ThrowsIOException()
    {
        // Arrange
        var gcsException = new GoogleApiException("storage", "Conflict")
        {
            HttpStatusCode = HttpStatusCode.Conflict,
        };

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithGenericError_ThrowsGcsStorageException()
    {
        // Arrange
        var gcsException = new GoogleApiException("storage", "Internal error")
        {
            HttpStatusCode = HttpStatusCode.InternalServerError,
        };

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<GcsStorageException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_CalledMultipleTimes_UploadsOnlyOnce()
    {
        // Arrange
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void Dispose_CalledMultipleTimes_UploadsOnlyOnce()
    {
        // Arrange
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        stream.Write(data, 0, data.Length);

        // Act
        stream.Dispose();
        stream.Dispose();

        // Assert
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithLargeData_UploadsAllData()
    {
        // Arrange
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[1024 * 1024]; // 1MB

        for (var i = 0; i < data.Length; i++)
        {
            data[i] = (byte)(i % 256);
        }

        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithMultipleWrites_UploadsAllData()
    {
        // Arrange
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[100];

        // Act
        for (var i = 0; i < 10; i++)
        {
            await stream.WriteAsync(data, 0, data.Length);
        }

        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
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
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName, contentType);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>.That.Matches(o => o.ContentType == contentType),
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void CanWrite_AfterDispose_ReturnsFalse()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        stream.Dispose();

        // Act & Assert
        stream.CanWrite.Should().BeFalse();
    }

    [Fact]
    public async Task CanWrite_AfterDisposeAsync_ReturnsFalse()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        await stream.DisposeAsync();

        // Act & Assert
        stream.CanWrite.Should().BeFalse();
    }

    [Fact]
    public void Write_WithMultipleChunks_WritesAllData()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data1 = new byte[] { 1, 2, 3 };
        var data2 = new byte[] { 4, 5, 6 };
        var data3 = new byte[] { 7, 8, 9 };

        // Act
        stream.Write(data1, 0, data1.Length);
        stream.Write(data2, 0, data2.Length);
        stream.Write(data3, 0, data3.Length);

        // Assert
        stream.Length.Should().Be(9);
    }

    [Fact]
    public async Task WriteAsync_WithCancellationToken_PassesTokenCorrectly()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        await stream.WriteAsync(data, 0, data.Length, cts.Token);

        // Assert
        stream.Length.Should().Be(data.Length);
    }

    [Fact]
    public async Task DisposeAsync_WithEmptyStream_StillUploads()
    {
        // Arrange
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);

        // Act - No data written
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void Write_WithOffsetAndCount_WritesCorrectPortion()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        // Act - Write only bytes 2-6 (indices 2-6, count 5)
        stream.Write(data, 2, 5);

        // Assert
        stream.Length.Should().Be(5);
    }

    [Fact]
    public async Task WriteAsync_WithOffsetAndCount_WritesCorrectPortion()
    {
        // Arrange
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        // Act - Write only bytes 2-6 (indices 2-6, count 5)
        await stream.WriteAsync(data, 2, 5);

        // Assert
        stream.Length.Should().Be(5);
    }

    [Fact]
    public void Dispose_WhenUploadThrowsOperationCanceledException_PropagatesCancellation()
    {
        // Arrange - upload cancels during synchronous Dispose
        var cts = new CancellationTokenSource();
        cts.Cancel();

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .ThrowsAsync(new OperationCanceledException(cts.Token));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        stream.Write(new byte[] { 1, 2, 3 }, 0, 3);

        // Act & Assert - OperationCanceledException propagates even from sync Dispose
        var act = () => stream.Dispose();
        act.Should().Throw<OperationCanceledException>();

        // Stream should be in disposed state after the throw
        stream.CanWrite.Should().BeFalse();
    }

    [Fact]
    public async Task DisposeAsync_CalledAfterDispose_IsIdempotent()
    {
        // Arrange
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act - mix async and sync dispose
        stream.Dispose();
        await stream.DisposeAsync();

        // Assert - upload only happened once
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }
}
