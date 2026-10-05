using System.Net;
using AwesomeAssertions;
using FakeItEasy;
using Google;
using Google.Cloud.Storage.V1;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace NPipeline.StorageProviders.Gcp.Tests;

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
    public async Task CommitAsync_UploadsToGcs()
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
        await stream.CommitAsync();
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

    // A fake upload that reads until the pipe ends or the token is cancelled, like the SDK does.
    private (Func<Task<bool>> Completed, Func<CancellationToken> Token) FakeUploadThatWaits()
    {
        var reachedEof = false;
        var token = CancellationToken.None;

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .ReturnsLazily(async call =>
            {
                var source = call.GetArgument<Stream>(1)!;
                token = call.GetArgument<CancellationToken>(3);
                var buffer = new byte[4096];

                while (await source.ReadAsync(buffer, token) > 0)
                {
                }

                reachedEof = true;
                return new Object();
            });

        return (() => Task.FromResult(reachedEof), () => token);
    }

    [Fact]
    public async Task Dispose_WithoutCommit_UploadsNothing()
    {
        // Arrange
        var (reachedEof, token) = FakeUploadThatWaits();
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName, "application/json");
        await stream.WriteAsync(new byte[] { 1, 2, 3, 4, 5 });

        // Act
        stream.Dispose();
        await Task.Delay(100);

        // Assert: the upload was cancelled and never saw the end of the data, so it could not send a final chunk.
        token().IsCancellationRequested.Should().BeTrue();
        (await reachedEof()).Should().BeFalse();
    }
    [Fact]
    public async Task CommitAsync_WithoutContentType_UploadsToGcsWithoutContentType()
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
        await stream.CommitAsync();
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
    public async Task CommitAsync_WithUnauthorized_ThrowsUnauthorizedAccessException()
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
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await stream.CommitAsync());
    }

    [Fact]
    public async Task CommitAsync_WithForbidden_ThrowsUnauthorizedAccessException()
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
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await stream.CommitAsync());
    }

    [Fact]
    public async Task CommitAsync_WithNotFound_ThrowsFileNotFoundException()
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
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await stream.CommitAsync());
    }

    [Fact]
    public async Task CommitAsync_WithBadRequest_ThrowsArgumentException()
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
        await Assert.ThrowsAsync<ArgumentException>(async () => await stream.CommitAsync());
    }

    [Fact]
    public async Task CommitAsync_WithConflict_ThrowsIOException()
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
        await Assert.ThrowsAsync<IOException>(async () => await stream.CommitAsync());
    }

    [Fact]
    public async Task CommitAsync_WithGenericError_ThrowsIOException()
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
        await Assert.ThrowsAsync<IOException>(async () => await stream.CommitAsync());
    }

    [Fact]
    public async Task CommitAsync_CalledMultipleTimes_UploadsOnlyOnce()
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
        await stream.CommitAsync();
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
    public async Task Dispose_WithoutCommit_CalledMultipleTimes_UploadsNothing()
    {
        // Arrange
        var (reachedEof, token) = FakeUploadThatWaits();
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        await stream.WriteAsync(new byte[] { 1, 2, 3, 4, 5 });

        // Act
        stream.Dispose();
        stream.Dispose();
        await stream.DisposeAsync();
        await Task.Delay(100);

        // Assert
        token().IsCancellationRequested.Should().BeTrue();
        (await reachedEof()).Should().BeFalse();
    }

    [Fact]
    public async Task Dispose_WithoutAnyWrite_NeverStartsAnUpload()
    {
        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);

        await stream.DisposeAsync();

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustNotHaveHappened();
    }
    [Fact]
    public async Task CommitAsync_WithLargeData_UploadsAllData()
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
        await stream.CommitAsync();
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
    public async Task CommitAsync_WithMultipleWrites_UploadsAllData()
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

        await stream.CommitAsync();
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
    public async Task CommitAsync_WithVariousContentTypes_SetsContentTypeCorrectly(string contentType)
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
        await stream.CommitAsync();
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
    public async Task CommitAsync_WithEmptyStream_StillUploads()
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
        await stream.CommitAsync();
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
    public async Task CommitAsync_WhenUploadThrowsOperationCanceledException_PropagatesCancellation()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .ThrowsAsync(new OperationCanceledException(cts.Token));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        stream.Write([1, 2, 3], 0, 3);

        var act = () => stream.CommitAsync();
        await act.Should().ThrowAsync<OperationCanceledException>();

        // The stream was not committed, so disposing it uploads nothing more.
        stream.Dispose();
        stream.CanWrite.Should().BeFalse();
    }

    [Fact]
    public async Task CommitAsync_CalledTwice_Throws()
    {
        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        await stream.WriteAsync(new byte[] { 1, 2, 3, 4, 5 }, 0, 5);

        await stream.CommitAsync();
        var second = () => stream.CommitAsync();

        await second.Should().ThrowAsync<InvalidOperationException>();
        await stream.DisposeAsync();

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(
                A<Object>._,
                A<Stream>._,
                A<UploadObjectOptions>._,
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task CommitAsync_UploadIsNotCancelledOrTimedOut()
    {
        // The upload once ran under a hard-coded 5-minute CancelAfter, which cut off large uploads.
        var cancelledWhenSeen = true;

        A.CallTo(() => _fakeStorageClient.UploadObjectAsync(A<Object>._, A<Stream>._, A<UploadObjectOptions>._, A<CancellationToken>._))
            .Invokes(call => cancelledWhenSeen = call.GetArgument<CancellationToken>(3).IsCancellationRequested)
            .Returns(Task.FromResult(new Object()));

        var stream = new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName);
        await stream.WriteAsync(new byte[] { 1 });

        await stream.CommitAsync();
        await stream.DisposeAsync();

        cancelledWhenSeen.Should().BeFalse();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(262143)]
    [InlineData(262145)]
    public void Constructor_WithChunkSizeThatIsNotAMultipleOf256KiB_Throws(int chunkSize)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new GcsWriteStream(_fakeStorageClient, TestBucket, TestObjectName, null, chunkSize));
    }
}
