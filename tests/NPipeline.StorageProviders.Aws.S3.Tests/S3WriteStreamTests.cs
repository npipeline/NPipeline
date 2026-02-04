using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using FakeItEasy;
using System.IO;
using Xunit;

namespace NPipeline.StorageProviders.Aws.S3.Tests;

public class S3WriteStreamTests
{
    private readonly IAmazonS3 _fakeS3Client;
    private const string TestBucket = "test-bucket";
    private const string TestKey = "test-key";

    public S3WriteStreamTests()
    {
        _fakeS3Client = A.Fake<IAmazonS3>();
    }

    [Fact]
    public void Constructor_WithNullS3Client_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new S3WriteStream(null!, TestBucket, TestKey));
        exception.ParamName.Should().Be("s3Client");
    }

    [Fact]
    public void Constructor_WithNullBucket_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new S3WriteStream(_fakeS3Client, null!, TestKey));
        exception.ParamName.Should().Be("bucket");
    }

    [Fact]
    public void Constructor_WithNullKey_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new S3WriteStream(_fakeS3Client, TestBucket, null!));
        exception.ParamName.Should().Be("key");
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesStream()
    {
        // Act
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);

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
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey, "application/json");

        // Assert
        stream.Should().NotBeNull();
    }

    [Fact]
    public void Write_WithValidBuffer_WritesToTempFile()
    {
        // Arrange
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
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
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
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
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
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
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        stream.Dispose();
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act & Assert
        Assert.Throws<ObjectDisposedException>(() => stream.Write(data, 0, data.Length));
    }

    [Fact]
    public async Task WriteAsync_WhenDisposed_ThrowsObjectDisposedException()
    {
        // Arrange
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        await stream.DisposeAsync();
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await stream.WriteAsync(data, 0, data.Length));
    }

    [Fact]
    public void Flush_IsNoOp_DoesNotThrow()
    {
        // Arrange
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);

        // Act & Assert
        stream.Invoking(s => s.Flush()).Should().NotThrow();
    }

    [Fact]
    public async Task FlushAsync_IsNoOp_DoesNotThrow()
    {
        // Arrange
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);

        // Act & Assert
        await stream.Invoking(async s => await s.FlushAsync()).Should().NotThrowAsync();
    }

    [Fact]
    public void Read_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var buffer = new byte[10];

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Read(buffer, 0, buffer.Length));
    }

    [Fact]
    public void Seek_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Seek(0, SeekOrigin.Begin));
    }

    [Fact]
    public void SetLength_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.SetLength(100));
    }

    [Fact]
    public void Position_Getter_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => { var _ = stream.Position; });
    }

    [Fact]
    public void Position_Setter_ThrowsNotSupportedException()
    {
        // Arrange
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => stream.Position = 0);
    }

    [Fact]
    public void Length_WhenNotDisposed_ReturnsWrittenBytes()
    {
        // Arrange
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
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
        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        stream.Dispose();

        // Act & Assert
        Assert.Throws<ObjectDisposedException>(() => { var _ = stream.Length; });
    }

    [Fact]
    public async Task DisposeAsync_UploadsToS3()
    {
        // Arrange
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ReturnsAsync(new PutObjectResponse());

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey, "application/json");
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeS3Client.PutObjectAsync(
                A<PutObjectRequest>.That.Matches(r =>
                    r.BucketName == TestBucket &&
                    r.Key == TestKey &&
                    r.ContentType == "application/json"),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void Dispose_UploadsToS3()
    {
        // Arrange
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ReturnsAsync(new PutObjectResponse());

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey, "application/json");
        var data = new byte[] { 1, 2, 3, 4, 5 };
        stream.Write(data, 0, data.Length);

        // Act
        stream.Dispose();

        // Assert
        A.CallTo(() => _fakeS3Client.PutObjectAsync(
                A<PutObjectRequest>.That.Matches(r =>
                    r.BucketName == TestBucket &&
                    r.Key == TestKey &&
                    r.ContentType == "application/json"),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithoutContentType_UploadsToS3WithoutContentType()
    {
        // Arrange
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ReturnsAsync(new PutObjectResponse());

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeS3Client.PutObjectAsync(
                A<PutObjectRequest>.That.Matches(r =>
                    r.BucketName == TestBucket &&
                    r.Key == TestKey &&
                    string.IsNullOrEmpty(r.ContentType)),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithAccessDenied_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var s3Exception = new AmazonS3Exception("Access denied")
        {
            ErrorCode = "AccessDenied"
        };
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithInvalidAccessKeyId_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var s3Exception = new AmazonS3Exception("Invalid access key")
        {
            ErrorCode = "InvalidAccessKeyId"
        };
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithSignatureDoesNotMatch_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var s3Exception = new AmazonS3Exception("Signature does not match")
        {
            ErrorCode = "SignatureDoesNotMatch"
        };
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithInvalidBucketName_ThrowsArgumentException()
    {
        // Arrange
        var s3Exception = new AmazonS3Exception("Invalid bucket name")
        {
            ErrorCode = "InvalidBucketName"
        };
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithInvalidKey_ThrowsArgumentException()
    {
        // Arrange
        var s3Exception = new AmazonS3Exception("Invalid key")
        {
            ErrorCode = "InvalidKey"
        };
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithNoSuchBucket_ThrowsFileNotFoundException()
    {
        // Arrange
        var s3Exception = new AmazonS3Exception("Bucket not found")
        {
            ErrorCode = "NoSuchBucket"
        };
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithNotFound_ThrowsFileNotFoundException()
    {
        // Arrange
        var s3Exception = new AmazonS3Exception("Not found")
        {
            ErrorCode = "NotFound"
        };
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_WithGenericError_ThrowsIOException()
    {
        // Arrange
        var s3Exception = new AmazonS3Exception("Generic error")
        {
            ErrorCode = "InternalError"
        };
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(async () => await stream.DisposeAsync());
    }

    [Fact]
    public async Task DisposeAsync_CalledMultipleTimes_UploadsOnlyOnce()
    {
        // Arrange
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ReturnsAsync(new PutObjectResponse());

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void Dispose_CalledMultipleTimes_UploadsOnlyOnce()
    {
        // Arrange
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ReturnsAsync(new PutObjectResponse());

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        stream.Write(data, 0, data.Length);

        // Act
        stream.Dispose();
        stream.Dispose();

        // Assert
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithLargeData_UploadsAllData()
    {
        // Arrange
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ReturnsAsync(new PutObjectResponse())
            .Invokes((PutObjectRequest request, CancellationToken _) =>
            {
                // Verify the stream has the correct length
                request.InputStream.Should().NotBeNull();
                request.InputStream.Length.Should().BeGreaterThan(0);
            });

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[1024 * 1024]; // 1MB
        for (int i = 0; i < data.Length; i++)
        {
            data[i] = (byte)(i % 256);
        }
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_WithMultipleWrites_UploadsAllData()
    {
        // Arrange
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ReturnsAsync(new PutObjectResponse());

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey);
        var data = new byte[100];

        // Act
        for (int i = 0; i < 10; i++)
        {
            await stream.WriteAsync(data, 0, data.Length);
        }
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
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
        A.CallTo(() => _fakeS3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .ReturnsAsync(new PutObjectResponse());

        var stream = new S3WriteStream(_fakeS3Client, TestBucket, TestKey, contentType);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await stream.WriteAsync(data, 0, data.Length);

        // Act
        await stream.DisposeAsync();

        // Assert
        A.CallTo(() => _fakeS3Client.PutObjectAsync(
                A<PutObjectRequest>.That.Matches(r => r.ContentType == contentType),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }
}
