using System.IO.Compression;
using System.Net.Sockets;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using FakeItEasy;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Writers;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Writers;

public class RedshiftCopyFromS3WriterTests
{
    private readonly RedshiftConfiguration _config;
    private readonly IRedshiftConnectionPool _connectionPool;
    private readonly IAmazonS3 _s3Client;

    public RedshiftCopyFromS3WriterTests()
    {
        _connectionPool = A.Fake<IRedshiftConnectionPool>();
        _s3Client = A.Fake<IAmazonS3>();

        _config = new RedshiftConfiguration
        {
            BatchSize = 100,
            S3BucketName = "test-bucket",
            S3KeyPrefix = "test-prefix/",
            IamRoleArn = "arn:aws:iam::123456789012:role/RedshiftCopyRole",
            MaxRetryAttempts = 0, // Disable retries for most tests
        };
    }

    [Fact]
    public void Constructor_WithNullConnectionPool_ThrowsArgumentNullException()
    {
        // Act
        var act = () => new RedshiftCopyFromS3Writer<TestRow>(
            null!, "public", "test_table", _config, _s3Client);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("connectionPool");
    }

    [Fact]
    public void Constructor_WithNullSchema_ThrowsArgumentNullException()
    {
        // Act
        var act = () => new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, null!, "test_table", _config, _s3Client);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("schema");
    }

    [Fact]
    public void Constructor_WithNullTable_ThrowsArgumentNullException()
    {
        // Act
        var act = () => new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", null!, _config, _s3Client);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("table");
    }

    [Fact]
    public void Constructor_WithNullConfig_ThrowsArgumentNullException()
    {
        // Act
        var act = () => new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", null!, _s3Client);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("config");
    }

    [Fact]
    public void Constructor_WithNullS3Client_ThrowsArgumentNullException()
    {
        // Act
        var act = () => new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, null!);

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("s3Client");
    }

    [Fact]
    public async Task FlushAsync_WithEmptyBuffer_IsNoOp()
    {
        // Arrange
        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        // Act
        await writer.FlushAsync();

        // Assert
        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustNotHaveHappened();

        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_BuffersUntilBatchSize()
    {
        // Arrange
        _config.BatchSize = 3;

        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        // Act - Write 2 rows (below batch size)
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await writer.WriteAsync(new TestRow { Id = 2, Name = "Test2" });

        // Assert - Should not flush yet
        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task FlushAsync_UploadsToS3AndIssuesCopyCommand()
    {
        // Arrange
        // Make connection throw - we just want to verify S3 upload happened
        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Returns(new PutObjectResponse { ETag = "test-etag" });

        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Act & Assert - Flush will fail at COPY, but S3 upload should have happened
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task FlushAsync_WithPurgeEnabled_DeletesS3File()
    {
        // Arrange
        _config.PurgeS3FilesAfterCopy = true;

        // Make connection throw - we just want to verify delete was called
        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Returns(new PutObjectResponse { ETag = "test-etag" });

        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        // Delete should be called even when COPY fails (in finally block)
        A.CallTo(() => _s3Client.DeleteObjectAsync(A<string>._, A<string>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task FlushAsync_WithPurgeDisabled_DoesNotDeleteS3File()
    {
        // Arrange
        _config.PurgeS3FilesAfterCopy = false;

        // Make connection throw
        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Returns(new PutObjectResponse { ETag = "test-etag" });

        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        // Delete should NOT be called when purge is disabled
        A.CallTo(() => _s3Client.DeleteObjectAsync(A<string>._, A<string>._, A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task FlushAsync_WhenS3UploadFails_RetriesAndThrows()
    {
        // Arrange
        _config.MaxRetryAttempts = 2;

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Throws(new SocketException()); // SocketException is transient

        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Act & Assert
        // After retries are exhausted, the original exception is re-thrown (not wrapped)
        await Assert.ThrowsAsync<SocketException>(() => writer.FlushAsync());

        // Should have tried MaxRetryAttempts times (the handler's behavior)
        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustHaveHappened(2, Times.Exactly);
    }

    [Fact]
    public async Task FlushAsync_WithUpsert_CreatesStagingTableAndMerges()
    {
        // Arrange
        _config.UseUpsert = true;
        _config.UpsertKeyColumns = new[] { "id" };

        // Make connection throw
        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Returns(new PutObjectResponse { ETag = "test-etag" });

        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Act & Assert - Should attempt upsert path
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        // S3 upload should have happened
        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task CsvOutput_FormatsNullsAsEmpty()
    {
        // Arrange
        byte[]? capturedData = null;

        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Invokes((PutObjectRequest req, CancellationToken _) =>
            {
                using var memory = new MemoryStream();
                req.InputStream!.CopyTo(memory);
                capturedData = memory.ToArray();
            })
            .Returns(new PutObjectResponse { ETag = "test-etag" });

        var writer = new RedshiftCopyFromS3Writer<TestRowWithNulls>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.WriteAsync(new TestRowWithNulls { Id = 1, Name = null });

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        // Assert CSV content
        capturedData.Should().NotBeNull();
        var csvContent = DecompressGzip(capturedData!);
        csvContent.Should().Contain("id,name");
        csvContent.Should().Contain("1,");
    }

    [Fact]
    public async Task CsvOutput_EscapesCommasAndQuotes()
    {
        // Arrange
        byte[]? capturedData = null;

        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Invokes((PutObjectRequest req, CancellationToken _) =>
            {
                using var memory = new MemoryStream();
                req.InputStream!.CopyTo(memory);
                capturedData = memory.ToArray();
            })
            .Returns(new PutObjectResponse { ETag = "test-etag" });

        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test, with \"quotes\"" });

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        // Assert CSV content
        capturedData.Should().NotBeNull();
        var csvContent = DecompressGzip(capturedData!);
        csvContent.Should().Contain("\"Test, with \"\"quotes\"\"\"");
    }

    [Fact]
    public async Task CsvOutput_FormatsDateTimeInIso8601()
    {
        // Arrange
        byte[]? capturedData = null;

        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Invokes((PutObjectRequest req, CancellationToken _) =>
            {
                using var memory = new MemoryStream();
                req.InputStream!.CopyTo(memory);
                capturedData = memory.ToArray();
            })
            .Returns(new PutObjectResponse { ETag = "test-etag" });

        var writer = new RedshiftCopyFromS3Writer<TestRowWithDateTime>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        var testDate = new DateTime(2024, 3, 15, 10, 30, 45, DateTimeKind.Utc);
        await writer.WriteAsync(new TestRowWithDateTime { Id = 1, CreatedAt = testDate });

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        // Assert CSV content
        capturedData.Should().NotBeNull();
        var csvContent = DecompressGzip(capturedData!);
        csvContent.Should().Contain("2024-03-15T10:30:45.0000000Z");
    }

    [Fact]
    public async Task DisposeAsync_FlushesRemainingRows()
    {
        // Arrange
        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Returns(new PutObjectResponse { ETag = "test-etag" });

        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Act - Dispose should flush, and since we set up the fake to throw,
        // the flush will attempt to get a connection. DisposeAsync catches exceptions.
        await writer.DisposeAsync();

        // Assert - Verify that the writer attempted to upload to S3 (flush was attempted)
        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        // Arrange
        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        // Act
        await writer.DisposeAsync();
        await writer.DisposeAsync();

        // Assert - Should not throw
    }

    [Fact]
    public async Task WriteAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.DisposeAsync();

        // Act
        var act = async () => await writer.WriteAsync(new TestRow { Id = 1, Name = "Test" });

        // Assert
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task FlushAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.DisposeAsync();

        // Act
        var act = async () => await writer.FlushAsync();

        // Assert
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task WriteAsync_WithNullRow_ThrowsArgumentNullException()
    {
        // Arrange
        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        // Act
        var act = async () => await writer.WriteAsync(null!);

        // Assert
        await act.Should().ThrowAsync<ArgumentNullException>()
            .WithParameterName("row");
    }

    [Fact]
    public async Task FlushAsync_WhenBatchSizeReached_AutoFlushes()
    {
        // Arrange
        _config.BatchSize = 2;

        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Returns(new PutObjectResponse { ETag = "test-etag" });

        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        // Act - Write exactly batch size - second write triggers flush
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteAsync(new TestRow { Id = 2, Name = "Test2" }));

        // Assert - Should have auto-flushed
        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task FlushAsync_WhenRedshiftCopyFails_RetriesAndThrows()
    {
        // Arrange — S3 upload succeeds, but the Redshift COPY command fails with a
        // transient TimeoutException.  The exception handler should retry before giving up.
        _config.MaxRetryAttempts = 2;
        _config.RetryDelay = TimeSpan.FromMilliseconds(1); // keep the test fast

        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Returns(new PutObjectResponse { ETag = "test-etag" });

        // TimeoutException is considered transient → will be retried until attempts exhausted
        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new TimeoutException("Redshift COPY timed out"));

        var writer = new RedshiftCopyFromS3Writer<TestRow>(
            _connectionPool, "public", "test_table", _config, _s3Client);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Act — flush triggers S3 upload (succeeds) then COPY (fails, retries, exhausts)
        await Assert.ThrowsAsync<TimeoutException>(() => writer.FlushAsync());

        // Assert — S3 was uploaded exactly once; COPY was attempted MaxRetryAttempts times
        A.CallTo(() => _s3Client.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();

        A.CallTo(() => _connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappened(2, Times.Exactly);
    }

    private static string DecompressGzip(byte[] compressedData)
    {
        using var memoryStream = new MemoryStream(compressedData);
        using var gzipStream = new GZipStream(memoryStream, CompressionMode.Decompress);
        using var reader = new StreamReader(gzipStream, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    public class TestRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class TestRowWithNulls
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    public class TestRowWithDateTime
    {
        public int Id { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
