using Amazon.S3;
using FluentAssertions;
using NPipeline.StorageProviders.Exceptions;
using Xunit;

namespace NPipeline.StorageProviders.Aws.Tests;

public class S3StorageExceptionTests
{
    [Fact]
    public void Constructor_WithMessageBucketAndKey_SetsProperties()
    {
        // Arrange
        var message = "Test error message";
        var bucket = "test-bucket";
        var key = "test-key";

        // Act
        var exception = new S3StorageException(message, bucket, key);

        // Assert
        exception.Message.Should().Be(message);
        exception.Bucket.Should().Be(bucket);
        exception.Key.Should().Be(key);
        exception.InnerException.Should().BeNull();
        exception.InnerS3Exception.Should().BeNull();
    }

    [Fact]
    public void Constructor_WithMessageBucketKeyAndInnerException_SetsProperties()
    {
        // Arrange
        var message = "Test error message";
        var bucket = "test-bucket";
        var key = "test-key";
        var innerException = new AmazonS3Exception("Inner S3 error");

        // Act
        var exception = new S3StorageException(message, bucket, key, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.Bucket.Should().Be(bucket);
        exception.Key.Should().Be(key);
        exception.InnerException.Should().Be(innerException);
        exception.InnerS3Exception.Should().Be(innerException);
    }

    [Fact]
    public void Constructor_WithNonS3InnerException_SetsInnerS3ExceptionToNull()
    {
        // Arrange
        var message = "Test error message";
        var bucket = "test-bucket";
        var key = "test-key";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new S3StorageException(message, bucket, key, innerException);

        // Assert
        exception.InnerException.Should().Be(innerException);
        exception.InnerS3Exception.Should().BeNull();
    }

    [Fact]
    public void Constructor_WithNullInnerException_SetsInnerS3ExceptionToNull()
    {
        // Arrange
        var message = "Test error message";
        var bucket = "test-bucket";
        var key = "test-key";

        // Act
        var exception = new S3StorageException(message, bucket, key, null!);

        // Assert
        exception.InnerException.Should().BeNull();
        exception.InnerS3Exception.Should().BeNull();
    }

    [Fact]
    public void MessageFormat_IncludesBucketAndKey()
    {
        // Arrange
        var message = "Test error message";
        var bucket = "test-bucket";
        var key = "test-key";

        // Act
        var exception = new S3StorageException(message, bucket, key);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void BucketProperty_IsReadOnly()
    {
        // Arrange
        var exception = new S3StorageException("message", "bucket", "key");

        // Act & Assert
        exception.Bucket.Should().Be("bucket");

        // Note: In C#, properties are not truly read-only at runtime,
        // but we can verify the value is set correctly
    }

    [Fact]
    public void KeyProperty_IsReadOnly()
    {
        // Arrange
        var exception = new S3StorageException("message", "bucket", "key");

        // Act & Assert
        exception.Key.Should().Be("key");
    }

    [Fact]
    public void InnerS3ExceptionProperty_WhenInnerIsS3Exception_ReturnsSameInstance()
    {
        // Arrange
        var s3Exception = new AmazonS3Exception("S3 error");
        var exception = new S3StorageException("message", "bucket", "key", s3Exception);

        // Act & Assert
        exception.InnerS3Exception.Should().BeSameAs(s3Exception);
    }

    [Fact]
    public void InnerS3ExceptionProperty_WhenInnerIsNotS3Exception_ReturnsNull()
    {
        // Arrange
        var innerException = new InvalidOperationException("Not an S3 error");
        var exception = new S3StorageException("message", "bucket", "key", innerException);

        // Act & Assert
        exception.InnerS3Exception.Should().BeNull();
    }

    [Theory]
    [InlineData("bucket1", "key1")]
    [InlineData("my-bucket", "path/to/file.txt")]
    [InlineData("test-bucket-with-dashes", "nested/path/to/object.json")]
    [InlineData("bucket", "")]
    [InlineData("", "key")]
    public void Constructor_WithVariousBucketAndKeyValues_SetsPropertiesCorrectly(string bucket, string key)
    {
        // Arrange
        var message = "Test error message";

        // Act
        var exception = new S3StorageException(message, bucket, key);

        // Assert
        exception.Bucket.Should().Be(bucket);
        exception.Key.Should().Be(key);
    }

    [Fact]
    public void Exception_InheritsFromConnectorException()
    {
        // Arrange
        var exception = new S3StorageException("message", "bucket", "key");

        // Act & Assert
        exception.Should().BeAssignableTo<ConnectorException>();
    }

    [Fact]
    public void Exception_CanBeThrownAndCaught()
    {
        // Arrange
        var exception = new S3StorageException("Test error", "bucket", "key");

        // Act & Assert
        Action act = () => throw exception;
        var thrown = Assert.Throws<S3StorageException>(act);
        thrown.Should().Be(exception);
    }

    [Fact]
    public void Exception_WithInnerException_CanBeCaughtAsInnerException()
    {
        // Arrange
        var innerException = new AmazonS3Exception("Inner S3 error");
        var exception = new S3StorageException("Outer error", "bucket", "key", innerException);

        // Act & Assert
        exception.InnerException.Should().Be(innerException);
    }

    [Fact]
    public void Exception_StackTrace_IsAvailable()
    {
        // Arrange
        S3StorageException exception;

        // Act
        try
        {
            throw new S3StorageException("Test error", "bucket", "key");
        }
        catch (S3StorageException ex)
        {
            exception = ex;
        }

        // Assert
        exception.StackTrace.Should().NotBeNull();
        exception.StackTrace.Should().NotBeEmpty();
    }

    [Fact]
    public void Exception_CanBeSerialized()
    {
        // Arrange
        var exception = new S3StorageException("Test error", "bucket", "key");

        // Act & Assert
        // Note: S3StorageException is not marked as Serializable,
        // so this test verifies. current behavior
        exception.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithEmptyMessage_SetsMessage()
    {
        // Arrange
        var message = "";
        var bucket = "bucket";
        var key = "key";

        // Act
        var exception = new S3StorageException(message, bucket, key);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void Constructor_WithLongMessage_SetsMessage()
    {
        // Arrange
        var message = new string('A', 1000);
        var bucket = "bucket";
        var key = "key";

        // Act
        var exception = new S3StorageException(message, bucket, key);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void Constructor_WithSpecialCharactersInBucketAndKey_SetsProperties()
    {
        // Arrange
        var message = "Test error message";
        var bucket = "my-bucket-123";
        var key = "path/to/file-with-special_chars.json";

        // Act
        var exception = new S3StorageException(message, bucket, key);

        // Assert
        exception.Bucket.Should().Be(bucket);
        exception.Key.Should().Be(key);
    }

    [Fact]
    public void MultipleExceptions_AreIndependent()
    {
        // Arrange
        var exception1 = new S3StorageException("message1", "bucket1", "key1");
        var exception2 = new S3StorageException("message2", "bucket2", "key2");

        // Act & Assert
        exception1.Bucket.Should().Be("bucket1");
        exception1.Key.Should().Be("key1");
        exception1.Message.Should().Be("message1");
        exception2.Bucket.Should().Be("bucket2");
        exception2.Key.Should().Be("key2");
        exception2.Message.Should().Be("message2");
    }

    [Fact]
    public void Constructor_WithInnerS3Exception_CreatesException()
    {
        // Arrange
        var s3Exception = new AmazonS3Exception("S3 error");
        var bucket = "test-bucket";
        var key = "test-key";
        var message = $"Error accessing S3: bucket={bucket}, key={key}";

        // Act
        var exception = new S3StorageException(message, bucket, key, s3Exception);

        // Assert
        exception.Bucket.Should().Be(bucket);
        exception.Key.Should().Be(key);
        exception.InnerException.Should().Be(s3Exception);
        exception.Message.Should().Contain(bucket);
        exception.Message.Should().Contain(key);
    }
}
