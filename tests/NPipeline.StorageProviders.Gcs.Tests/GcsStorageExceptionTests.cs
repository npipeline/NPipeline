using AwesomeAssertions;

namespace NPipeline.StorageProviders.Gcs.Tests;

public class GcsStorageExceptionTests
{
    [Fact]
    public void Constructor_WithMessageBucketObjectNameAndOperation_SetsProperties()
    {
        // Arrange
        const string message = "Test error message";
        const string bucket = "test-bucket";
        const string objectName = "test-object.txt";
        const string operation = "read";

        // Act
        var exception = new GcsStorageException(message, bucket, objectName, operation);

        // Assert
        exception.Message.Should().Be(message);
        exception.Bucket.Should().Be(bucket);
        exception.ObjectName.Should().Be(objectName);
        exception.Operation.Should().Be(operation);
        exception.InnerException.Should().BeNull();
        exception.OriginalException.Should().Be(exception);
    }

    [Fact]
    public void Constructor_WithMessageBucketObjectNameOperationAndInnerException_SetsProperties()
    {
        // Arrange
        const string message = "Test error message";
        const string bucket = "test-bucket";
        const string objectName = "test-object.txt";
        const string operation = "write";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new GcsStorageException(message, bucket, objectName, operation, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.Bucket.Should().Be(bucket);
        exception.ObjectName.Should().Be(objectName);
        exception.Operation.Should().Be(operation);
        exception.InnerException.Should().Be(innerException);
        exception.OriginalException.Should().Be(innerException);
    }

    [Fact]
    public void Constructor_WithNullBucket_ThrowsArgumentNullException()
    {
        // Arrange
        const string message = "Test error message";
        const string objectName = "test-object.txt";
        const string operation = "read";
        var innerException = new InvalidOperationException("Inner error");

        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new GcsStorageException(message, null!, objectName, operation, innerException));

        exception.ParamName.Should().Be("bucket");
    }

    [Fact]
    public void Constructor_WithNullObjectName_ThrowsArgumentNullException()
    {
        // Arrange
        const string message = "Test error message";
        const string bucket = "test-bucket";
        const string operation = "read";
        var innerException = new InvalidOperationException("Inner error");

        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new GcsStorageException(message, bucket, null!, operation, innerException));

        exception.ParamName.Should().Be("objectName");
    }

    [Fact]
    public void Constructor_WithNullInnerException_ThrowsArgumentNullException()
    {
        // Arrange
        const string message = "Test error message";
        const string bucket = "test-bucket";
        const string objectName = "test-object.txt";
        const string operation = "read";

        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new GcsStorageException(message, bucket, objectName, operation, null!));

        exception.ParamName.Should().Be("innerException");
    }

    [Fact]
    public void Constructor_WithNullBucket_ThrowsArgumentNullException_OverloadWithoutInnerException()
    {
        // Arrange
        const string message = "Test error message";
        const string objectName = "test-object.txt";
        const string operation = "read";

        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new GcsStorageException(message, null!, objectName, operation));

        exception.ParamName.Should().Be("bucket");
    }

    [Fact]
    public void Constructor_WithNullObjectName_ThrowsArgumentNullException_OverloadWithoutInnerException()
    {
        // Arrange
        const string message = "Test error message";
        const string bucket = "test-bucket";
        const string operation = "read";

        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new GcsStorageException(message, bucket, null!, operation));

        exception.ParamName.Should().Be("objectName");
    }

    [Fact]
    public void Constructor_WithNullOperation_SetsOperationToNull()
    {
        // Arrange
        const string message = "Test error message";
        const string bucket = "test-bucket";
        const string objectName = "test-object.txt";

        // Act
        var exception = new GcsStorageException(message, bucket, objectName, null);

        // Assert
        exception.Operation.Should().BeNull();
    }

    [Fact]
    public void GetDetailedMessage_WithOperation_ReturnsFormattedMessage()
    {
        // Arrange
        const string message = "Test error message";
        const string bucket = "test-bucket";
        const string objectName = "test-object.txt";
        const string operation = "read";

        var exception = new GcsStorageException(message, bucket, objectName, operation);

        // Act
        var detailedMessage = exception.GetDetailedMessage();

        // Assert
        detailedMessage.Should().Contain("GCS operation 'read' failed");
        detailedMessage.Should().Contain("bucket 'test-bucket'");
        detailedMessage.Should().Contain("object 'test-object.txt'");
        detailedMessage.Should().Contain(message);
    }

    [Fact]
    public void GetDetailedMessage_WithNullOperation_ReturnsFormattedMessageWithUnknown()
    {
        // Arrange
        const string message = "Test error message";
        const string bucket = "test-bucket";
        const string objectName = "test-object.txt";

        var exception = new GcsStorageException(message, bucket, objectName, null);

        // Act
        var detailedMessage = exception.GetDetailedMessage();

        // Assert
        detailedMessage.Should().Contain("GCS operation 'unknown' failed");
        detailedMessage.Should().Contain("bucket 'test-bucket'");
        detailedMessage.Should().Contain("object 'test-object.txt'");
    }

    [Fact]
    public void Exception_InheritsFromIOException()
    {
        // Arrange
        var exception = new GcsStorageException("message", "bucket", "object", "operation");

        // Act & Assert
        exception.Should().BeAssignableTo<IOException>();
    }

    [Fact]
    public void Exception_CanBeThrownAndCaught()
    {
        // Arrange
        var exception = new GcsStorageException("Test error", "bucket", "object", "read");

        // Act & Assert
        Action act = () => throw exception;
        var thrown = Assert.Throws<GcsStorageException>(act);
        thrown.Should().Be(exception);
    }

    [Fact]
    public void Exception_WithInnerException_CanBeCaughtAsInnerException()
    {
        // Arrange
        var innerException = new InvalidOperationException("Inner error");
        var exception = new GcsStorageException("Outer error", "bucket", "object", "write", innerException);

        // Act & Assert
        exception.InnerException.Should().Be(innerException);
    }

    [Fact]
    public void Exception_StackTrace_IsAvailable()
    {
        // Arrange
        GcsStorageException exception;

        // Act
        try
        {
            throw new GcsStorageException("Test error", "bucket", "object", "read");
        }
        catch (GcsStorageException ex)
        {
            exception = ex;
        }

        // Assert
        exception.StackTrace.Should().NotBeNull();
        exception.StackTrace.Should().NotBeEmpty();
    }

    [Theory]
    [InlineData("bucket1", "object1")]
    [InlineData("my-bucket", "path/to/file.txt")]
    [InlineData("test-bucket-with-dashes", "nested/path/to/object.json")]
    [InlineData("bucket", "")]
    public void Constructor_WithVariousBucketAndObjectNameValues_SetsPropertiesCorrectly(string bucket, string objectName)
    {
        // Arrange
        const string message = "Test error message";

        // Act
        var exception = new GcsStorageException(message, bucket, objectName, "read");

        // Assert
        exception.Bucket.Should().Be(bucket);
        exception.ObjectName.Should().Be(objectName);
    }

    [Fact]
    public void Constructor_WithEmptyMessage_SetsMessage()
    {
        // Arrange
        const string message = "";
        const string bucket = "bucket";
        const string objectName = "object";

        // Act
        var exception = new GcsStorageException(message, bucket, objectName, "read");

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void Constructor_WithLongMessage_SetsMessage()
    {
        // Arrange
        var message = new string('A', 1000);
        const string bucket = "bucket";
        const string objectName = "object";

        // Act
        var exception = new GcsStorageException(message, bucket, objectName, "read");

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void Constructor_WithSpecialCharactersInBucketAndObjectName_SetsProperties()
    {
        // Arrange
        const string message = "Test error message";
        const string bucket = "my-bucket-123";
        const string objectName = "path/to/file-with-special_chars.json";

        // Act
        var exception = new GcsStorageException(message, bucket, objectName, "read");

        // Assert
        exception.Bucket.Should().Be(bucket);
        exception.ObjectName.Should().Be(objectName);
    }

    [Fact]
    public void MultipleExceptions_AreIndependent()
    {
        // Arrange & Act
        var exception1 = new GcsStorageException("message1", "bucket1", "object1", "read");
        var exception2 = new GcsStorageException("message2", "bucket2", "object2", "write");

        // Assert
        exception1.Bucket.Should().Be("bucket1");
        exception1.ObjectName.Should().Be("object1");
        exception1.Message.Should().Be("message1");
        exception1.Operation.Should().Be("read");
        exception2.Bucket.Should().Be("bucket2");
        exception2.ObjectName.Should().Be("object2");
        exception2.Message.Should().Be("message2");
        exception2.Operation.Should().Be("write");
    }

    [Fact]
    public void OriginalException_WhenNoInnerException_ReturnsSelf()
    {
        // Arrange
        var exception = new GcsStorageException("message", "bucket", "object", "read");

        // Act & Assert
        exception.OriginalException.Should().Be(exception);
    }

    [Fact]
    public void OriginalException_WithInnerException_ReturnsInnerException()
    {
        // Arrange
        var innerException = new InvalidOperationException("Inner error");
        var exception = new GcsStorageException("message", "bucket", "object", "read", innerException);

        // Act & Assert
        exception.OriginalException.Should().Be(innerException);
    }

    [Theory]
    [InlineData("read")]
    [InlineData("write")]
    [InlineData("delete")]
    [InlineData("list")]
    [InlineData("metadata")]
    public void Constructor_WithVariousOperations_SetsOperationCorrectly(string operation)
    {
        // Arrange
        const string message = "Test error message";
        const string bucket = "test-bucket";
        const string objectName = "test-object.txt";

        // Act
        var exception = new GcsStorageException(message, bucket, objectName, operation);

        // Assert
        exception.Operation.Should().Be(operation);
    }
}
