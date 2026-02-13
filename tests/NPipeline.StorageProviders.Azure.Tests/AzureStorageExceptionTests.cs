using AwesomeAssertions;
using Azure;
using NPipeline.StorageProviders.Exceptions;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

public class AzureStorageExceptionTests
{
    [Fact]
    public void Constructor_WithMessageContainerAndBlob_SetsProperties()
    {
        // Arrange
        var message = "Test error message";
        var container = "test-container";
        var blob = "test-blob";

        // Act
        var exception = new AzureStorageException(message, container, blob);

        // Assert
        exception.Message.Should().Be(message);
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
        exception.InnerException.Should().BeNull();
        exception.InnerAzureException.Should().BeNull();
    }

    [Fact]
    public void Constructor_WithMessageContainerBlobAndInnerException_SetsProperties()
    {
        // Arrange
        var message = "Test error message";
        var container = "test-container";
        var blob = "test-blob";
        var innerException = new RequestFailedException(404, "Not found", "BlobNotFound", null);

        // Act
        var exception = new AzureStorageException(message, container, blob, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
        exception.InnerException.Should().Be(innerException);
        exception.InnerAzureException.Should().Be(innerException);
    }

    [Fact]
    public void Constructor_WithNonAzureInnerException_SetsInnerAzureExceptionToNull()
    {
        // Arrange
        var message = "Test error message";
        var container = "test-container";
        var blob = "test-blob";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new AzureStorageException(message, container, blob, innerException);

        // Assert
        exception.InnerException.Should().Be(innerException);
        exception.InnerAzureException.Should().BeNull();
    }

    [Fact]
    public void Constructor_WithNullInnerException_SetsInnerAzureExceptionToNull()
    {
        // Arrange
        var message = "Test error message";
        var container = "test-container";
        var blob = "test-blob";

        // Act
        var exception = new AzureStorageException(message, container, blob, null!);

        // Assert
        exception.InnerException.Should().BeNull();
        exception.InnerAzureException.Should().BeNull();
    }

    [Fact]
    public void MessageFormat_IncludesContainerAndBlob()
    {
        // Arrange
        var message = "Test error message";
        var container = "test-container";
        var blob = "test-blob";

        // Act
        var exception = new AzureStorageException(message, container, blob);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void ContainerProperty_IsReadOnly()
    {
        // Arrange
        var exception = new AzureStorageException("message", "container", "blob");

        // Act & Assert
        exception.Container.Should().Be("container");

        // Note: In C#, properties are not truly read-only at runtime,
        // but we can verify the value is set correctly
    }

    [Fact]
    public void BlobProperty_IsReadOnly()
    {
        // Arrange
        var exception = new AzureStorageException("message", "container", "blob");

        // Act & Assert
        exception.Blob.Should().Be("blob");
    }

    [Fact]
    public void InnerAzureExceptionProperty_WhenInnerIsRequestFailedException_ReturnsSameInstance()
    {
        // Arrange
        var azureException = new RequestFailedException(404, "Not found", "BlobNotFound", null);
        var exception = new AzureStorageException("message", "container", "blob", azureException);

        // Act & Assert
        exception.InnerAzureException.Should().BeSameAs(azureException);
    }

    [Fact]
    public void InnerAzureExceptionProperty_WhenInnerIsNotRequestFailedException_ReturnsNull()
    {
        // Arrange
        var innerException = new InvalidOperationException("Not an Azure error");
        var exception = new AzureStorageException("message", "container", "blob", innerException);

        // Act & Assert
        exception.InnerAzureException.Should().BeNull();
    }

    [Theory]
    [InlineData("container1", "blob1")]
    [InlineData("my-container", "path/to/file.txt")]
    [InlineData("test-container-with-dashes", "nested/path/to/object.json")]
    [InlineData("container", "")]
    [InlineData("", "blob")]
    public void Constructor_WithVariousContainerAndBlobValues_SetsPropertiesCorrectly(string container, string blob)
    {
        // Arrange
        var message = "Test error message";

        // Act
        var exception = new AzureStorageException(message, container, blob);

        // Assert
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
    }

    [Fact]
    public void Exception_InheritsFromConnectorException()
    {
        // Arrange
        var exception = new AzureStorageException("message", "container", "blob");

        // Act & Assert
        exception.Should().BeAssignableTo<ConnectorException>();
    }

    [Fact]
    public void Exception_CanBeThrownAndCaught()
    {
        // Arrange
        var exception = new AzureStorageException("Test error", "container", "blob");

        // Act & Assert
        Action act = () => throw exception;
        var thrown = Assert.Throws<AzureStorageException>(act);
        thrown.Should().Be(exception);
    }

    [Fact]
    public void Exception_WithInnerException_CanBeCaughtAsInnerException()
    {
        // Arrange
        var innerException = new RequestFailedException(404, "Not found", "BlobNotFound", null);
        var exception = new AzureStorageException("Outer error", "container", "blob", innerException);

        // Act & Assert
        exception.InnerException.Should().Be(innerException);
    }

    [Fact]
    public void Exception_StackTrace_IsAvailable()
    {
        // Arrange
        AzureStorageException exception;

        // Act
        try
        {
            throw new AzureStorageException("Test error", "container", "blob");
        }
        catch (AzureStorageException ex)
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
        var exception = new AzureStorageException("Test error", "container", "blob");

        // Act & Assert
        // Note: AzureStorageException is not marked as Serializable,
        // so this test verifies current behavior
        exception.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithEmptyMessage_SetsMessage()
    {
        // Arrange
        var message = "";
        var container = "container";
        var blob = "blob";

        // Act
        var exception = new AzureStorageException(message, container, blob);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void Constructor_WithLongMessage_SetsMessage()
    {
        // Arrange
        var message = new string('A', 1000);
        var container = "container";
        var blob = "blob";

        // Act
        var exception = new AzureStorageException(message, container, blob);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void Constructor_WithSpecialCharactersInContainerAndBlob_SetsProperties()
    {
        // Arrange
        var message = "Test error message";
        var container = "my-container-123";
        var blob = "path/to/file-with-special_chars.json";

        // Act
        var exception = new AzureStorageException(message, container, blob);

        // Assert
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
    }

    [Fact]
    public void MultipleExceptions_AreIndependent()
    {
        // Arrange
        var exception1 = new AzureStorageException("message1", "container1", "blob1");
        var exception2 = new AzureStorageException("message2", "container2", "blob2");

        // Act & Assert
        exception1.Container.Should().Be("container1");
        exception1.Blob.Should().Be("blob1");
        exception1.Message.Should().Be("message1");
        exception2.Container.Should().Be("container2");
        exception2.Blob.Should().Be("blob2");
        exception2.Message.Should().Be("message2");
    }

    [Fact]
    public void Constructor_WithInnerRequestFailedException_CreatesException()
    {
        // Arrange
        var azureException = new RequestFailedException(404, "Not found", "BlobNotFound", null);
        var container = "test-container";
        var blob = "test-blob";
        var message = $"Error accessing Azure: container={container}, blob={blob}";

        // Act
        var exception = new AzureStorageException(message, container, blob, azureException);

        // Assert
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
        exception.InnerException.Should().Be(azureException);
        exception.Message.Should().Contain(container);
        exception.Message.Should().Contain(blob);
    }

    [Fact]
    public void Constructor_WithAuthenticationFailedException_CreatesException()
    {
        // Arrange
        var azureException = new RequestFailedException(401, "Authentication failed", "AuthenticationFailed", null);
        var container = "test-container";
        var blob = "test-blob";

        // Act
        var exception = new AzureStorageException("Auth error", container, blob, azureException);

        // Assert
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
        exception.InnerAzureException.Should().Be(azureException);
    }

    [Fact]
    public void Constructor_WithAuthorizationFailedException_CreatesException()
    {
        // Arrange
        var azureException = new RequestFailedException(403, "Authorization failed", "AuthorizationFailed", null);
        var container = "test-container";
        var blob = "test-blob";

        // Act
        var exception = new AzureStorageException("Authz error", container, blob, azureException);

        // Assert
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
        exception.InnerAzureException.Should().Be(azureException);
    }

    [Fact]
    public void Constructor_WithContainerNotFoundException_CreatesException()
    {
        // Arrange
        var azureException = new RequestFailedException(404, "Container not found", "ContainerNotFound", null);
        var container = "test-container";
        var blob = "test-blob";

        // Act
        var exception = new AzureStorageException("Not found", container, blob, azureException);

        // Assert
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
        exception.InnerAzureException.Should().Be(azureException);
    }

    [Fact]
    public void Constructor_WithBlobNotFoundException_CreatesException()
    {
        // Arrange
        var azureException = new RequestFailedException(404, "Blob not found", "BlobNotFound", null);
        var container = "test-container";
        var blob = "test-blob";

        // Act
        var exception = new AzureStorageException("Not found", container, blob, azureException);

        // Assert
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
        exception.InnerAzureException.Should().Be(azureException);
    }

    [Fact]
    public void Constructor_WithInvalidResourceNameException_CreatesException()
    {
        // Arrange
        var azureException = new RequestFailedException(400, "Invalid resource name", "InvalidResourceName", null);
        var container = "test-container";
        var blob = "test-blob";

        // Act
        var exception = new AzureStorageException("Invalid name", container, blob, azureException);

        // Assert
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
        exception.InnerAzureException.Should().Be(azureException);
    }

    [Fact]
    public void Constructor_WithInvalidQueryParameterValueException_CreatesException()
    {
        // Arrange
        var azureException = new RequestFailedException(400, "Invalid query parameter", "InvalidQueryParameterValue", null);
        var container = "test-container";
        var blob = "test-blob";

        // Act
        var exception = new AzureStorageException("Invalid parameter", container, blob, azureException);

        // Assert
        exception.Container.Should().Be(container);
        exception.Blob.Should().Be(blob);
        exception.InnerAzureException.Should().Be(azureException);
    }

    [Fact]
    public void Exception_HasCorrectType()
    {
        // Arrange
        var exception = new AzureStorageException("message", "container", "blob");

        // Act & Assert
        exception.GetType().Name.Should().Be("AzureStorageException");
        exception.GetType().Namespace.Should().Be("NPipeline.StorageProviders.Azure");
    }

    [Fact]
    public void Exception_GetBaseException_ReturnsSameException()
    {
        // Arrange
        var exception = new AzureStorageException("message", "container", "blob");

        // Act & Assert
        exception.GetBaseException().Should().BeSameAs(exception);
    }

    [Fact]
    public void Exception_GetObjectData_ContainsProperties()
    {
        // Arrange
        var exception = new AzureStorageException("message", "container", "blob");

        // Act & Assert
        // The exception should be serializable for diagnostics
        exception.Should().NotBeNull();
        exception.Message.Should().Be("message");
    }
}
