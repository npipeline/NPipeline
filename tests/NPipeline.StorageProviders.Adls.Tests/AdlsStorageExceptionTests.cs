using Azure;
using AwesomeAssertions;
using NPipeline.StorageProviders.Exceptions;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

public class AdlsStorageExceptionTests
{
    [Fact]
    public void Constructor_WithFilesystemAndPath_SetsProperties()
    {
        // Arrange
        var message = "Test error message";
        var filesystem = "testfs";
        var path = "path/to/file.txt";

        // Act
        var exception = new AdlsStorageException(message, filesystem, path);

        // Assert
        exception.Message.Should().Be(message);
        exception.Filesystem.Should().Be(filesystem);
        exception.Path.Should().Be(path);
    }

    [Fact]
    public void Constructor_WithFilesystemPathAndInnerException_SetsProperties()
    {
        // Arrange
        var message = "Test error message";
        var filesystem = "testfs";
        var path = "path/to/file.txt";
        var innerException = new RequestFailedException(404, "Not found");

        // Act
        var exception = new AdlsStorageException(message, filesystem, path, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.Filesystem.Should().Be(filesystem);
        exception.Path.Should().Be(path);
        exception.InnerException.Should().Be(innerException);
        exception.InnerAdlsException.Should().Be(innerException);
    }

    [Fact]
    public void AdlsStorageException_InheritsFromConnectorException()
    {
        // Arrange
        var exception = new AdlsStorageException("Test", "fs", "path");

        // Assert
        exception.Should().BeAssignableTo<ConnectorException>();
    }

    [Fact]
    public void InnerAdlsException_IsSetCorrectly()
    {
        // Arrange
        var innerException = new RequestFailedException(403, "Forbidden");

        // Act
        var exception = new AdlsStorageException("Test", "fs", "path", innerException);

        // Assert
        exception.InnerAdlsException.Should().Be(innerException);
    }

    [Fact]
    public void InnerAdlsException_IsNullWhenNotProvided()
    {
        // Act
        var exception = new AdlsStorageException("Test", "fs", "path");

        // Assert
        exception.InnerAdlsException.Should().BeNull();
    }
}
