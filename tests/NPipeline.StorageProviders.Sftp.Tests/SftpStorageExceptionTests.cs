namespace NPipeline.StorageProviders.Sftp.Tests;

/// <summary>
///     Unit tests for <see cref="SftpStorageException" />.
/// </summary>
public class SftpStorageExceptionTests
{
    [Fact]
    public void Constructor_WithMessage_ShouldSetMessage()
    {
        // Arrange & Act
        var exception = new SftpStorageException("Test error message");

        // Assert
        exception.Message.Should().Be("Test error message");
        exception.Host.Should().BeNull();
        exception.Path.Should().BeNull();
        exception.ErrorCode.Should().Be(SftpErrorCode.Unknown);
    }

    [Fact]
    public void Constructor_WithAllParameters_ShouldSetAllProperties()
    {
        // Arrange & Act
        var exception = new SftpStorageException(
            "Test error message",
            "sftp.example.com",
            "/data/file.csv",
            SftpErrorCode.FileNotFound);

        // Assert
        exception.Message.Should().Be("Test error message");
        exception.Host.Should().Be("sftp.example.com");
        exception.Path.Should().Be("/data/file.csv");
        exception.ErrorCode.Should().Be(SftpErrorCode.FileNotFound);
    }

    [Fact]
    public void Constructor_WithInnerException_ShouldSetInnerException()
    {
        // Arrange
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new SftpStorageException(
            "Test error message",
            "sftp.example.com",
            "/data/file.csv",
            SftpErrorCode.ConnectionFailed,
            innerException);

        // Assert
        exception.Message.Should().Be("Test error message");
        exception.Host.Should().Be("sftp.example.com");
        exception.Path.Should().Be("/data/file.csv");
        exception.ErrorCode.Should().Be(SftpErrorCode.ConnectionFailed);
        exception.InnerException.Should().Be(innerException);
    }

    [Theory]
    [InlineData(SftpErrorCode.Unknown)]
    [InlineData(SftpErrorCode.ConnectionFailed)]
    [InlineData(SftpErrorCode.AuthenticationFailed)]
    [InlineData(SftpErrorCode.FileNotFound)]
    [InlineData(SftpErrorCode.PermissionDenied)]
    [InlineData(SftpErrorCode.PathNotFound)]
    [InlineData(SftpErrorCode.OperationTimeout)]
    [InlineData(SftpErrorCode.ConnectionLost)]
    public void Constructor_ShouldAcceptAllErrorCodes(SftpErrorCode errorCode)
    {
        // Arrange & Act
        var exception = new SftpStorageException("Test", "host", "/path", errorCode);

        // Assert
        exception.ErrorCode.Should().Be(errorCode);
    }
}
