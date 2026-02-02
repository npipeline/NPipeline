using AwesomeAssertions;
using NPipeline.Connectors.SqlServer.Exceptions;

namespace NPipeline.Connectors.SqlServer.Tests.Exceptions;

public class SqlServerTransientErrorDetectorTests
{
    [Fact]
    public void IsTransient_WithTimeoutException_ReturnsTrue()
    {
        // Arrange
        var exception = new TimeoutException("Connection timeout");

        // Act
        var result = SqlServerTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithOperationCanceledException_ReturnsTrue()
    {
        // Arrange
        var exception = new OperationCanceledException("Operation canceled");

        // Act
        var result = SqlServerTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithGenericException_ReturnsFalse()
    {
        // Arrange
        var exception = new InvalidOperationException("Invalid operation");

        // Act
        var result = SqlServerTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithNull_ReturnsFalse()
    {
        // Arrange
        Exception? exception = null;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransient(exception!);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithInvalidOperationExceptionWithTimeout_ReturnsTrue()
    {
        // Arrange
        var exception = new InvalidOperationException("Connection timeout");

        // Act
        var result = SqlServerTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithInvalidOperationExceptionWithConnection_ReturnsTrue()
    {
        // Arrange
        var exception = new InvalidOperationException("The connection is closed");

        // Act
        var result = SqlServerTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithInvalidOperationExceptionGeneric_ReturnsFalse()
    {
        // Arrange
        var exception = new InvalidOperationException("Invalid operation");

        // Act
        var result = SqlServerTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransientError_WithTimeoutCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = -2;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithNetworkNotFoundCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 53;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithNetworkDisconnectedCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 64;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithNetworkTimeoutCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 121;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithDeadlockCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 1205;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithAzureServiceBusyCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 40501;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithAzureServiceUnavailableCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 40613;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithAzureInsufficientResourcesCode1_ReturnsTrue()
    {
        // Arrange
        var errorCode = 49918;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithAzureInsufficientResourcesCode2_ReturnsTrue()
    {
        // Arrange
        var errorCode = 49919;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithAzureInsufficientResourcesCode3_ReturnsTrue()
    {
        // Arrange
        var errorCode = 49920;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithUniqueConstraintCode_ReturnsFalse()
    {
        // Arrange
        var errorCode = 2601;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransientError_WithForeignKeyViolationCode_ReturnsFalse()
    {
        // Arrange
        var errorCode = 547;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransientError_WithInvalidObjectNameCode_ReturnsFalse()
    {
        // Arrange
        var errorCode = 208;

        // Act
        var result = SqlServerTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeFalse();
    }
}
