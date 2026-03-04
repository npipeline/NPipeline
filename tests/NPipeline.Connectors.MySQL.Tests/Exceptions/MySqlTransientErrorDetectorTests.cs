using AwesomeAssertions;
using NPipeline.Connectors.MySql.Exceptions;

namespace NPipeline.Connectors.MySql.Tests.Exceptions;

public class MySqlTransientErrorDetectorTests
{
    [Fact]
    public void IsTransient_WithTimeoutException_ReturnsTrue()
    {
        // Arrange
        var exception = new TimeoutException("Connection timeout");

        // Act
        var result = MySqlTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithOperationCanceledException_ReturnsTrue()
    {
        // Arrange
        var exception = new OperationCanceledException("Operation canceled");

        // Act
        var result = MySqlTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithGenericException_ReturnsFalse()
    {
        // Arrange
        var exception = new InvalidOperationException("Invalid operation");

        // Act
        var result = MySqlTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithNull_ReturnsFalse()
    {
        // Arrange
        Exception? exception = null;

        // Act
        var result = MySqlTransientErrorDetector.IsTransient(exception!);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithInvalidOperationExceptionWithTimeout_ReturnsTrue()
    {
        // Arrange
        var exception = new InvalidOperationException("Connection timeout");

        // Act
        var result = MySqlTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithInvalidOperationExceptionWithConnection_ReturnsTrue()
    {
        // Arrange
        var exception = new InvalidOperationException("The connection is closed");

        // Act
        var result = MySqlTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithInvalidOperationExceptionGeneric_ReturnsFalse()
    {
        // Arrange
        var exception = new InvalidOperationException("Invalid operation");

        // Act
        var result = MySqlTransientErrorDetector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransientError_WithTooManyConnectionsCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 1040; // ER_CON_COUNT_ERROR

        // Act
        var result = MySqlTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithLockWaitTimeoutCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 1205; // ER_LOCK_WAIT_TIMEOUT

        // Act
        var result = MySqlTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithDeadlockCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 1213; // ER_LOCK_DEADLOCK

        // Act
        var result = MySqlTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithServerGoneAwayCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 2006; // CR_SERVER_GONE_ERROR

        // Act
        var result = MySqlTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithServerLostCode_ReturnsTrue()
    {
        // Arrange
        var errorCode = 2013; // CR_SERVER_LOST

        // Act
        var result = MySqlTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransientError_WithUniqueConstraintCode_ReturnsFalse()
    {
        // Arrange
        var errorCode = 1062; // ER_DUP_ENTRY

        // Act
        var result = MySqlTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransientError_WithForeignKeyViolationCode_ReturnsFalse()
    {
        // Arrange
        var errorCode = 1216; // ER_NO_REFERENCED_ROW

        // Act
        var result = MySqlTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransientError_WithTableNotFoundCode_ReturnsFalse()
    {
        // Arrange
        var errorCode = 1146; // ER_NO_SUCH_TABLE

        // Act
        var result = MySqlTransientErrorDetector.IsTransientError(errorCode);

        // Assert
        _ = result.Should().BeFalse();
    }
}
