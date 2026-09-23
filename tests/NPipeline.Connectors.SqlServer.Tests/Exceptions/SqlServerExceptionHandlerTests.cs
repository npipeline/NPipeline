using AwesomeAssertions;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Exceptions;
using SqlServerException = NPipeline.Connectors.SqlServer.Exceptions.SqlServerException;

namespace NPipeline.Connectors.SqlServer.Tests.Exceptions;

/// <summary>
///     Tests for SqlServerExceptionHandler.
///     Validates exception translation, error description generation, and transient error detection.
/// </summary>
public sealed class SqlServerExceptionHandlerTests
{
    #region Handle Tests

    [Fact]
    public void Handle_WithGenericException_ThrowsSqlServerException()
    {
        // Arrange
        var exception = new InvalidOperationException("Invalid operation");
        var configuration = new SqlServerConfiguration();

        // Act & Assert
        var thrown = Assert.Throws<SqlServerException>(() => SqlServerExceptionHandler.Handle(exception, configuration));
        _ = thrown.Message.Should().Contain("Invalid operation");
        _ = thrown.InnerException.Should().BeSameAs(exception);
    }

    [Fact]
    public void Handle_WithTimeoutException_ThrowsSqlServerException()
    {
        // Arrange
        var exception = new TimeoutException("Timeout");
        var configuration = new SqlServerConfiguration();

        // Act & Assert
        var thrown = Assert.Throws<SqlServerException>(() => SqlServerExceptionHandler.Handle(exception, configuration));
        _ = thrown.Message.Should().Contain("Timeout");
        _ = thrown.IsTransient.Should().BeTrue();
        _ = thrown.InnerException.Should().BeSameAs(exception);
    }

    [Fact]
    public void Handle_WithAlreadyWrappedException_RethrowsSameException()
    {
        // Arrange
        var exception = new SqlServerException("Already wrapped");
        var configuration = new SqlServerConfiguration();

        // Act & Assert
        var thrown = Assert.Throws<SqlServerException>(() => SqlServerExceptionHandler.Handle(exception, configuration));
        _ = thrown.Should().BeSameAs(exception);
    }

    [Fact]
    public void Handle_WithConnectionException_RethrowsSameException()
    {
        // Arrange
        var exception = new SqlServerConnectionException("Connection failed");
        var configuration = new SqlServerConfiguration();

        // Act & Assert
        var thrown = Assert.Throws<SqlServerConnectionException>(() => SqlServerExceptionHandler.Handle(exception, configuration));
        _ = thrown.Should().BeSameAs(exception);
    }

    [Fact]
    public void Handle_WithMappingException_RethrowsSameException()
    {
        // Arrange
        var exception = new SqlServerMappingException("Mapping failed");
        var configuration = new SqlServerConfiguration();

        // Act & Assert
        var thrown = Assert.Throws<SqlServerMappingException>(() => SqlServerExceptionHandler.Handle(exception, configuration));
        _ = thrown.Should().BeSameAs(exception);
    }

    #endregion

    #region GetErrorCode Tests

    [Fact]
    public void GetErrorCode_WithSqlServerException_ReturnsErrorCode()
    {
        // Arrange
        var exception = new SqlServerException("Error", "12345", true);

        // Act
        var errorCode = SqlServerExceptionHandler.GetErrorCode(exception);

        // Assert
        _ = errorCode.Should().Be("12345");
    }

    [Fact]
    public void GetErrorCode_WithGenericException_ReturnsNull()
    {
        // Arrange
        var exception = new InvalidOperationException("Generic error");

        // Act
        var errorCode = SqlServerExceptionHandler.GetErrorCode(exception);

        // Assert
        _ = errorCode.Should().BeNull();
    }

    #endregion

    #region GetErrorDescription Tests

    [Fact]
    public void GetErrorDescription_WithTimeoutCode_ReturnsDescription()
    {
        // Arrange
        const int errorCode = -2;

        // Act
        var description = SqlServerExceptionHandler.GetErrorDescription(errorCode);

        // Assert
        _ = description.Should().NotBeNullOrEmpty();
        _ = description.Should().Contain("Timeout");
    }

    [Fact]
    public void GetErrorDescription_WithNetworkNotFoundCode_ReturnsDescription()
    {
        // Arrange
        const int errorCode = 53;

        // Act
        var description = SqlServerExceptionHandler.GetErrorDescription(errorCode);

        // Assert
        _ = description.Should().NotBeNullOrEmpty();
        _ = description.Should().Contain("network");
    }

    [Fact]
    public void GetErrorDescription_WithDeadlockCode_ReturnsDescription()
    {
        // Arrange
        const int errorCode = 1205;

        // Act
        var description = SqlServerExceptionHandler.GetErrorDescription(errorCode);

        // Assert
        _ = description.Should().NotBeNullOrEmpty();
        _ = description.Should().Contain("Deadlock");
    }

    [Fact]
    public void GetErrorDescription_WithUniqueConstraintCode_ReturnsDescription()
    {
        // Arrange
        const int errorCode = 2601;

        // Act
        var description = SqlServerExceptionHandler.GetErrorDescription(errorCode);

        // Assert
        _ = description.Should().NotBeNullOrEmpty();
        _ = description.Should().Contain("duplicate");
    }

    [Fact]
    public void GetErrorDescription_WithForeignKeyViolationCode_ReturnsDescription()
    {
        // Arrange
        const int errorCode = 547;

        // Act
        var description = SqlServerExceptionHandler.GetErrorDescription(errorCode);

        // Assert
        _ = description.Should().NotBeNullOrEmpty();
        _ = description.Should().Contain("Foreign key");
    }

    [Fact]
    public void GetErrorDescription_WithUnknownCode_ReturnsNull()
    {
        // Arrange
        const int errorCode = 99999;

        // Act
        var description = SqlServerExceptionHandler.GetErrorDescription(errorCode);

        // Assert
        _ = description.Should().BeNull();
    }

    [Fact]
    public void GetErrorDescription_WithAzureServiceBusyCode_ReturnsDescription()
    {
        // Arrange
        const int errorCode = 40501;

        // Act
        var description = SqlServerExceptionHandler.GetErrorDescription(errorCode);

        // Assert
        _ = description.Should().NotBeNullOrEmpty();
        _ = description.Should().Contain("busy");
    }

    [Fact]
    public void GetErrorDescription_WithStringTruncationCode_ReturnsDescription()
    {
        // Arrange
        const int errorCode = 8152;

        // Act
        var description = SqlServerExceptionHandler.GetErrorDescription(errorCode);

        // Assert
        _ = description.Should().NotBeNullOrEmpty();
        _ = description.Should().Contain("truncated");
    }

    #endregion
}
