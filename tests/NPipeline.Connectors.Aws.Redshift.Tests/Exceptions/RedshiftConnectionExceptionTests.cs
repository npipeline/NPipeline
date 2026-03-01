using NPipeline.Connectors.Aws.Redshift.Exceptions;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Exceptions;

public class RedshiftConnectionExceptionTests
{
    [Fact]
    public void DefaultConstructor_CreatesExceptionWithDefaultMessage()
    {
        // Act
        var exception = new RedshiftConnectionException();

        // Assert
        exception.Message.Should().Contain("connection");
    }

    [Fact]
    public void MessageConstructor_CreatesExceptionWithMessage()
    {
        // Arrange
        const string message = "Connection failed";

        // Act
        var exception = new RedshiftConnectionException(message);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void InnerExceptionConstructor_CreatesExceptionWithInnerException()
    {
        // Arrange
        const string message = "Connection failed";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new RedshiftConnectionException(message, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.InnerException.Should().Be(innerException);
    }

    [Fact]
    public void ConnectionStringConstructor_RedactsPassword()
    {
        // Arrange
        const string connectionString = "Host=localhost;Database=test;Username=user;Password=secret123";

        // Act
        var exception = new RedshiftConnectionException("Connection failed", connectionString);

        // Assert
        exception.ConnectionString.Should().NotContain("secret123");
        exception.ConnectionString.Should().Contain("Password=***");
    }

    [Fact]
    public void ConnectionStringConstructor_RedactsPWD()
    {
        // Arrange
        const string connectionString = "Host=localhost;Database=test;Username=user;PWD=secret123";

        // Act
        var exception = new RedshiftConnectionException("Connection failed", connectionString);

        // Assert
        exception.ConnectionString.Should().NotContain("secret123");
        exception.ConnectionString.Should().Contain("PWD=***");
    }

    [Fact]
    public void ConnectionStringConstructor_WithNullConnectionString_HandlesGracefully()
    {
        // Act
        var exception = new RedshiftConnectionException("Connection failed", (string?)null);

        // Assert
        exception.ConnectionString.Should().BeNull();
    }

    [Fact]
    public void ConnectionStringConstructor_WithEmptyConnectionString_HandlesGracefully()
    {
        // Act
        var exception = new RedshiftConnectionException("Connection failed", string.Empty);

        // Assert
        exception.ConnectionString.Should().BeEmpty();
    }

    [Fact]
    public void ErrorCodeConstructor_CreatesExceptionWithErrorCode()
    {
        // Arrange
        const string message = "Connection failed";
        const string errorCode = "08006";

        // Act
        var exception = new RedshiftConnectionException(message, errorCode);

        // Assert
        exception.Message.Should().Be(message);
    }
}
