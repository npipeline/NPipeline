using System.Net.Sockets;
using NPipeline.Connectors.Aws.Redshift.Exceptions;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Exceptions;

public class RedshiftTransientErrorDetectorTests
{
    [Fact]
    public void IsTransient_SocketException_ReturnsTrue()
    {
        // Arrange
        var exception = new SocketException();

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_TimeoutException_ReturnsTrue()
    {
        // Arrange
        var exception = new TimeoutException();

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_NullException_ReturnsFalse()
    {
        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(null);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_NonNpgsqlException_ReturnsFalse()
    {
        // Arrange
        var exception = new InvalidOperationException("Some error");

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_AggregateExceptionWithTransientInner_ReturnsTrue()
    {
        // Arrange
        var exception = new AggregateException(new TimeoutException());

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_AggregateExceptionWithNonTransientInner_ReturnsFalse()
    {
        // Arrange
        var exception = new AggregateException(new InvalidOperationException());

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_RedshiftExceptionWithTransientSqlState_ReturnsTrue()
    {
        // Arrange - Create RedshiftException with a transient SQL state
        var exception = new RedshiftException("Connection error", "SELECT 1", "08006", null);

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_RedshiftExceptionWithNonTransientSqlState_ReturnsFalse()
    {
        // Arrange - Create RedshiftException with a non-transient SQL state
        var exception = new RedshiftException("Syntax error", "SELECT 1", "42601", null);

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_ExceptionWithTransientInnerException_ReturnsTrue()
    {
        // Arrange
        var exception = new InvalidOperationException("Wrapper", new TimeoutException());

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_ExceptionWithNonTransientInnerException_ReturnsFalse()
    {
        // Arrange
        var exception = new InvalidOperationException("Wrapper", new ArgumentException());

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_SerializationFailure_ReturnsTrue()
    {
        // Arrange — SQLSTATE 40001 is serialization_failure (concurrent transaction conflict)
        var exception = new RedshiftException("Serialization failure", "SELECT 1", "40001", null);

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_TooManyConnections_ReturnsTrue()
    {
        // Arrange — SQLSTATE 53300 is too_many_connections
        var exception = new RedshiftException("Too many connections", "SELECT 1", "53300", null);

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_SyntaxError_ReturnsFalse()
    {
        // Arrange — SQLSTATE 42601 is syntax_error, which is not transient
        var exception = new RedshiftException("Syntax error", "SELEKT 1", "42601", null);

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_ObjectNotExist_ReturnsFalse()
    {
        // Arrange — SQLSTATE 42P01 is undefined_table, which is not transient
        var exception = new RedshiftException("Table not found", "SELECT * FROM nonexistent", "42P01", null);

        // Act
        var result = RedshiftTransientErrorDetector.IsTransient(exception);

        // Assert
        result.Should().BeFalse();
    }
}
