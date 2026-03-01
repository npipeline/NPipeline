using NPipeline.Connectors.Aws.Redshift.Exceptions;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Exceptions;

public class RedshiftExceptionTests
{
    [Fact]
    public void DefaultConstructor_CreatesExceptionWithDefaultMessage()
    {
        // Act
        var exception = new RedshiftException();

        // Assert
        exception.Message.Should().Contain("Redshift");
    }

    [Fact]
    public void MessageConstructor_CreatesExceptionWithMessage()
    {
        // Arrange
        const string message = "Test error message";

        // Act
        var exception = new RedshiftException(message);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void InnerExceptionConstructor_CreatesExceptionWithInnerException()
    {
        // Arrange
        const string message = "Test error message";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new RedshiftException(message, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.InnerException.Should().Be(innerException);
    }

    [Fact]
    public void ToString_WithSql_IncludesSqlInOutput()
    {
        // Arrange
        var exception = new RedshiftException("Error", "SELECT * FROM test");

        // Act
        var result = exception.ToString();

        // Assert
        result.Should().Contain("SQL: SELECT * FROM test");
    }

    [Fact]
    public void ToString_WithSqlState_IncludesSqlStateInOutput()
    {
        // Arrange
        var exception = new RedshiftException("Error", "SELECT 1", "42601", null);

        // Act
        var result = exception.ToString();

        // Assert
        result.Should().Contain("SQLState: 42601");
    }

    [Fact]
    public void Sql_PropertyIsSetCorrectly()
    {
        // Arrange
        const string sql = "SELECT * FROM users";

        // Act
        var exception = new RedshiftException("Error", sql);

        // Assert
        exception.Sql.Should().Be(sql);
    }

    [Fact]
    public void SqlState_PropertyIsSetCorrectly()
    {
        // Arrange
        const string sqlState = "42601";

        // Act
        var exception = new RedshiftException("Error", "SELECT 1", sqlState, null);

        // Assert
        exception.SqlState.Should().Be(sqlState);
    }
}
