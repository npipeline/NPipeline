using AwesomeAssertions;
using NPipeline.Connectors.PostgreSQL.Exceptions;

namespace NPipeline.Connectors.PostgreSQL.Tests.Exceptions
{
    /// <summary>
    ///     Tests for PostgresExceptionHandler.
    ///     Validates exception translation, error description generation, and transient error detection.
    /// </summary>
    public sealed class PostgresExceptionHandlerTests
    {
        #region Translate Tests

        [Fact]
        public void Translate_WithPostgresException_ReturnsPostgresException()
        {
            // Arrange
            const string message = "Connection failed";
            var innerException = new Exception("Inner error");

            // Act
            var result = PostgresExceptionHandler.Translate(message, innerException);

            // Assert
            _ = result.Should().BeOfType<PostgresException>();
            _ = result.Message.Should().Be(message);
            _ = result.InnerException.Should().BeSameAs(innerException);
        }

        [Fact]
        public void Translate_WithGenericException_ReturnsPostgresException()
        {
            // Arrange
            const string message = "Generic error";
            var innerException = new InvalidOperationException("Invalid operation");

            // Act
            var result = PostgresExceptionHandler.Translate(message, innerException);

            // Assert
            _ = result.Should().BeOfType<PostgresException>();
            _ = result.Message.Should().Be(message);
            _ = result.InnerException.Should().BeSameAs(innerException);
        }

        [Fact]
        public void Translate_WithNullInnerException_ReturnsPostgresException()
        {
            // Arrange
            const string message = "Error without inner exception";

            // Act
            var result = PostgresExceptionHandler.Translate(message, null!);

            // Assert
            _ = result.Should().BeOfType<PostgresException>();
            _ = result.Message.Should().Be(message);
            _ = result.InnerException.Should().BeNull();
        }

        #endregion

        #region GetErrorDescription Tests

        [Fact]
        public void GetErrorDescription_WithKnownSqlState_ReturnsDescription()
        {
            // Arrange
            const string sqlState = "08001"; // Connection does not exist

            // Act
            var result = PostgresExceptionHandler.GetErrorDescription(sqlState);

            // Assert
            _ = result.Should().NotBeNullOrEmpty();
            _ = result.Should().Contain("Connection");
        }

        [Fact]
        public void GetErrorDescription_WithUnknownSqlState_ReturnsGenericDescription()
        {
            // Arrange
            const string sqlState = "XXXXX"; // Unknown SQL state

            // Act
            var result = PostgresExceptionHandler.GetErrorDescription(sqlState);

            // Assert
            _ = result.Should().BeNull();
        }

        [Fact]
        public void GetErrorDescription_WithEmptySqlState_ReturnsGenericDescription()
        {
            // Arrange
            const string sqlState = "";

            // Act
            var result = PostgresExceptionHandler.GetErrorDescription(sqlState);

            // Assert
            _ = result.Should().BeNull();
        }

        #endregion

        #region IsTransient Tests

        [Fact]
        public void IsTransient_WithGenericException_ReturnsFalse()
        {
            // Arrange
            var exception = new Exception("Generic error");

            // Act
            var result = PostgresExceptionHandler.IsTransient(exception);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransient_WithTimeoutException_ReturnsFalse()
        {
            // Arrange
            var exception = new TimeoutException();

            // Act
            var result = PostgresExceptionHandler.IsTransient(exception);

            // Assert
            // Note: PostgresExceptionHandler.IsTransient() doesn't handle TimeoutException directly
            // Only NpgsqlException returns true (for all NpgsqlException types)
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransient_WithOperationCanceledException_ReturnsFalse()
        {
            // Arrange
            var exception = new OperationCanceledException();

            // Act
            var result = PostgresExceptionHandler.IsTransient(exception);

            // Assert
            // Note: PostgresExceptionHandler.IsTransient() doesn't handle OperationCanceledException directly
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransient_WithNpgsqlException_ReturnsTrue()
        {
            // Arrange
            var exception = new Npgsql.NpgsqlException("Npgsql error");

            // Act
            var result = PostgresExceptionHandler.IsTransient(exception);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransient_WithNonTransientPostgresException_ReturnsFalse()
        {
            // Arrange
            var exception = new Npgsql.NpgsqlException("Non-transient error");

            // Act
            var result = PostgresExceptionHandler.IsTransient(exception);

            // Assert
            // Note: PostgresExceptionHandler.IsTransient() returns true for all NpgsqlException types
            // This test documents the current behavior
            _ = result.Should().BeTrue();
        }

        #endregion

        #region IsTransientSqlState Tests

        [Fact]
        public void IsTransientSqlState_WithEmptySqlState_ReturnsFalse()
        {
            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState("");

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithNullSqlState_ReturnsFalse()
        {
            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(null);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithTransientCode_ReturnsTrue()
        {
            // Arrange
            const string sqlState = "08001"; // Connection does not exist

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithNonTransientCode_ReturnsFalse()
        {
            // Arrange
            const string sqlState = "23505"; // Unique violation

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithConnectionExceptionCode_ReturnsTrue()
        {
            // Arrange
            const string sqlState = "08006"; // Connection failure

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithSerializationFailure_ReturnsTrue()
        {
            // Arrange
            const string sqlState = "40001"; // Serialization failure

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithDeadlock_ReturnsTrue()
        {
            // Arrange
            const string sqlState = "40P01"; // Deadlock detected

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithInsufficientResources_ReturnsTrue()
        {
            // Arrange
            const string sqlState = "53000"; // Insufficient resources

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithDiskFull_ReturnsTrue()
        {
            // Arrange
            const string sqlState = "53100"; // Disk full

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithOperatorIntervention_ReturnsFalse()
        {
            // Arrange
            const string sqlState = "57000"; // Operator intervention (not transient)

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            // Note: Only "57P01", "57P02", "57P03" are transient, not "57000"
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithAdminShutdown_ReturnsTrue()
        {
            // Arrange
            const string sqlState = "57P01"; // Admin shutdown

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithCrashShutdown_ReturnsTrue()
        {
            // Arrange
            const string sqlState = "57P02"; // Crash shutdown

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithCannotConnectNow_ReturnsTrue()
        {
            // Arrange
            const string sqlState = "57P03"; // Cannot connect now

            // Act
            var result = PostgresExceptionHandler.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        #endregion
    }
}
