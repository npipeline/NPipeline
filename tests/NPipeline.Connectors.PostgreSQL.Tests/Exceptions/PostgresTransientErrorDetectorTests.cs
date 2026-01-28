using AwesomeAssertions;
using NPipeline.Connectors.PostgreSQL.Exceptions;

namespace NPipeline.Connectors.PostgreSQL.Tests.Exceptions
{
    public class PostgresTransientErrorDetectorTests
    {
        [Fact]
        public void IsTransientError_WithTimeoutException_ReturnsTrue()
        {
            // Arrange
            var exception = new TimeoutException("Connection timeout");

            // Act
            var result = PostgresTransientErrorDetector.IsTransientError(exception);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientError_WithOperationCanceledException_ReturnsTrue()
        {
            // Arrange
            var exception = new OperationCanceledException("Operation canceled");

            // Act
            var result = PostgresTransientErrorDetector.IsTransientError(exception);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientError_WithGenericException_ReturnsFalse()
        {
            // Arrange
            var exception = new InvalidOperationException("Invalid operation");

            // Act
            var result = PostgresTransientErrorDetector.IsTransientError(exception);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientError_WithNull_ReturnsFalse()
        {
            // Arrange
            Exception? exception = null;

            // Act
            var result = PostgresTransientErrorDetector.IsTransientError(exception!);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithConnectionFailure_ReturnsTrue()
        {
            // Arrange
            var sqlState = "08006";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithConnectionDoesNotExist_ReturnsFalse()
        {
            // Arrange
            const string sqlState = "08003"; // Connection does not exist (not in TransientErrorCodes)

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            // Note: "08003" is not in TransientErrorCodes set
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithSerializationFailure_ReturnsTrue()
        {
            // Arrange
            var sqlState = "40001";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithDeadlockDetected_ReturnsTrue()
        {
            // Arrange
            var sqlState = "40P01";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithInsufficientResources_ReturnsTrue()
        {
            // Arrange
            var sqlState = "53000";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithDiskFull_ReturnsTrue()
        {
            // Arrange
            var sqlState = "53100";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithOutOfMemory_ReturnsTrue()
        {
            // Arrange
            var sqlState = "53200";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithTooManyConnections_ReturnsFalse()
        {
            // Arrange
            var sqlState = "53300"; // too_many_connections

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithAdminShutdown_ReturnsTrue()
        {
            // Arrange
            var sqlState = "57P01";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithCrashShutdown_ReturnsTrue()
        {
            // Arrange
            var sqlState = "57P02";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithCannotConnectNow_ReturnsTrue()
        {
            // Arrange
            var sqlState = "57P03";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeTrue();
        }

        [Fact]
        public void IsTransientSqlState_WithSyntaxError_ReturnsFalse()
        {
            // Arrange
            var sqlState = "42601";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithUniqueViolation_ReturnsFalse()
        {
            // Arrange
            var sqlState = "23505";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithNullSqlState_ReturnsFalse()
        {
            // Arrange
            string? sqlState = null;

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState!);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithEmptySqlState_ReturnsFalse()
        {
            // Arrange
            var sqlState = string.Empty;

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithInvalidAuthorization_ReturnsFalse()
        {
            // Arrange
            var sqlState = "28000";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeFalse();
        }

        [Fact]
        public void IsTransientSqlState_WithUndefinedTable_ReturnsFalse()
        {
            // Arrange
            var sqlState = "42P01";

            // Act
            var result = PostgresTransientErrorDetector.IsTransientSqlState(sqlState);

            // Assert
            _ = result.Should().BeFalse();
        }
    }
}
