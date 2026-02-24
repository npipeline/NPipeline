using System.Net.Sockets;
using AwesomeAssertions;
using Azure;
using NPipeline.Connectors.Azure.Exceptions;

namespace NPipeline.Connectors.Azure.Tests.Exceptions;

public class AzureTransientErrorDetectorTests
{
    private readonly AzureTransientErrorDetector _detector;

    public AzureTransientErrorDetectorTests()
    {
        _detector = new AzureTransientErrorDetector();
    }

    #region IsTransient Tests - Null Exception

    [Fact]
    public void IsTransient_WithNullException_ShouldReturnFalse()
    {
        // Act
        var result = _detector.IsTransient(null);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion

    #region IsTransient Tests - RequestFailedException

    [Theory]
    [InlineData(408)] // RequestTimeout
    [InlineData(410)] // Gone
    [InlineData(429)] // TooManyRequests
    [InlineData(503)] // ServiceUnavailable
    [InlineData(449)] // RetryWith
    public void IsTransient_WithTransientRequestFailedException_ShouldReturnTrue(int statusCode)
    {
        // Arrange
        var exception = new RequestFailedException(statusCode, "Test error");

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Theory]
    [InlineData(400)] // BadRequest
    [InlineData(401)] // Unauthorized
    [InlineData(403)] // Forbidden
    [InlineData(404)] // NotFound
    [InlineData(409)] // Conflict
    [InlineData(500)] // InternalServerError
    public void IsTransient_WithNonTransientRequestFailedException_ShouldReturnFalse(int statusCode)
    {
        // Arrange
        var exception = new RequestFailedException(statusCode, "Test error");

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion

    #region IsTransient Tests - Base Exception Types

    [Fact]
    public void IsTransient_WithTimeoutException_ShouldReturnTrue()
    {
        // Arrange
        var exception = new TimeoutException();

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithSocketException_ShouldReturnTrue()
    {
        // Arrange
        var exception = new SocketException();

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithTaskCanceledException_ShouldReturnTrue()
    {
        // Arrange
        var exception = new TaskCanceledException();

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithOperationCanceledException_ShouldReturnTrue()
    {
        // Arrange
        var exception = new OperationCanceledException();

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithTaskCanceledExceptionWithToken_ShouldReturnFalse()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var exception = new TaskCanceledException(null, null, cts.Token);

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithOperationCanceledExceptionWithToken_ShouldReturnFalse()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var exception = new OperationCanceledException(cts.Token);

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion

    #region IsTransient Tests - HttpRequestException

    [Fact]
    public void IsTransient_WithHttpRequestException_WithTimeoutMessage_ShouldReturnTrue()
    {
        // Arrange
        var exception = new HttpRequestException("The operation has timed out");

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithHttpRequestException_WithTimedOutMessage_ShouldReturnTrue()
    {
        // Arrange
        var exception = new HttpRequestException("The operation timed out");

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithHttpRequestException_WithTimeOutPhrase_ShouldReturnTrue()
    {
        // Arrange
        var exception = new HttpRequestException("Request failed due to time out while connecting");

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithHttpRequestException_WithConnectionResetMessage_ShouldReturnTrue()
    {
        // Arrange
        var exception = new HttpRequestException("The connection was reset");

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithHttpRequestException_WithConnectionMessage_ShouldReturnTrue()
    {
        // Arrange
        var exception = new HttpRequestException("A connection attempt failed");

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithHttpRequestException_WithInnerTimeoutException_ShouldReturnTrue()
    {
        // Arrange
        var exception = new HttpRequestException("Network request failed", new TimeoutException());

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithHttpRequestException_WithInnerSocketException_ShouldReturnTrue()
    {
        // Arrange
        var exception = new HttpRequestException("Network error", new SocketException());

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithHttpRequestException_WithInnerInvalidOperationException_ShouldReturnFalse()
    {
        // Arrange
        var exception = new HttpRequestException("Network request failed", new InvalidOperationException());

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithHttpRequestException_WithGenericMessage_ShouldReturnFalse()
    {
        // Arrange
        var exception = new HttpRequestException("Some other error");

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion

    #region IsTransient Tests - Aggregate Exception

    [Fact]
    public void IsTransient_WithAggregateException_ContainingTransientException_ShouldReturnTrue()
    {
        // Arrange
        var innerException = new TimeoutException();
        var exception = new AggregateException(innerException);

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithAggregateException_ContainingNonTransientException_ShouldReturnFalse()
    {
        // Arrange
        var innerException = new InvalidOperationException();
        var exception = new AggregateException(innerException);

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithAggregateException_ContainingMultipleExceptions_ShouldReturnTrueIfAnyTransient()
    {
        // Arrange
        var exception = new AggregateException(
            new InvalidOperationException(),
            new TimeoutException(),
            new ArgumentException()
        );

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithAggregateException_ContainingAllNonTransientExceptions_ShouldReturnFalse()
    {
        // Arrange
        var exception = new AggregateException(
            new InvalidOperationException(),
            new ArgumentException(),
            new NullReferenceException()
        );

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion

    #region IsTransient Tests - Inner Exception

    [Fact]
    public void IsTransient_WithInnerTransientException_ShouldReturnTrue()
    {
        // Arrange
        var innerException = new TimeoutException();
        var exception = new InvalidOperationException("Outer", innerException);

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithNestedInnerTransientException_ShouldReturnTrue()
    {
        // Arrange
        var timeoutException = new TimeoutException();
        var innerException = new InvalidOperationException("Inner", timeoutException);
        var exception = new ArgumentException("Outer", innerException);

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithDeeplyNestedTransientException_ShouldReturnTrue()
    {
        // Arrange
        var socketException = new SocketException();
        var level1 = new InvalidOperationException("Level 1", socketException);
        var level2 = new ArgumentException("Level 2", level1);
        var level3 = new NotSupportedException("Level 3", level2);

        // Act
        var result = _detector.IsTransient(level3);

        // Assert
        _ = result.Should().BeTrue();
    }

    #endregion

    #region IsTransient Tests - Non-Transient Exceptions

    [Fact]
    public void IsTransient_WithInvalidOperationException_ShouldReturnFalse()
    {
        // Arrange
        var exception = new InvalidOperationException();

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithArgumentException_ShouldReturnFalse()
    {
        // Arrange
        var exception = new ArgumentException();

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithNullReferenceException_ShouldReturnFalse()
    {
        // Arrange
        var exception = new NullReferenceException();

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithNotSupportedException_ShouldReturnFalse()
    {
        // Arrange
        var exception = new NotSupportedException();

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithFormatException_ShouldReturnFalse()
    {
        // Arrange
        var exception = new FormatException();

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion

    #region IsRateLimited Tests

    [Fact]
    public void IsRateLimited_WithRequestFailedException429_ShouldReturnTrue()
    {
        // Arrange
        var exception = new RequestFailedException(429, "Too many requests");

        // Act
        var result = _detector.IsRateLimited(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsRateLimited_WithRequestFailedException503_ShouldReturnFalse()
    {
        // Arrange
        var exception = new RequestFailedException(503, "Service unavailable");

        // Assert
        var result = _detector.IsRateLimited(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsRateLimited_WithNullException_ShouldReturnFalse()
    {
        // Act
        var result = _detector.IsRateLimited(null);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsRateLimited_WithNonRateLimitedException_ShouldReturnFalse()
    {
        // Arrange
        var exception = new InvalidOperationException();

        // Act
        var result = _detector.IsRateLimited(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsRateLimited_WithHttpRequestException_ShouldReturnFalse()
    {
        // Arrange
        var exception = new HttpRequestException("Error");

        // Act
        var result = _detector.IsRateLimited(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion

    #region GetRetryDelay Tests

    [Fact]
    public void GetRetryDelay_WithNullException_ShouldReturnNull()
    {
        // Act
        var result = _detector.GetRetryDelay(null);

        // Assert
        _ = result.Should().BeNull();
    }

    [Fact]
    public void GetRetryDelay_WithNonAzureException_ShouldReturnNull()
    {
        // Arrange
        var exception = new InvalidOperationException();

        // Act
        var result = _detector.GetRetryDelay(exception);

        // Assert
        _ = result.Should().BeNull();
    }

    [Fact]
    public void GetRetryDelay_WithRequestFailedException_ShouldReturnNull()
    {
        // Arrange
        var exception = new RequestFailedException(429, "Too many requests");

        // Act
        var result = _detector.GetRetryDelay(exception);

        // Assert - Base implementation returns null, service-specific implementations should override
        _ = result.Should().BeNull();
    }

    #endregion

    #region GetCorrelationId Tests

    [Fact]
    public void GetCorrelationId_WithNullException_ShouldReturnNull()
    {
        // Act
        var result = _detector.GetCorrelationId(null);

        // Assert
        _ = result.Should().BeNull();
    }

    [Fact]
    public void GetCorrelationId_WithNonAzureException_ShouldReturnNull()
    {
        // Arrange
        var exception = new InvalidOperationException();

        // Act
        var result = _detector.GetCorrelationId(exception);

        // Assert
        _ = result.Should().BeNull();
    }

    [Fact]
    public void GetCorrelationId_WithRequestFailedException_ShouldReturnNull()
    {
        // Arrange
        var exception = new RequestFailedException(500, "Server error");

        // Act
        var result = _detector.GetCorrelationId(exception);

        // Assert - Base implementation returns null, service-specific implementations should override
        _ = result.Should().BeNull();
    }

    #endregion
}
