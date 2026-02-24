using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Exceptions;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Exceptions;

/// <summary>
///     Tests for CosmosDB-specific transient error detection.
///     Base class exception tests are in NPipeline.Connectors.Azure.Tests.
/// </summary>
public class CosmosTransientErrorDetectorTests
{
    private readonly CosmosTransientErrorDetector _detector;

    public CosmosTransientErrorDetectorTests()
    {
        _detector = new CosmosTransientErrorDetector();
    }

    #region IsTransient Tests - CosmosException

    [Theory]
    [InlineData((int)HttpStatusCode.RequestTimeout)] // 408
    [InlineData((int)HttpStatusCode.ServiceUnavailable)] // 503
    [InlineData((int)HttpStatusCode.TooManyRequests)] // 429
    [InlineData(449)] // RetryWith
    public void IsTransient_WithTransientCosmosStatusCode_ShouldReturnTrue(int statusCode)
    {
        // Arrange
        var exception = CreateCosmosException((HttpStatusCode)statusCode, 0);

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Theory]
    [InlineData((int)HttpStatusCode.BadRequest)] // 400
    [InlineData((int)HttpStatusCode.Unauthorized)] // 401
    [InlineData((int)HttpStatusCode.Forbidden)] // 403
    [InlineData((int)HttpStatusCode.NotFound)] // 404
    [InlineData((int)HttpStatusCode.Conflict)] // 409
    [InlineData((int)HttpStatusCode.InternalServerError)] // 500
    public void IsTransient_WithNonTransientCosmosStatusCode_ShouldReturnFalse(int statusCode)
    {
        // Arrange
        var exception = CreateCosmosException((HttpStatusCode)statusCode, 0);

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Theory]
    [InlineData(4100)]
    [InlineData(4101)]
    [InlineData(4150)]
    [InlineData(4199)]
    public void IsTransient_WithTransientSubStatusCode_ShouldReturnTrue(int subStatusCode)
    {
        // Arrange
        var exception = CreateCosmosException(HttpStatusCode.OK, subStatusCode);

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Theory]
    [InlineData(3200)] // Request entity too large
    [InlineData(4000)]
    [InlineData(4040)]
    [InlineData(4200)]
    public void IsTransient_WithNonTransientSubStatusCode_ShouldReturnFalse(int subStatusCode)
    {
        // Arrange
        var exception = CreateCosmosException(HttpStatusCode.OK, subStatusCode);

        // Act
        var result = _detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion

    #region IsRateLimited Tests - CosmosException

    [Fact]
    public void IsRateLimited_With429CosmosException_ShouldReturnTrue()
    {
        // Arrange
        var exception = CreateCosmosException(HttpStatusCode.TooManyRequests, 0);

        // Act
        var result = _detector.IsRateLimited(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsRateLimited_With503CosmosException_ShouldReturnFalse()
    {
        // Arrange
        var exception = CreateCosmosException(HttpStatusCode.ServiceUnavailable, 0);

        // Act
        var result = _detector.IsRateLimited(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion

    #region GetRetryDelay Tests - CosmosException

    [Fact]
    public void GetRetryDelay_WithCosmosException_ShouldReturnExceptionRetryAfterValue()
    {
        // Arrange
        var exception = CreateCosmosExceptionWithRetryAfter(HttpStatusCode.TooManyRequests);

        // Act
        var result = _detector.GetRetryDelay(exception);

        // Assert
        _ = result.Should().Be(exception.RetryAfter);
    }

    [Fact]
    public void GetRetryDelay_WithNonCosmosException_ShouldReturnNull()
    {
        // Arrange
        var exception = new InvalidOperationException();

        // Act
        var result = _detector.GetRetryDelay(exception);

        // Assert
        _ = result.Should().BeNull();
    }

    #endregion

    #region GetCorrelationId Tests - CosmosException

    [Fact]
    public void GetCorrelationId_WithCosmosException_ShouldReturnActivityId()
    {
        // Arrange
        const string activityId = "test-activity-id-123";
        var exception = CreateCosmosExceptionWithActivityId(HttpStatusCode.InternalServerError, activityId);

        // Act
        var result = _detector.GetCorrelationId(exception);

        // Assert
        _ = result.Should().Be(activityId);
    }

    [Fact]
    public void GetCorrelationId_WithNonCosmosException_ShouldReturnNull()
    {
        // Arrange
        var exception = new InvalidOperationException();

        // Act
        var result = _detector.GetCorrelationId(exception);

        // Assert
        _ = result.Should().BeNull();
    }

    #endregion

    #region Singleton Instance Tests

    [Fact]
    public void Instance_ShouldReturnSameInstance()
    {
        // Act & Assert
        _ = CosmosTransientErrorDetector.Instance.Should().BeSameAs(CosmosTransientErrorDetector.Instance);
    }

    [Fact]
    public void Instance_ShouldBeOfTypeCosmosTransientErrorDetector()
    {
        // Act & Assert
        _ = CosmosTransientErrorDetector.Instance.Should().BeOfType<CosmosTransientErrorDetector>();
    }

    #endregion

    #region Static Extension Methods Tests

    [Fact]
    public void StaticIsTransient_WithTransientException_ShouldReturnTrue()
    {
        // Arrange
        var exception = new TimeoutException();

        // Act
        var result = CosmosTransientErrorDetectorExtensions.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void StaticIsRateLimited_With429Exception_ShouldReturnTrue()
    {
        // Arrange
        var exception = CreateCosmosException(HttpStatusCode.TooManyRequests, 0);

        // Act
        var result = CosmosTransientErrorDetectorExtensions.IsRateLimited(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void StaticGetRetryDelay_WithCosmosException_ShouldReturnExceptionRetryAfterValue()
    {
        // Arrange
        var exception = CreateCosmosExceptionWithRetryAfter(HttpStatusCode.TooManyRequests);

        // Act
        var result = CosmosTransientErrorDetectorExtensions.GetRetryDelay(exception);

        // Assert
        _ = result.Should().Be(exception.RetryAfter);
    }

    [Fact]
    public void StaticGetActivityId_WithCosmosException_ShouldReturnActivityId()
    {
        // Arrange
        const string activityId = "activity-id-456";
        var exception = CreateCosmosExceptionWithActivityId(HttpStatusCode.InternalServerError, activityId);

        // Act
        var result = CosmosTransientErrorDetectorExtensions.GetActivityId(exception);

        // Assert
        _ = result.Should().Be(activityId);
    }

    #endregion

    #region Helper Methods

    private static CosmosException CreateCosmosException(HttpStatusCode statusCode, int subStatusCode)
    {
        // CosmosException requires specific parameters - use the constructor that takes individual values
        return new CosmosException(
            $"Test CosmosException with status {statusCode}",
            statusCode,
            subStatusCode,
            $"activity-{Guid.NewGuid()}",
            0.0
        );
    }

    private static CosmosException CreateCosmosExceptionWithRetryAfter(HttpStatusCode statusCode)
    {
        return new CosmosException(
            $"Test CosmosException with status {statusCode}",
            statusCode,
            0,
            $"activity-{Guid.NewGuid()}",
            0.0
        );
    }

    private static CosmosException CreateCosmosExceptionWithActivityId(HttpStatusCode statusCode, string activityId)
    {
        return new CosmosException(
            "Test CosmosException with activity ID",
            statusCode,
            0,
            activityId,
            0.0
        );
    }

    #endregion
}
