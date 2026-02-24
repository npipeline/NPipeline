using System.Net.Sockets;
using AwesomeAssertions;
using NPipeline.Connectors.Azure.Exceptions;

namespace NPipeline.Connectors.Azure.Tests.Exceptions;

/// <summary>
///     Contract tests for <see cref="ITransientErrorDetector" /> implementations.
///     All implementations should satisfy these base contracts.
/// </summary>
public abstract class ITransientErrorDetectorContractTests<T> where T : ITransientErrorDetector, new()
{
    protected T Detector { get; } = new();

    #region GetRetryDelay Contract Tests

    [Fact]
    public void GetRetryDelay_WithNullException_ShouldReturnNull()
    {
        // Act
        var result = Detector.GetRetryDelay(null);

        // Assert
        _ = result.Should().BeNull();
    }

    #endregion

    #region GetCorrelationId Contract Tests

    [Fact]
    public void GetCorrelationId_WithNullException_ShouldReturnNull()
    {
        // Act
        var result = Detector.GetCorrelationId(null);

        // Assert
        _ = result.Should().BeNull();
    }

    #endregion

    #region IsTransient Contract Tests

    [Fact]
    public void IsTransient_WithNullException_ShouldReturnFalse()
    {
        // Act
        var result = Detector.IsTransient(null);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_WithTimeoutException_ShouldReturnTrue()
    {
        // Arrange
        var exception = new TimeoutException();

        // Act
        var result = Detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithSocketException_ShouldReturnTrue()
    {
        // Arrange
        var exception = new SocketException();

        // Act
        var result = Detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithTaskCanceledException_WithoutToken_ShouldReturnTrue()
    {
        // Arrange
        var exception = new TaskCanceledException();

        // Act
        var result = Detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_WithTaskCanceledException_WithCancelledToken_ShouldReturnFalse()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var exception = new TaskCanceledException(null, null, cts.Token);

        // Act
        var result = Detector.IsTransient(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion

    #region IsRateLimited Contract Tests

    [Fact]
    public void IsRateLimited_WithNullException_ShouldReturnFalse()
    {
        // Act
        var result = Detector.IsRateLimited(null);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void IsRateLimited_WithNonRateLimitedException_ShouldReturnFalse()
    {
        // Arrange
        var exception = new InvalidOperationException();

        // Act
        var result = Detector.IsRateLimited(exception);

        // Assert
        _ = result.Should().BeFalse();
    }

    #endregion
}

/// <summary>
///     Contract tests for <see cref="AzureTransientErrorDetector" />.
/// </summary>
public class AzureTransientErrorDetectorContractTests : ITransientErrorDetectorContractTests<AzureTransientErrorDetector>
{
    // Inherits all contract tests from base class
}
