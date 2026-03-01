using NPipeline.Connectors.Aws.Redshift.Exceptions;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Exceptions;

public class RedshiftExceptionHandlerTests
{
    [Fact]
    public void GetRetryDelay_FirstAttempt_ReturnsBaseDelay()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler(
            3,
            TimeSpan.FromSeconds(2));

        // Act
        var delay = handler.GetRetryDelay(1);

        // Assert
        // Should be around 2 seconds ± 25% (1.5 to 2.5 seconds)
        delay.Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(1.5));
        delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(2.5));
    }

    [Fact]
    public void GetRetryDelay_ThirdAttempt_ReturnsExponentialDelay()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler(
            3,
            TimeSpan.FromSeconds(2));

        // Act
        var delay = handler.GetRetryDelay(3);

        // Assert
        // Should be around 8 seconds (2 * 2^2) ± 25% (6 to 10 seconds)
        delay.Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(6));
        delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void GetRetryDelay_IsCapped_AtSixtySeconds()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler(
            10,
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(60));

        // Act
        var delay = handler.GetRetryDelay(10);

        // Assert
        delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(60));
    }

    [Fact]
    public void GetRetryDelay_IncludesJitter()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler(
            3,
            TimeSpan.FromSeconds(2));

        // Act - Get multiple delays and verify they vary
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 10; i++)
        {
            delays.Add(handler.GetRetryDelay(1));
        }

        // Assert - Delays should not all be identical (jitter is applied)
        delays.Distinct().Count().Should().BeGreaterThan(1);
    }

    [Fact]
    public void ShouldRetry_WhenMaxAttemptsIsZero_ReturnsFalse()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler(0);

        // Act
        var result = handler.ShouldRetry(new TimeoutException(), 1);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void ShouldRetry_WhenAttemptCountExceedsMax_ReturnsFalse()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler();

        // Act
        var result = handler.ShouldRetry(new TimeoutException(), 3);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void ShouldRetry_WhenExceptionIsTransient_ReturnsTrue()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler();

        // Act
        var result = handler.ShouldRetry(new TimeoutException(), 1);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void ShouldRetry_WhenExceptionIsNotTransient_ReturnsFalse()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler();

        // Act
        var result = handler.ShouldRetry(new InvalidOperationException(), 1);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_OnTransientError_RetriesAndSucceeds()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler(3, TimeSpan.FromMilliseconds(10));
        var callCount = 0;

        // Act
        var result = await handler.ExecuteWithRetryAsync(() =>
        {
            callCount++;

            if (callCount < 2)
                throw new TimeoutException();

            return Task.FromResult(42);
        });

        // Assert
        result.Should().Be(42);
        callCount.Should().Be(2);
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_OnNonTransientError_ThrowsImmediately()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler(3, TimeSpan.FromMilliseconds(10));
        var callCount = 0;

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            handler.ExecuteWithRetryAsync<int>(() =>
            {
                callCount++;
                throw new InvalidOperationException();
            }));

        callCount.Should().Be(1);
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_NonGeneric_OnTransientError_RetriesAndSucceeds()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler(3, TimeSpan.FromMilliseconds(10));
        var callCount = 0;

        // Act
        await handler.ExecuteWithRetryAsync(() =>
        {
            callCount++;

            if (callCount < 2)
                throw new TimeoutException();

            return Task.CompletedTask;
        });

        // Assert
        callCount.Should().Be(2);
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_NonGeneric_OnNonTransientError_ThrowsImmediately()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler(3, TimeSpan.FromMilliseconds(10));
        var callCount = 0;

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            handler.ExecuteWithRetryAsync(() =>
            {
                callCount++;
                throw new InvalidOperationException();
            }));

        callCount.Should().Be(1);
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_ExceedsMaxRetries_ThrowsOriginalException()
    {
        // Arrange
        var handler = new RedshiftExceptionHandler(2, TimeSpan.FromMilliseconds(10));
        var callCount = 0;

        // Act & Assert
        var exception = await Assert.ThrowsAsync<TimeoutException>(() =>
            handler.ExecuteWithRetryAsync<int>(() =>
            {
                callCount++;
                throw new TimeoutException();
            }));

        exception.Should().NotBeNull();
        callCount.Should().Be(2);
    }

    [Fact]
    public void Constructor_DefaultValues_AreCorrect()
    {
        // Arrange & Act
        var handler = new RedshiftExceptionHandler();

        // Assert
        handler.MaxRetryAttempts.Should().Be(3);
        handler.BaseDelay.Should().Be(TimeSpan.FromSeconds(2));
        handler.MaxDelay.Should().Be(TimeSpan.FromSeconds(60));
    }
}
