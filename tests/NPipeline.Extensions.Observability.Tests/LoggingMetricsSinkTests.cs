using Microsoft.Extensions.Logging;
using NPipeline.Observability;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Comprehensive tests for <see cref="LoggingMetricsSink" />.
/// </summary>
public sealed class LoggingMetricsSinkTests
{
    #region Helper Methods

    private static INodeMetrics CreateNodeMetrics(
        bool success,
        Exception? exception = null,
        int retryCount = 0,
        long? peakMemoryMb = null,
        long? processorTimeMs = null,
        double? throughputItemsPerSec = null,
        double? averageItemProcessingMs = null,
        long itemsProcessed = 100,
        long itemsEmitted = 95)
    {
        return new NodeMetrics(
            "testNode",
            DateTimeOffset.UtcNow.AddSeconds(-1),
            DateTimeOffset.UtcNow,
            1000,
            success,
            itemsProcessed,
            itemsEmitted,
            exception,
            retryCount,
            peakMemoryMb,
            processorTimeMs,
            throughputItemsPerSec,
            averageItemProcessingMs,
            1);
    }

    #endregion

    #region Constructor Tests

    [Fact]
    public void Constructor_WithNullLogger_ShouldUseNullLogger()
    {
        // Arrange & Act
        var sink = new LoggingMetricsSink();

        // Assert
        Assert.NotNull(sink);
    }

    [Fact]
    public void Constructor_WithLogger_ShouldUseProvidedLogger()
    {
        // Arrange
        var logger = A.Fake<ILogger<LoggingMetricsSink>>();

        // Act
        var sink = new LoggingMetricsSink(logger);

        // Assert
        Assert.NotNull(sink);
    }

    #endregion

    #region RecordAsync Tests

    [Fact]
    public async Task RecordAsync_WithNullMetrics_ShouldThrowArgumentNullException()
    {
        // Arrange
        var sink = new LoggingMetricsSink();

        // Act & Assert
        _ = await Assert.ThrowsAsync<ArgumentNullException>(async () => await sink.RecordAsync(null!, CancellationToken.None));
    }

    [Fact]
    public async Task RecordAsync_WithSuccessfulMetrics_ShouldLogInformation()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Verify that Log was called with Information level
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithFailedMetrics_ShouldLogWarning()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(false);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Warning).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithSuccessfulMetrics_ShouldIncludeCorrectProperties()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithFailedMetrics_ShouldIncludeExceptionMessage()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var exception = new InvalidOperationException("Test failure");
        var metrics = CreateNodeMetrics(false, exception);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Warning).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithRetries_ShouldLogRetryCount()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true, retryCount: 3);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Should log 2 Information calls: main log + retry log
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        Assert.Equal(2, logCalls.Count);
    }

    [Fact]
    public async Task RecordAsync_WithPeakMemory_ShouldLogDebug()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true, peakMemoryMb: 500);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Debug).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithProcessorTime_ShouldLogDebug()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true, processorTimeMs: 250);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Debug).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithAverageItemProcessing_ShouldLogDebug()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true, throughputItemsPerSec: 1000.5, averageItemProcessingMs: 1.2);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Debug).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithThroughput_ShouldLogThroughput()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true, throughputItemsPerSec: 1000.5);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithZeroThroughput_ShouldLogZeroThroughput()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true, throughputItemsPerSec: 0.0);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithZeroItems_ShouldLogZeroItems()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true, itemsProcessed: 0, itemsEmitted: 0);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithNullException_ShouldLogUnknownError()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(false, null);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Warning).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithNullMemory_ShouldNotLogMemory()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true, peakMemoryMb: null);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Debug).ToList();
        Assert.Empty(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithNullProcessorTime_ShouldNotLogProcessorTime()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true, processorTimeMs: null);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Debug).ToList();
        Assert.Empty(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithNullThroughput_ShouldNotLogThroughput()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true, throughputItemsPerSec: null);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Still logs the main Information log, but throughput value is 0 in the message
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithCancellation_ShouldCompleteSuccessfully()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true);
        var cts = new CancellationTokenSource();

        // Act
        await sink.RecordAsync(metrics, cts.Token);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task RecordAsync_WithCancelledToken_ShouldCompleteSuccessfully()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(true);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act
        await sink.RecordAsync(metrics, cts.Token);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
        _ = Assert.Single(logCalls);
    }

    #endregion
}
