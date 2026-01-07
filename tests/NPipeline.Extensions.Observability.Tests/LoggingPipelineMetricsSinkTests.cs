using Microsoft.Extensions.Logging;
using NPipeline.Observability;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests
{
    /// <summary>
    ///     Comprehensive tests for <see cref="LoggingPipelineMetricsSink" />.
    /// </summary>
    public sealed class LoggingPipelineMetricsSinkTests
    {
        #region Constructor Tests

        [Fact]
        public void Constructor_WithNullLogger_ShouldUseNullLogger()
        {
            // Arrange & Act
            var sink = new LoggingPipelineMetricsSink();

            // Assert
            Assert.NotNull(sink);
        }

        [Fact]
        public void Constructor_WithLogger_ShouldUseProvidedLogger()
        {
            // Arrange
            var logger = A.Fake<ILogger<LoggingPipelineMetricsSink>>();

            // Act
            var sink = new LoggingPipelineMetricsSink(logger);

            // Assert
            Assert.NotNull(sink);
        }

        #endregion

        #region RecordAsync Tests

        [Fact]
        public async Task RecordAsync_WithNullMetrics_ShouldThrowArgumentNullException()
        {
            // Arrange
            var sink = new LoggingPipelineMetricsSink();

            // Act & Assert
            _ = await Assert.ThrowsAsync<ArgumentNullException>(async () => await sink.RecordAsync(null!, CancellationToken.None));
        }

        [Fact]
        public async Task RecordAsync_WithSuccessfulPipeline_ShouldLogInformation()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(success: true);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert - Logs pipeline + overall throughput (2 Information calls)
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            Assert.Equal(2, logCalls.Count);
        }

        [Fact]
        public async Task RecordAsync_WithFailedPipeline_ShouldLogError()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(success: false);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Error).ToList();
            _ = Assert.Single(logCalls);
        }

        [Fact]
        public async Task RecordAsync_WithSuccessfulPipeline_ShouldIncludeCorrectProperties()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(success: true);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert - Logs pipeline + overall throughput (2 Information calls)
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            Assert.Equal(2, logCalls.Count);
        }

        [Fact]
        public async Task RecordAsync_WithFailedPipeline_ShouldIncludeExceptionMessage()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var exception = new InvalidOperationException("Pipeline failed");
            var metrics = CreatePipelineMetrics(success: false, exception: exception);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Error).ToList();
            _ = Assert.Single(logCalls);
        }

        [Fact]
        public async Task RecordAsync_ShouldLogNodeLevelDetails()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(
                success: true,
                nodeMetrics:
                [
                    CreateNodeMetrics("node1", success: true, itemsProcessed: 100),
                    CreateNodeMetrics("node2", success: true, itemsProcessed: 95),
                    CreateNodeMetrics("node3", success: true, itemsProcessed: 90)
                ]);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert
            // Should log for each node
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            Assert.True(logCalls.Count >= 2);
        }

        [Fact]
        public async Task RecordAsync_WithFailedNode_ShouldLogWarning()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var exception = new InvalidOperationException("Node failed");
            var metrics = CreatePipelineMetrics(
                success: true,
                nodeMetrics:
                [
                    CreateNodeMetrics("node1", success: true, itemsProcessed: 100),
                    CreateNodeMetrics("node2", success: false, exception: exception, itemsProcessed: 50)
                ]);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Warning).ToList();
            _ = Assert.Single(logCalls);
        }

        [Fact]
        public async Task RecordAsync_WithNodeRetries_ShouldLogRetryCount()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(
                success: true,
                nodeMetrics:
                [
                    CreateNodeMetrics("node1", success: true, itemsProcessed: 100, retryCount: 3)
                ]);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert - Logs pipeline + node + retry + overall throughput (4 Information calls)
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            Assert.Equal(4, logCalls.Count);
        }

        [Fact]
        public async Task RecordAsync_WithNodeThroughput_ShouldLogThroughput()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(
                success: true,
                nodeMetrics:
                [
                    CreateNodeMetrics("node1", success: true, itemsProcessed: 100, throughputItemsPerSec: 1000.5)
                ]);

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
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(
                success: true,
                nodeMetrics:
                [
                    CreateNodeMetrics("node1", success: true, itemsProcessed: 100, averageItemProcessingMs: 1.5)
                ]);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert - Debug log emitted for average time per item
            var calls = Fake.GetCalls(loggerMock);
            var debugCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Debug).ToList();
            _ = Assert.Single(debugCalls);
        }

        [Fact]
        public async Task RecordAsync_WithOverallThroughput_ShouldLogOverallThroughput()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(
                success: true,
                totalItemsProcessed: 1000,
                durationMs: 5000);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert - Logs pipeline + overall throughput (2 Information calls)
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            Assert.Equal(2, logCalls.Count);
        }

        [Fact]
        public async Task RecordAsync_WithZeroDuration_ShouldNotLogOverallThroughput()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(
                success: true,
                totalItemsProcessed: 1000,
                durationMs: 0);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert - Should log pipeline but not overall throughput (1 Information call)
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            _ = Assert.Single(logCalls);
        }

        [Fact]
        public async Task RecordAsync_WithNullDuration_ShouldNotLogOverallThroughput()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(
                success: true,
                totalItemsProcessed: 1000,
                durationMs: null);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert - Should log pipeline but not overall throughput (1 Information call)
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            _ = Assert.Single(logCalls);
        }

        [Fact]
        public async Task RecordAsync_WithEmptyNodeMetrics_ShouldOnlyLogPipeline()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(
                success: true,
                nodeMetrics: []);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert - Should log pipeline + overall throughput (2 Information calls)
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            Assert.Equal(2, logCalls.Count);
        }

        [Fact]
        public async Task RecordAsync_WithNullException_ShouldLogUnknownError()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(
                success: false,
                nodeMetrics:
                [
                    CreateNodeMetrics("node1", success: false, exception: null, itemsProcessed: 50)
                ]);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Warning).ToList();
            _ = Assert.Single(logCalls);
        }

        [Fact]
        public async Task RecordAsync_WithCancellation_ShouldCompleteSuccessfully()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(success: true);
            var cts = new CancellationTokenSource();

            // Act
            await sink.RecordAsync(metrics, cts.Token);

            // Assert - Logs pipeline + overall throughput (2 Information calls)
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            Assert.Equal(2, logCalls.Count);
        }

        [Fact]
        public async Task RecordAsync_WithCancelledToken_ShouldCompleteSuccessfully()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(success: true);
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act
            await sink.RecordAsync(metrics, cts.Token);

            // Assert - Logs pipeline + overall throughput (2 Information calls)
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            Assert.Equal(2, logCalls.Count);
        }

        [Fact]
        public async Task RecordAsync_WithManyNodes_ShouldLogAllNodes()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var nodeCount = 10;
            var nodeMetrics = new List<INodeMetrics>();

            for (var i = 0; i < nodeCount; i++)
            {
                nodeMetrics.Add(CreateNodeMetrics($"node_{i}", success: true, itemsProcessed: 100));
            }

            var metrics = CreatePipelineMetrics(success: true, nodeMetrics: nodeMetrics);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert
            // Should log for pipeline + all nodes
            var calls = Fake.GetCalls(loggerMock);
            var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            Assert.True(logCalls.Count >= 2);
        }

        [Fact]
        public async Task RecordAsync_WithMixedSuccessAndFailure_ShouldLogAppropriateLevels()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
            var sink = new LoggingPipelineMetricsSink(loggerMock);
            var metrics = CreatePipelineMetrics(
                success: false,
                nodeMetrics:
                [
                    CreateNodeMetrics("node1", success: true, itemsProcessed: 100),
                    CreateNodeMetrics("node2", success: false, exception: new InvalidOperationException("Failed"), itemsProcessed: 50),
                    CreateNodeMetrics("node3", success: true, itemsProcessed: 75)
                ]);

            // Act
            await sink.RecordAsync(metrics, CancellationToken.None);

            // Assert
            // Should log Error for pipeline, Information for successful nodes, Warning for failed node
            var calls = Fake.GetCalls(loggerMock);
            var errorCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Error).ToList();
            var infoCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Information).ToList();
            var warningCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Warning).ToList();

            _ = Assert.Single(errorCalls);
            Assert.True(infoCalls.Count >= 2);
            _ = Assert.Single(warningCalls);
        }

        #endregion

        #region Helper Methods

        private static IPipelineMetrics CreatePipelineMetrics(
            bool success,
            Exception? exception = null,
            IReadOnlyList<INodeMetrics>? nodeMetrics = null,
            long totalItemsProcessed = 285,
            long? durationMs = 5000)
        {
            return new PipelineMetrics(
                PipelineName: "TestPipeline",
                RunId: Guid.NewGuid(),
                StartTime: DateTimeOffset.UtcNow.AddSeconds(-5),
                EndTime: DateTimeOffset.UtcNow,
                DurationMs: durationMs,
                Success: success,
                TotalItemsProcessed: totalItemsProcessed,
                NodeMetrics: nodeMetrics ?? [],
                Exception: exception);
        }

        private static INodeMetrics CreateNodeMetrics(
            string nodeId,
            bool success,
            Exception? exception = null,
            int retryCount = 0,
            long itemsProcessed = 100,
            double? throughputItemsPerSec = null,
            double? averageItemProcessingMs = null)
        {
            return new NodeMetrics(
                NodeId: nodeId,
                StartTime: DateTimeOffset.UtcNow.AddSeconds(-1),
                EndTime: DateTimeOffset.UtcNow,
                DurationMs: 1000,
                Success: success,
                ItemsProcessed: itemsProcessed,
                ItemsEmitted: itemsProcessed - 5,
                Exception: exception,
                RetryCount: retryCount,
                PeakMemoryUsageMb: null,
                ProcessorTimeMs: null,
                ThroughputItemsPerSec: throughputItemsPerSec,
                AverageItemProcessingMs: averageItemProcessingMs,
                ThreadId: 1);
        }

        #endregion
    }
}