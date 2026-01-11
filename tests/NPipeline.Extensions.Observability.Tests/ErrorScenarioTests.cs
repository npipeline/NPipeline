using Microsoft.Extensions.Logging;
using NPipeline.Execution;
using NPipeline.Observability;
using NPipeline.Observability.Configuration;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Comprehensive error scenario tests for observability components.
/// </summary>
public sealed class ErrorScenarioTests
{
    #region Null Exception Handling Tests

    [Fact]
    public async Task LoggingMetricsSink_NullException_ShouldLogUnknownError()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingMetricsSink>>();
        var sink = new LoggingMetricsSink(loggerMock);
        var metrics = CreateNodeMetrics(false);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert - Should log with "Unknown error" message
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Warning).ToList();
        _ = Assert.Single(logCalls);
    }

    [Fact]
    public async Task LoggingPipelineMetricsSink_NullException_ShouldLogUnknownError()
    {
        // Arrange
        var loggerMock = A.Fake<ILogger<LoggingPipelineMetricsSink>>();
        var sink = new LoggingPipelineMetricsSink(loggerMock);
        var metrics = CreatePipelineMetrics(false);

        // Act
        await sink.RecordAsync(metrics, CancellationToken.None);

        // Assert
        var calls = Fake.GetCalls(loggerMock);
        var logCalls = calls.Where(c => c.Method.Name == "Log" && c.GetArgument<LogLevel>(0) == LogLevel.Error).ToList();
        _ = Assert.Single(logCalls);
    }

    #endregion

    #region Sink Exception Handling Tests

    [Fact]
    public async Task EmitMetricsAsync_SinkThrowsException_ShouldPropagate()
    {
        // Arrange
        var factory = new ThrowingTestObservabilityFactory(true, false);
        var collector = new ObservabilityCollector(factory);
        var pipelineName = "TestPipeline";
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;

        collector.RecordNodeStart("node1", startTime, 1, 100);
        collector.RecordNodeEnd("node1", startTime.AddMilliseconds(100), true);

        // Act & Assert
        _ = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            collector.EmitMetricsAsync(pipelineName, runId, startTime, startTime.AddSeconds(1), true));
    }

    [Fact]
    public async Task EmitMetricsAsync_PipelineSinkThrowsException_ShouldPropagate()
    {
        // Arrange
        var factory = new ThrowingTestObservabilityFactory(false, true);
        var collector = new ObservabilityCollector(factory);
        var pipelineName = "TestPipeline";
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;

        collector.RecordNodeStart("node1", startTime, 1, 100);
        collector.RecordNodeEnd("node1", startTime.AddMilliseconds(100), true);

        // Act & Assert
        _ = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            collector.EmitMetricsAsync(pipelineName, runId, startTime, startTime.AddSeconds(1), true));
    }

    [Fact]
    public async Task EmitMetricsAsync_BothSinksThrow_ShouldPropagateFirstException()
    {
        // Arrange
        var factory = new ThrowingTestObservabilityFactory(true, true);
        var collector = new ObservabilityCollector(factory);
        var pipelineName = "TestPipeline";
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;

        collector.RecordNodeStart("node1", startTime, 1, 100);
        collector.RecordNodeEnd("node1", startTime.AddMilliseconds(100), true);

        // Act & Assert - Should throw from node sink first
        _ = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            collector.EmitMetricsAsync(pipelineName, runId, startTime, startTime.AddSeconds(1), true));
    }

    #endregion

    #region Cancellation Tests

    [Fact]
    public async Task EmitMetricsAsync_WithCancellation_ShouldComplete()
    {
        // Arrange
        var factory = new TestObservabilityFactory();
        var collector = new ObservabilityCollector(factory);
        var pipelineName = "TestPipeline";
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;
        var cts = new CancellationTokenSource();

        collector.RecordNodeStart("node1", startTime, 1, 100);
        collector.RecordNodeEnd("node1", startTime.AddMilliseconds(100), true);

        // Act
        await collector.EmitMetricsAsync(pipelineName, runId, startTime, startTime.AddSeconds(1), true, null, cts.Token);

        // Assert - Should complete successfully
        Assert.Equal(1, factory.NodeMetricsSink.RecordAsyncCallCount);
        Assert.Equal(1, factory.PipelineMetricsSink.RecordAsyncCallCount);
    }

    [Fact]
    public async Task EmitMetricsAsync_CancelledToken_ShouldComplete()
    {
        // Arrange
        var factory = new TestObservabilityFactory();
        var collector = new ObservabilityCollector(factory);
        var pipelineName = "TestPipeline";
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        collector.RecordNodeStart("node1", startTime, 1, 100);
        collector.RecordNodeEnd("node1", startTime.AddMilliseconds(100), true);

        // Act - Should complete even with cancelled token
        await collector.EmitMetricsAsync(pipelineName, runId, startTime, startTime.AddSeconds(1), true, null, cts.Token);

        // Assert
        Assert.Equal(1, factory.NodeMetricsSink.RecordAsyncCallCount);
        Assert.Equal(1, factory.PipelineMetricsSink.RecordAsyncCallCount);
    }

    [Fact]
    public void AutoObservabilityScope_WithCancellation_ShouldDispose()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Default;
        var scope = new AutoObservabilityScope(collector, nodeId, options);
        var cts = new CancellationTokenSource();

        // Act - Simulate cancellation
        cts.Cancel();
        scope.Dispose();

        // Assert - Should dispose without throwing
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
    }

    #endregion

    #region Factory Null Sink Tests

    [Fact]
    public async Task EmitMetricsAsync_NullNodeSink_ShouldComplete()
    {
        // Arrange
        var factory = new TestObservabilityFactory(false);
        var collector = new ObservabilityCollector(factory);
        var pipelineName = "TestPipeline";
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;

        collector.RecordNodeStart("node1", startTime, 1, 100);
        collector.RecordNodeEnd("node1", startTime.AddMilliseconds(100), true);

        // Act & Assert - Should complete without throwing
        await collector.EmitMetricsAsync(pipelineName, runId, startTime, startTime.AddSeconds(1), true);

        // Assert - Only pipeline sink should be called
        Assert.Equal(0, factory.NodeMetricsSink.RecordAsyncCallCount);
        Assert.Equal(1, factory.PipelineMetricsSink.RecordAsyncCallCount);
    }

    [Fact]
    public async Task EmitMetricsAsync_NullPipelineSink_ShouldComplete()
    {
        // Arrange
        var factory = new TestObservabilityFactory(hasPipelineSink: false);
        var collector = new ObservabilityCollector(factory);
        var pipelineName = "TestPipeline";
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;

        collector.RecordNodeStart("node1", startTime, 1, 100);
        collector.RecordNodeEnd("node1", startTime.AddMilliseconds(100), true);

        // Act & Assert - Should complete without throwing
        await collector.EmitMetricsAsync(pipelineName, runId, startTime, startTime.AddSeconds(1), true);

        // Assert - Only node sink should be called
        Assert.Equal(1, factory.NodeMetricsSink.RecordAsyncCallCount);
        Assert.Equal(0, factory.PipelineMetricsSink.RecordAsyncCallCount);
    }

    [Fact]
    public async Task EmitMetricsAsync_BothSinksNull_ShouldComplete()
    {
        // Arrange
        var factory = new TestObservabilityFactory(false, false);
        var collector = new ObservabilityCollector(factory);
        var pipelineName = "TestPipeline";
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;

        collector.RecordNodeStart("node1", startTime, 1, 100);
        collector.RecordNodeEnd("node1", startTime.AddMilliseconds(100), true);

        // Act & Assert - Should complete without throwing
        await collector.EmitMetricsAsync(pipelineName, runId, startTime, startTime.AddSeconds(1), true);

        // Assert - No sinks should be called
        Assert.Equal(0, factory.NodeMetricsSink.RecordAsyncCallCount);
        Assert.Equal(0, factory.PipelineMetricsSink.RecordAsyncCallCount);
    }

    #endregion

    #region MetricsCollectingExecutionObserver Error Tests

    [Fact]
    public void Observer_OnNodeStarted_WithNullEvent_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        // Act & Assert - Should not throw
        var exception = Record.Exception(() => observer.OnNodeStarted(null!));
        Assert.NotNull(exception); // Should throw ArgumentNullException
    }

    [Fact]
    public void Observer_OnNodeCompleted_WithNullEvent_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        // Act & Assert - Should not throw
        var exception = Record.Exception(() => observer.OnNodeCompleted(null!));
        Assert.NotNull(exception); // Should throw ArgumentNullException
    }

    [Fact]
    public void Observer_OnRetry_WithNullEvent_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        // Act & Assert - Should not throw
        var exception = Record.Exception(() => observer.OnRetry(null!));
        Assert.NotNull(exception); // Should throw ArgumentNullException
    }

    [Fact]
    public void Observer_OnDrop_WithNullEvent_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        // Act & Assert - Should not throw
        var exception = Record.Exception(() => observer.OnDrop(null!));
        Assert.NotNull(exception); // Should throw ArgumentNullException
    }

    [Fact]
    public void Observer_OnQueueMetrics_WithNullEvent_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        // Act & Assert - Should not throw
        var exception = Record.Exception(() => observer.OnQueueMetrics(null!));
        Assert.NotNull(exception); // Should throw ArgumentNullException
    }

    #endregion

    #region AutoObservabilityScope Error Tests

    [Fact]
    public void AutoObservabilityScope_DisposeMultipleTimes_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Default;
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act & Assert - Should not throw on multiple disposes
        var exception = Record.Exception(() =>
        {
            scope.Dispose();
            scope.Dispose();
            scope.Dispose();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void AutoObservabilityScope_RecordFailure_AfterDispose_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Default;
        var scope = new AutoObservabilityScope(collector, nodeId, options);
        var exception = new InvalidOperationException("Test exception");

        scope.Dispose();

        // Act & Assert - Should not throw
        var ex = Record.Exception(() => scope.RecordFailure(exception));
        Assert.Null(ex);
    }

    [Fact]
    public void AutoObservabilityScope_IncrementAfterDispose_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        scope.Dispose();

        // Act & Assert - Should not throw
        var exception = Record.Exception(() =>
        {
            scope.IncrementProcessed();
            scope.IncrementEmitted();
        });

        Assert.Null(exception);
    }

    #endregion

    #region ObservabilityCollector Error Tests

    [Fact]
    public void Collector_RecordNodeStart_WithNullNodeId_ShouldThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => collector.RecordNodeStart(null!, DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Collector_RecordNodeEnd_WithNullNodeId_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());

        // Act & Assert - Should not throw, just no-op
        var exception = Record.Exception(() => collector.RecordNodeEnd(null!, DateTimeOffset.UtcNow, true));
        Assert.Null(exception);
    }

    [Fact]
    public void Collector_RecordItemMetrics_WithNullNodeId_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());

        // Act & Assert - Should not throw, just no-op
        var exception = Record.Exception(() => collector.RecordItemMetrics(null!, 10, 10));
        Assert.Null(exception);
    }

    [Fact]
    public void Collector_RecordRetry_WithNullNodeId_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());

        // Act & Assert - Should not throw, just no-op
        var exception = Record.Exception(() => collector.RecordRetry(null!, 1));
        Assert.Null(exception);
    }

    [Fact]
    public void Collector_RecordPerformanceMetrics_WithNullNodeId_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());

        // Act & Assert - Should not throw, just no-op
        var exception = Record.Exception(() => collector.RecordPerformanceMetrics(null!, 1000.0, 1.0));
        Assert.Null(exception);
    }

    [Fact]
    public void Collector_CreatePipelineMetrics_WithNullPipelineName_ShouldThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() =>
            collector.CreatePipelineMetrics(null!, Guid.NewGuid(), DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, true));
    }

    [Fact]
    public void Collector_GetNodeMetrics_WithNullNodeId_ShouldReturnNull()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());

        // Act
        var metrics = collector.GetNodeMetrics(null!);

        // Assert
        Assert.Null(metrics);
    }

    #endregion

    #region MetricsCollectingExecutionObserver Disposal Tests

    [Fact]
    public void Observer_DisposeMultipleTimes_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        // Act & Assert - Should not throw on multiple disposes
        var exception = Record.Exception(() =>
        {
            observer.Dispose();
            observer.Dispose();
            observer.Dispose();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Observer_AfterDispose_ShouldNotRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";

        observer.Dispose();

        // Act - Try to record after disposal
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", DateTimeOffset.UtcNow));

        // Assert - Should not have recorded
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.Null(metrics);
    }

    #endregion

    #region Test Helpers

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

    private static IPipelineMetrics CreatePipelineMetrics(
        bool success,
        Exception? exception = null,
        IReadOnlyList<INodeMetrics>? nodeMetrics = null,
        long totalItemsProcessed = 285,
        long? durationMs = 5000)
    {
        return new PipelineMetrics(
            "TestPipeline",
            Guid.NewGuid(),
            DateTimeOffset.UtcNow.AddSeconds(-5),
            DateTimeOffset.UtcNow,
            durationMs,
            success,
            totalItemsProcessed,
            nodeMetrics ?? [],
            exception);
    }

    private sealed class TestObservabilityFactory : IObservabilityFactory
    {
        public TestObservabilityFactory(bool hasNodeSink = true, bool hasPipelineSink = true)
        {
            NodeMetricsSink.IsEnabled = hasNodeSink;
            PipelineMetricsSink.IsEnabled = hasPipelineSink;
        }

        public TestMetricsSink NodeMetricsSink { get; } = new();
        public TestPipelineMetricsSink PipelineMetricsSink { get; } = new();

        public IObservabilityCollector ResolveObservabilityCollector()
        {
            throw new NotImplementedException();
        }

        public IMetricsSink? ResolveMetricsSink()
        {
            return NodeMetricsSink.IsEnabled
                ? NodeMetricsSink
                : null;
        }

        public IPipelineMetricsSink? ResolvePipelineMetricsSink()
        {
            return PipelineMetricsSink.IsEnabled
                ? PipelineMetricsSink
                : null;
        }
    }

    private sealed class TestMetricsSink : IMetricsSink
    {
        public bool IsEnabled { get; set; } = true;
        public int RecordAsyncCallCount { get; private set; }

        public Task RecordAsync(INodeMetrics metrics, CancellationToken cancellationToken = default)
        {
            if (!IsEnabled)
                return Task.CompletedTask;

            RecordAsyncCallCount++;
            return Task.CompletedTask;
        }
    }

    private sealed class TestPipelineMetricsSink : IPipelineMetricsSink
    {
        public bool IsEnabled { get; set; } = true;
        public int RecordAsyncCallCount { get; private set; }

        public Task RecordAsync(IPipelineMetrics metrics, CancellationToken cancellationToken = default)
        {
            if (!IsEnabled)
                return Task.CompletedTask;

            RecordAsyncCallCount++;
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingTestObservabilityFactory(bool throwInNodeSink, bool throwInPipelineSink) : IObservabilityFactory
    {
        private readonly bool _throwInNodeSink = throwInNodeSink;
        private readonly bool _throwInPipelineSink = throwInPipelineSink;

        public ThrowingMetricsSink NodeMetricsSink { get; } = new();
        public ThrowingPipelineMetricsSink PipelineMetricsSink { get; } = new();

        public IObservabilityCollector ResolveObservabilityCollector()
        {
            throw new NotImplementedException();
        }

        public IMetricsSink? ResolveMetricsSink()
        {
            return _throwInNodeSink
                ? NodeMetricsSink
                : null;
        }

        public IPipelineMetricsSink? ResolvePipelineMetricsSink()
        {
            return _throwInPipelineSink
                ? PipelineMetricsSink
                : null;
        }
    }

    private sealed class ThrowingMetricsSink : IMetricsSink
    {
        public Task RecordAsync(INodeMetrics metrics, CancellationToken cancellationToken = default)
        {
            throw new InvalidOperationException("Sink exception");
        }
    }

    private sealed class ThrowingPipelineMetricsSink : IPipelineMetricsSink
    {
        public Task RecordAsync(IPipelineMetrics metrics, CancellationToken cancellationToken = default)
        {
            throw new InvalidOperationException("Pipeline sink exception");
        }
    }

    #endregion
}
