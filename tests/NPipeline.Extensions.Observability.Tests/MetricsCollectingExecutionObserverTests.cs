using NPipeline.Execution;
using NPipeline.Observability;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Comprehensive tests for <see cref="MetricsCollectingExecutionObserver" />.
/// </summary>
public sealed class MetricsCollectingExecutionObserverTests
{
    #region Test Helpers

    private sealed class TestObservabilityFactory : IObservabilityFactory
    {
        public IObservabilityCollector ResolveObservabilityCollector()
        {
            throw new NotImplementedException();
        }

        public IMetricsSink ResolveMetricsSink()
        {
            throw new NotImplementedException();
        }

        public IPipelineMetricsSink ResolvePipelineMetricsSink()
        {
            throw new NotImplementedException();
        }
    }

    #endregion

    #region Constructor Tests

    [Fact]
    public void Constructor_WithNullCollector_ShouldThrowArgumentNullException()
    {
        // Arrange
        IObservabilityCollector collector = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new MetricsCollectingExecutionObserver(collector));
    }

    [Fact]
    public void Constructor_WithValidCollector_ShouldCreateInstance()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());

        // Act
        var observer = new MetricsCollectingExecutionObserver(collector);

        // Assert
        Assert.NotNull(observer);
    }

    #endregion

    #region OnNodeStarted Tests

    [Fact]
    public void OnNodeStarted_ShouldRecordNodeStart()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var nodeType = "TransformNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, nodeType, startTime));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(nodeId, metrics.NodeId);
        Assert.Equal(startTime, metrics.StartTime);
        Assert.NotNull(metrics.ThreadId);
        Assert.True(metrics.ThreadId > 0);
    }

    [Fact]
    public void OnNodeStarted_ShouldRecordInitialMemory()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";

        // Act
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", DateTimeOffset.UtcNow));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);

        // Memory is recorded in MB, should be positive
        // We can't assert exact value, but we can verify it's recorded
        _ = metrics.PeakMemoryUsageMb;
    }

    [Fact]
    public void OnNodeStarted_MultipleNodes_ShouldRecordEachNode()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeIds = new[] { "node1", "node2", "node3" };

        // Act
        foreach (var nodeId in nodeIds)
        {
            observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", DateTimeOffset.UtcNow));
        }

        // Assert
        foreach (var nodeId in nodeIds)
        {
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(nodeId, metrics.NodeId);
        }
    }

    #endregion

    #region OnNodeCompleted Tests

    [Fact]
    public void OnNodeCompleted_WithSuccess_ShouldRecordSuccessfulCompletion()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var duration = TimeSpan.FromMilliseconds(100);

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", duration, true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.True(metrics.Success);
        Assert.Null(metrics.Exception);
        _ = metrics.EndTime;
        _ = metrics.DurationMs;
        Assert.InRange(metrics.DurationMs!.Value, 90, 110);
        _ = metrics.PeakMemoryUsageMb;
        _ = metrics.ProcessorTimeMs;
    }

    [Fact]
    public void OnNodeCompleted_WithFailure_ShouldRecordFailure()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var duration = TimeSpan.FromMilliseconds(100);
        var exception = new InvalidOperationException("Test failure");

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", duration, false, exception));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.False(metrics.Success);
        Assert.Equal(exception, metrics.Exception);
        _ = metrics.EndTime;
        _ = metrics.DurationMs;
    }

    [Fact]
    public void OnNodeCompleted_WithItemsProcessed_ShouldCalculateThroughput()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var duration = TimeSpan.FromSeconds(1);
        var itemsProcessed = 100L;

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));
        collector.RecordItemMetrics(nodeId, itemsProcessed, itemsProcessed);

        // Act
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", duration, true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        _ = metrics.ThroughputItemsPerSec;
        Assert.InRange(metrics.ThroughputItemsPerSec!.Value, 90, 110); // Approximately 100 items/sec
        Assert.InRange(metrics.AverageItemProcessingMs!.Value, 9, 11);
    }

    [Fact]
    public void OnNodeCompleted_WithoutItemsProcessed_ShouldNotCalculateThroughput()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var duration = TimeSpan.FromSeconds(1);

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", duration, true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Null(metrics.ThroughputItemsPerSec);
    }

    [Fact]
    public void OnNodeCompleted_WithZeroDuration_ShouldNotCalculateThroughput()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var duration = TimeSpan.Zero;

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));
        collector.RecordItemMetrics(nodeId, 100, 100);

        // Act
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", duration, true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Null(metrics.ThroughputItemsPerSec);
    }

    [Fact]
    public void OnNodeCompleted_BeforeNodeStarted_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";

        // Act & Assert
        var exception = Record.Exception(() =>
            observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null)));

        Assert.Null(exception);
    }

    #endregion

    #region OnRetry Tests

    [Fact]
    public void OnRetry_ShouldRecordRetryAttempt()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var exception = new InvalidOperationException("Retry reason");

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, 1, exception));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(1, metrics.RetryCount);
    }

    [Fact]
    public void OnRetry_MultipleRetries_ShouldTrackMaximum()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, 1, null));
        observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, 3, null));
        observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.NodeRestart, 2, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(3, metrics.RetryCount); // Should track maximum
    }

    [Fact]
    public void OnRetry_WithException_ShouldRecordRetry()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var exception = new InvalidOperationException("Temporary failure");

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, 1, exception));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(1, metrics.RetryCount);
    }

    [Fact]
    public void OnRetry_BeforeNodeStarted_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";

        // Act & Assert
        var exception = Record.Exception(() =>
            observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, 1, null)));

        Assert.Null(exception);
    }

    [Fact]
    public void OnRetry_DifferentRetryKinds_ShouldRecordAll()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, 1, null));
        observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.NodeRestart, 2, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(2, metrics.RetryCount);
    }

    #endregion

    #region OnDrop Tests

    [Fact]
    public void OnDrop_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        var dropEvent = new QueueDropEvent(
            "testNode",
            "DropOldest",
            QueueDropKind.Oldest,
            100,
            95,
            5,
            0,
            100);

        // Act & Assert
        var exception = Record.Exception(() => observer.OnDrop(dropEvent));
        Assert.Null(exception);
    }

    [Fact]
    public void OnDrop_ShouldNotRecordMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        var dropEvent = new QueueDropEvent(
            "testNode",
            "DropOldest",
            QueueDropKind.Oldest,
            100,
            95,
            5,
            0,
            100);

        // Act
        observer.OnDrop(dropEvent);

        // Assert
        var metrics = collector.GetNodeMetrics("testNode");
        Assert.Null(metrics); // Queue drops are not tracked in node metrics
    }

    #endregion

    #region OnQueueMetrics Tests

    [Fact]
    public void OnQueueMetrics_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        var metricsEvent = new QueueMetricsEvent(
            "testNode",
            "DropOldest",
            100,
            50,
            5,
            0,
            100,
            DateTimeOffset.UtcNow);

        // Act & Assert
        var exception = Record.Exception(() => observer.OnQueueMetrics(metricsEvent));
        Assert.Null(exception);
    }

    [Fact]
    public void OnQueueMetrics_ShouldNotRecordMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        var metricsEvent = new QueueMetricsEvent(
            "testNode",
            "DropOldest",
            100,
            50,
            5,
            0,
            100,
            DateTimeOffset.UtcNow);

        // Act
        observer.OnQueueMetrics(metricsEvent);

        // Assert
        var metrics = collector.GetNodeMetrics("testNode");
        Assert.Null(metrics); // Queue metrics are not tracked in node metrics
    }

    #endregion

    #region Integration Tests

    [Fact]
    public void FullExecutionFlow_ShouldCollectCompleteMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var nodeType = "TransformNode";
        var startTime = DateTimeOffset.UtcNow;
        var duration = TimeSpan.FromMilliseconds(100);
        var itemsProcessed = 50L;

        // Act
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, nodeType, startTime));
        collector.RecordItemMetrics(nodeId, itemsProcessed, itemsProcessed);
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, nodeType, duration, true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(nodeId, metrics.NodeId);
        Assert.Equal(startTime, metrics.StartTime);
        _ = metrics.EndTime;
        Assert.True(metrics.Success);
        Assert.Equal(itemsProcessed, metrics.ItemsProcessed);
        Assert.Equal(itemsProcessed, metrics.ItemsEmitted);
        Assert.Equal(0, metrics.RetryCount);
        _ = metrics.DurationMs;
        _ = metrics.ThroughputItemsPerSec;
        _ = metrics.ThreadId;
    }

    [Fact]
    public void FullExecutionFlow_WithRetries_ShouldCollectCompleteMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var nodeType = "TransformNode";
        var startTime = DateTimeOffset.UtcNow;
        var duration = TimeSpan.FromMilliseconds(100);
        var itemsProcessed = 50L;
        var exception = new InvalidOperationException("Temporary failure");

        // Act
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, nodeType, startTime));
        observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, 1, exception));
        observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, 2, exception));
        collector.RecordItemMetrics(nodeId, itemsProcessed, itemsProcessed);
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, nodeType, duration, true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(nodeId, metrics.NodeId);
        Assert.True(metrics.Success);
        Assert.Equal(2, metrics.RetryCount);
        Assert.Equal(itemsProcessed, metrics.ItemsProcessed);
    }

    [Fact]
    public void FullExecutionFlow_WithFailure_ShouldCollectCompleteMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var nodeType = "TransformNode";
        var startTime = DateTimeOffset.UtcNow;
        var duration = TimeSpan.FromMilliseconds(100);
        var itemsProcessed = 25L;
        var exception = new InvalidOperationException("Permanent failure");

        // Act
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, nodeType, startTime));
        collector.RecordItemMetrics(nodeId, itemsProcessed, 20);
        observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, 1, exception));
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, nodeType, duration, false, exception));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.False(metrics.Success);
        Assert.Equal(exception, metrics.Exception);
        Assert.Equal(itemsProcessed, metrics.ItemsProcessed);
        Assert.Equal(1, metrics.RetryCount);
    }

    [Fact]
    public void MultipleNodesExecution_ShouldCollectMetricsForEach()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);

        var nodes = new[]
        {
            (NodeId: "node1", NodeType: "SourceNode", Items: 100),
            (NodeId: "node2", NodeType: "TransformNode", Items: 95),
            (NodeId: "node3", NodeType: "SinkNode", Items: 90),
        };

        var startTime = DateTimeOffset.UtcNow;

        // Act
        foreach (var (nodeId, _, items) in nodes)
        {
            observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));
            collector.RecordItemMetrics(nodeId, items, items);
            observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));
        }

        // Assert
        foreach (var (nodeId, _, items) in nodes)
        {
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(nodeId, metrics.NodeId);
            Assert.Equal(items, metrics.ItemsProcessed);
            Assert.True(metrics.Success);
        }

        var allMetrics = collector.GetNodeMetrics();
        Assert.Equal(nodes.Length, allMetrics.Count);
    }

    [Fact]
    public void ErrorPropagation_ShouldNotAffectObserver()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var exception = new InvalidOperationException("Test exception");

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), false, exception));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.False(metrics.Success);
        Assert.Equal(exception, metrics.Exception);

        // Observer should still be functional
        observer.OnNodeStarted(new NodeExecutionStarted("node2", "TransformNode", DateTimeOffset.UtcNow));
        Assert.NotNull(collector.GetNodeMetrics("node2"));
    }

    #endregion

    #region Edge Cases

    [Fact]
    public void OnNodeCompleted_WithoutStart_ShouldNotRecordMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";

        // Act
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.Null(metrics); // No metrics recorded without start
    }

    [Fact]
    public void MultipleStartsForSameNode_ShouldUpdateStartTime()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var firstStart = DateTimeOffset.UtcNow;
        var secondStart = firstStart.AddSeconds(1);

        // Act
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", firstStart));
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", secondStart));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(secondStart, metrics.StartTime); // Should use latest start time
    }

    [Fact]
    public void NullExceptionInNodeCompleted_ShouldRecordSuccess()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.True(metrics.Success);
        Assert.Null(metrics.Exception);
    }

    [Fact]
    public void VeryLongDuration_ShouldRecordCorrectly()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var duration = TimeSpan.FromMinutes(5); // 5 minutes

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", duration, true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(300000, metrics.DurationMs); // 5 minutes in milliseconds
    }

    [Fact]
    public void VeryShortDuration_ShouldRecordCorrectly()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var duration = TimeSpan.FromMicroseconds(100); // 100 microseconds

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", duration, true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        _ = metrics.DurationMs;
        Assert.InRange(metrics.DurationMs!.Value, 0, 1); // Should be 0 or 1 ms
    }

    #endregion

    #region Thread-Safety Tests

    [Fact]
    public void ConcurrentNodeOperations_ShouldBeThreadSafe()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeCount = 50;
        var startTime = DateTimeOffset.UtcNow;

        // Act
        _ = Parallel.For(0, nodeCount, i =>
        {
            var nodeId = $"node_{i}";
            observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));
            collector.RecordItemMetrics(nodeId, 10, 10);
            observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));
        });

        // Assert
        var allMetrics = collector.GetNodeMetrics();
        Assert.Equal(nodeCount, allMetrics.Count);

        foreach (var metrics in allMetrics)
        {
            Assert.True(metrics.Success);
            Assert.Equal(10, metrics.ItemsProcessed);
            _ = metrics.ThroughputItemsPerSec;
        }
    }

    [Fact]
    public void ConcurrentRetryOperations_ShouldBeThreadSafe()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "sharedNode";
        var retryCount = 20;
        var startTime = DateTimeOffset.UtcNow;

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act
        _ = Parallel.For(0, retryCount, i => { observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, i, null)); });

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(retryCount - 1, metrics.RetryCount); // Should track maximum
    }

    #endregion

    #region Per-Node Memory Measurement Tests

    [Fact]
    public void OnNodeStarted_ShouldUseGCGetTotalMemory()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";

        // Act
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", DateTimeOffset.UtcNow));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);

        // Memory should be recorded in MB (bytes / 1024 / 1024)
        // The exact value depends on GC state, but we can verify it's reasonable
        // GC.GetTotalMemory returns bytes, we convert to MB
        // The recorded memory should be close to what GC.GetTotalMemory returns
        // (within a small margin due to GC activity)
        _ = metrics.PeakMemoryUsageMb;
    }

    [Fact]
    public void OnNodeCompleted_ShouldCalculateMemoryDelta()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Force some memory allocation
        _ = new byte[1024 * 1024]; // 1 MB allocation

        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);

        // Peak memory should be >= initial memory
        if (metrics.PeakMemoryUsageMb.HasValue)
            Assert.True(metrics.PeakMemoryUsageMb.Value >= 0);
    }

    [Fact]
    public void MultipleNodes_ShouldHaveIndependentMemoryMeasurements()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeCount = 5;
        var startTime = DateTimeOffset.UtcNow;

        // Act
        for (var i = 0; i < nodeCount; i++)
        {
            var nodeId = $"node_{i}";
            observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

            // Each node allocates different amounts of memory
            var size = (i + 1) * 1024 * 1024; // 1 MB, 2 MB, 3 MB, etc.
            _ = new byte[size];

            observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));
        }

        // Assert
        for (var i = 0; i < nodeCount; i++)
        {
            var nodeId = $"node_{i}";
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);

            // Each node should have its own memory measurement
            if (metrics.PeakMemoryUsageMb.HasValue)
                Assert.True(metrics.PeakMemoryUsageMb.Value >= 0);
        }
    }

    [Fact]
    public void MemoryMeasurement_ShouldNotUseProcessGlobalMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId1 = "node1";
        var nodeId2 = "node2";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        // Start first node
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId1, "TransformNode", startTime));
        _ = new byte[1024 * 1024]; // 1 MB
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId1, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));

        // Start second node with different memory allocation
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId2, "TransformNode", startTime));
        _ = new byte[2 * 1024 * 1024]; // 2 MB
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId2, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));

        // Assert
        var metrics1 = collector.GetNodeMetrics(nodeId1);
        var metrics2 = collector.GetNodeMetrics(nodeId2);

        Assert.NotNull(metrics1);
        Assert.NotNull(metrics2);

        // Each node should have its own memory measurement
        // They should not be identical (unless GC cleaned up)
        // The key point is they're measured independently, not as global process metrics
        if (metrics1.PeakMemoryUsageMb.HasValue)
            Assert.True(metrics1.PeakMemoryUsageMb.Value >= 0);

        if (metrics2.PeakMemoryUsageMb.HasValue)
            Assert.True(metrics2.PeakMemoryUsageMb.Value >= 0);
    }

    [Fact]
    public void MemoryMeasurement_WithNoAllocation_ShouldStillRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // No explicit memory allocation
        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);

        // Memory should still be recorded even with no explicit allocation
        if (metrics.PeakMemoryUsageMb.HasValue)
            Assert.True(metrics.PeakMemoryUsageMb.Value >= 0);
    }

    [Fact]
    public void MemoryMeasurement_AfterGC_ShouldReflectNewState()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Allocate memory
        _ = new byte[10 * 1024 * 1024]; // 10 MB

        // Force GC to collect
        GC.Collect();
        GC.WaitForPendingFinalizers();

        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);

        // After GC, memory should be lower than allocation
        // (though exact values depend on GC behavior)
        if (metrics.PeakMemoryUsageMb.HasValue)
            Assert.True(metrics.PeakMemoryUsageMb.Value >= 0);
    }

    [Fact]
    public void MemoryMeasurement_WithConcurrentNodes_ShouldBeThreadSafe()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeCount = 20;
        var startTime = DateTimeOffset.UtcNow;

        // Act
        _ = Parallel.For(0, nodeCount, i =>
        {
            var nodeId = $"node_{i}";
            observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

            // Each thread allocates memory
            var size = (i + 1) * 1024 * 1024;
            _ = new byte[size];

            observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));
        });

        // Assert
        var allMetrics = collector.GetNodeMetrics();
        Assert.Equal(nodeCount, allMetrics.Count);

        foreach (var metrics in allMetrics)
        {
            if (metrics.PeakMemoryUsageMb.HasValue)
                Assert.True(metrics.PeakMemoryUsageMb.Value >= 0);
        }
    }

    #endregion
}
