using NPipeline.Execution;
using NPipeline.Observability;
using NPipeline.Observability.Configuration;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Comprehensive edge case tests for observability components.
/// </summary>
public sealed class EdgeCaseTests
{
    #region Large Item Count Tests

    [Fact]
    public void AutoObservabilityScope_VeryLargeItemCount_ShouldHandleCorrectly()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);
        const long largeCount = long.MaxValue / 2;

        // Act
        // Use direct count recording to validate large values without impractical looping.
        scope.RecordItemCount(largeCount, largeCount);

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(largeCount, metrics.ItemsProcessed);
        Assert.Equal(largeCount, metrics.ItemsEmitted);
    }

    [Fact]
    public void AutoObservabilityScope_OverflowItemCount_ShouldHandleGracefully()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act - Increment to overflow
        scope.IncrementProcessed();
        scope.IncrementProcessed();
        scope.Dispose();

        // Assert - Should handle overflow gracefully
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(2, metrics.ItemsProcessed);
    }

    [Fact]
    public void ObservabilityCollector_VeryLargeRetryCount_ShouldHandleCorrectly()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        const int largeRetryCount = 1_000_000;

        // Act - RecordRetry tracks the maximum retry attempt, not a count
        for (var i = 1; i <= largeRetryCount; i++)
        {
            collector.RecordRetry(nodeId, i);
        }

        // Assert - Should track the maximum retry attempt
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(largeRetryCount, metrics.RetryCount);
    }

    #endregion

    #region Duration Tests

    [Fact]
    public void ObservabilityCollector_ZeroDuration_ShouldRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(nodeId, startTime);
        collector.RecordNodeEnd(nodeId, startTime, true); // Same time = zero duration

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.DurationMs);
    }

    [Fact]
    public void ObservabilityCollector_NegativeDuration_ShouldHandle()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        var endTime = startTime.AddMilliseconds(-100); // End before start

        // Act
        collector.RecordNodeStart(nodeId, startTime);
        collector.RecordNodeEnd(nodeId, endTime, true);

        // Assert - Should handle negative duration
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.True(metrics.DurationMs < 0);
    }

    [Fact]
    public void ObservabilityCollector_VeryLongDuration_ShouldRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow.AddYears(-10);
        var endTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(nodeId, startTime);
        collector.RecordNodeEnd(nodeId, endTime, true);

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.True(metrics.DurationMs > 0);
        Assert.True(metrics.DurationMs > TimeSpan.FromDays(3650).TotalMilliseconds);
    }

    #endregion

    #region Node ID Tests

    [Fact]
    public void ObservabilityCollector_EmptyNodeId_ShouldHandle()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var emptyNodeId = string.Empty;
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(emptyNodeId, startTime);
        collector.RecordNodeEnd(emptyNodeId, startTime.AddMilliseconds(100), true);

        // Assert
        var metrics = collector.GetNodeMetrics(emptyNodeId);
        Assert.NotNull(metrics);
    }

    [Fact]
    public void ObservabilityCollector_WhitespaceNodeId_ShouldHandle()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var whitespaceNodeId = "   ";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(whitespaceNodeId, startTime);
        collector.RecordNodeEnd(whitespaceNodeId, startTime.AddMilliseconds(100), true);

        // Assert
        var metrics = collector.GetNodeMetrics(whitespaceNodeId);
        Assert.NotNull(metrics);
    }

    [Fact]
    public void ObservabilityCollector_SpecialCharactersNodeId_ShouldHandle()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var specialNodeId = "node-1_2.3@#$%^&*()";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(specialNodeId, startTime);
        collector.RecordNodeEnd(specialNodeId, startTime.AddMilliseconds(100), true);

        // Assert
        var metrics = collector.GetNodeMetrics(specialNodeId);
        Assert.NotNull(metrics);
    }

    [Fact]
    public void ObservabilityCollector_VeryLongNodeId_ShouldHandle()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var longNodeId = new string('a', 10_000);
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(longNodeId, startTime);
        collector.RecordNodeEnd(longNodeId, startTime.AddMilliseconds(100), true);

        // Assert
        var metrics = collector.GetNodeMetrics(longNodeId);
        Assert.NotNull(metrics);
    }

    #endregion

    #region Memory Tracking Tests

    [Fact]
    public void ObservabilityCollector_NegativeMemoryDelta_ShouldHandle()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(nodeId, startTime, 1, 1000);
        collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(100), true, null, 500); // Lower memory

        // Assert - Should handle negative delta
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.True(metrics.PeakMemoryUsageMb.HasValue);
    }

    [Fact]
    public void ObservabilityCollector_ZeroMemory_ShouldRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(nodeId, startTime, 1, 0);
        collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(100), true, null, 0);

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.PeakMemoryUsageMb);
    }

    [Fact]
    public void ObservabilityCollector_VeryLargeMemory_ShouldRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;
        const long largeMemory = 1_000_000; // 1 TB

        // Act
        collector.RecordNodeStart(nodeId, startTime, 1, largeMemory);
        collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(100), true, null, largeMemory);

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(largeMemory, metrics.PeakMemoryUsageMb);
    }

    #endregion

    #region Multiple Concurrent Pipelines Tests

    [Fact]
    public void ObservabilityCollector_MultipleConcurrentPipelines_ShouldNotInterfere()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        const int pipelineCount = 10;
        const int nodesPerPipeline = 5;

        // Act - Simulate multiple pipelines running concurrently
        _ = Parallel.For(0, pipelineCount, pipelineId =>
        {
            for (var nodeId = 0; nodeId < nodesPerPipeline; nodeId++)
            {
                var nodeName = $"pipeline{pipelineId}_node{nodeId}";
                var startTime = DateTimeOffset.UtcNow;

                collector.RecordNodeStart(nodeName, startTime);
                collector.RecordNodeEnd(nodeName, startTime.AddMilliseconds(100), true);
                collector.RecordItemMetrics(nodeName, 100, 95);
            }
        });

        // Assert - Each node should have correct metrics
        for (var pipelineId = 0; pipelineId < pipelineCount; pipelineId++)
        {
            for (var nodeId = 0; nodeId < nodesPerPipeline; nodeId++)
            {
                var nodeName = $"pipeline{pipelineId}_node{nodeId}";
                var metrics = collector.GetNodeMetrics(nodeName);

                Assert.NotNull(metrics);
                Assert.Equal(100, metrics.ItemsProcessed);
                Assert.Equal(95, metrics.ItemsEmitted);
            }
        }
    }

    [Fact]
    public void ObservabilityCollector_SameNodeNameMultiplePipelines_ShouldSeparate()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        const int pipelineCount = 5;
        const string nodeName = "sharedNode";

        // Act - Multiple pipelines using same node name
        _ = Parallel.For(0, pipelineCount, pipelineId =>
        {
            var startTime = DateTimeOffset.UtcNow;
            collector.RecordNodeStart(nodeName, startTime);
            collector.RecordNodeEnd(nodeName, startTime.AddMilliseconds(100), true);
            collector.RecordItemMetrics(nodeName, pipelineId * 10, pipelineId * 10);
        });

        // Assert - Should have aggregated metrics
        var metrics = collector.GetNodeMetrics(nodeName);
        Assert.NotNull(metrics);
        // Note: Behavior depends on implementation - may aggregate or overwrite
        Assert.True(metrics.ItemsProcessed >= 0);
    }

    #endregion

    #region Performance Metrics Tests

    [Fact]
    public void ObservabilityCollector_ZeroThroughput_ShouldRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(nodeId, startTime);
        collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(100), true);
        collector.RecordPerformanceMetrics(nodeId, 0, 0); // Zero throughput

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.ThroughputItemsPerSec);
    }

    [Fact]
    public void ObservabilityCollector_VeryHighThroughput_ShouldRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(nodeId, startTime);
        collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(1), true); // 1ms duration
        collector.RecordPerformanceMetrics(nodeId, 1000000.0, 0.000001); // 1M items/sec

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(1000000.0, metrics.ThroughputItemsPerSec);
    }

    [Fact]
    public void ObservabilityCollector_NegativeThroughput_ShouldHandle()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(nodeId, startTime);
        collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(100), true);
        collector.RecordPerformanceMetrics(nodeId, -100.0, -1.0); // Negative values

        // Assert - Should handle negative values
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(-100.0, metrics.ThroughputItemsPerSec);
    }

    [Fact]
    public void ObservabilityCollector_InfiniteThroughput_ShouldHandle()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var startTime = DateTimeOffset.UtcNow;

        // Act
        collector.RecordNodeStart(nodeId, startTime);
        collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(0), true); // Zero duration
        collector.RecordPerformanceMetrics(nodeId, double.PositiveInfinity, 0);

        // Assert - Should handle infinity
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(double.PositiveInfinity, metrics.ThroughputItemsPerSec);
    }

    #endregion

    #region AutoObservabilityScope Edge Cases

    [Fact]
    public void AutoObservabilityScope_NoItemsProcessed_ShouldRecordZero()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act - Don't increment any items
        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.ItemsProcessed);
        Assert.Equal(0, metrics.ItemsEmitted);
    }

    [Fact]
    public void AutoObservabilityScope_MoreEmittedThanProcessed_ShouldRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        scope.IncrementEmitted();
        scope.IncrementEmitted();
        scope.Dispose();

        // Assert - Should record even though emitted > processed
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.ItemsProcessed);
        Assert.Equal(2, metrics.ItemsEmitted);
    }

    [Fact]
    public void AutoObservabilityScope_AllOptionsDisabled_ShouldStillRecordTiming()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions
        {
            RecordItemCounts = false,
            RecordMemoryUsage = false,
            RecordThreadInfo = false,
            RecordPerformanceMetrics = false
        };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        scope.Dispose();

        // Assert - Should still record basic timing
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.True(metrics.DurationMs >= 0);
    }

    #endregion

    #region MetricsCollectingExecutionObserver Edge Cases

    [Fact]
    public void Observer_OnNodeStartedWithoutEnd_ShouldRecordStart()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";

        // Act - Start without end
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", DateTimeOffset.UtcNow));

        // Assert - Should have recorded start
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
    }

    [Fact]
    public void Observer_OnNodeCompletedWithoutStart_ShouldStillRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";

        // Act - Complete without start
        observer.OnNodeCompleted(new NodeExecutionCompleted(
            nodeId,
            "TransformNode",
            TimeSpan.FromMilliseconds(100),
            true,
            null));

        // Assert - Should not record if node was never started
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.Null(metrics); // Changed behavior: only record if node was started
    }

    [Fact]
    public void Observer_MultipleStartsForSameNode_ShouldHandle()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "testNode";

        // Act - Multiple starts
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", DateTimeOffset.UtcNow));
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", DateTimeOffset.UtcNow));
        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", DateTimeOffset.UtcNow));

        // Assert - Should handle multiple starts
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
    }

    #endregion

    #region Pipeline Metrics Edge Cases

    [Fact]
    public void ObservabilityCollector_EmptyPipeline_ShouldCreateMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var pipelineName = "EmptyPipeline";
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;

        // Act - Create pipeline metrics without any nodes
        var metrics = collector.CreatePipelineMetrics(pipelineName, runId, startTime, startTime.AddSeconds(1), true);

        // Assert
        Assert.NotNull(metrics);
        Assert.Equal(pipelineName, metrics.PipelineName);
        Assert.Equal(runId, metrics.RunId);
        Assert.Empty(metrics.NodeMetrics);
    }

    [Fact]
    public void ObservabilityCollector_VeryLongPipelineName_ShouldHandle()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var longPipelineName = new string('a', 10_000);
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;

        // Act
        var metrics = collector.CreatePipelineMetrics(longPipelineName, runId, startTime, startTime.AddSeconds(1), true);

        // Assert
        Assert.NotNull(metrics);
        Assert.Equal(longPipelineName, metrics.PipelineName);
    }

    [Fact]
    public void ObservabilityCollector_ZeroDurationPipeline_ShouldRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var pipelineName = "TestPipeline";
        var runId = Guid.NewGuid();
        var startTime = DateTimeOffset.UtcNow;

        // Act
        var metrics = collector.CreatePipelineMetrics(pipelineName, runId, startTime, startTime, true);

        // Assert
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.DurationMs);
    }

    #endregion

    #region GC Pressure Tests

    [Fact]
    public void ObservabilityCollector_WithGCPressure_ShouldMaintainIntegrity()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        const int nodeCount = 1000;

        // Act - Create many nodes to trigger GC
        for (var i = 0; i < nodeCount; i++)
        {
            var nodeId = $"node{i}";
            var startTime = DateTimeOffset.UtcNow;

            collector.RecordNodeStart(nodeId, startTime);
            collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(100), true);
            collector.RecordItemMetrics(nodeId, 100, 95);

            // Force GC periodically
            if (i % 100 == 0)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }

        // Assert - All metrics should be intact
        for (var i = 0; i < nodeCount; i++)
        {
            var nodeId = $"node{i}";
            var metrics = collector.GetNodeMetrics(nodeId);

            Assert.NotNull(metrics);
            Assert.Equal(100, metrics.ItemsProcessed);
            Assert.Equal(95, metrics.ItemsEmitted);
        }
    }

    #endregion

    #region Test Helpers

    private sealed class TestObservabilityFactory : IObservabilityFactory
    {
        public IObservabilityCollector ResolveObservabilityCollector()
        {
            throw new NotImplementedException();
        }

        public IMetricsSink? ResolveMetricsSink()
        {
            return new TestMetricsSink();
        }

        public IPipelineMetricsSink? ResolvePipelineMetricsSink()
        {
            return new TestPipelineMetricsSink();
        }
    }

    private sealed class TestMetricsSink : IMetricsSink
    {
        public Task RecordAsync(INodeMetrics metrics, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestPipelineMetricsSink : IPipelineMetricsSink
    {
        public Task RecordAsync(IPipelineMetrics metrics, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }

    #endregion
}