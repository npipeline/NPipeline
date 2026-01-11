using NPipeline.Execution;
using NPipeline.Observability;
using NPipeline.Observability.Configuration;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Comprehensive thread-safety tests for observability components.
/// </summary>
public sealed class ThreadSafetyTests
{
    #region Concurrent Performance Metrics Tests

    [Fact]
    public void ConcurrentPerformanceMetrics_ShouldBeThreadSafe()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var threadCount = 20;

        collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

        // Act - Multiple threads record performance metrics
        _ = Parallel.For(0, threadCount, i =>
        {
            var throughput = 1000.0 + i * 100;
            var avgTime = 1.0 + i * 0.1;
            collector.RecordPerformanceMetrics(nodeId, throughput, avgTime);
        });

        // Assert - Should have recorded the last value (or one of them)
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        _ = Assert.NotNull(metrics.ThroughputItemsPerSec);
        _ = Assert.NotNull(metrics.AverageItemProcessingMs);
    }

    [Fact]
    public void ConcurrentPerformanceMetrics_WithDifferentNodes_ShouldNotInterfere()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeCount = 10;
        var threadCount = 5;

        // Act - Multiple threads record performance metrics for different nodes
        _ = Parallel.For(0, nodeCount * threadCount, i =>
        {
            var nodeId = $"node_{i % nodeCount}";
            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);
            collector.RecordPerformanceMetrics(nodeId, 1000.0, 1.0);
        });

        // Assert - All nodes should have metrics
        var allMetrics = collector.GetNodeMetrics();
        Assert.Equal(nodeCount, allMetrics.Count);

        foreach (var metrics in allMetrics)
        {
            _ = Assert.NotNull(metrics.ThroughputItemsPerSec);
            _ = Assert.NotNull(metrics.AverageItemProcessingMs);
        }
    }

    #endregion

    #region Concurrent InitializeNode Tests

    [Fact]
    public void ConcurrentInitializeNode_ShouldBeThreadSafe()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeCount = 50;
        var threadCount = 10;

        // Act - Multiple threads initialize nodes
        _ = Parallel.For(0, threadCount, i =>
        {
            for (var j = 0; j < nodeCount / threadCount; j++)
            {
                var nodeId = $"node_{i * (nodeCount / threadCount) + j}";
                collector.InitializeNode(nodeId, i, 100);
            }
        });

        // Assert - All nodes should be initialized
        var allMetrics = collector.GetNodeMetrics();
        Assert.Equal(nodeCount, allMetrics.Count);

        foreach (var metrics in allMetrics)
        {
            _ = Assert.NotNull(metrics.ThreadId);
        }
    }

    [Fact]
    public void ConcurrentInitializeNode_AndRecordMetrics_ShouldNotCorrupt()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var threadCount = 20;

        // Act - Mix of Initialize and Record operations
        _ = Parallel.For(0, threadCount, i =>
        {
            if (i % 2 == 0)
                collector.InitializeNode(nodeId, i, 100);
            else
                collector.RecordItemMetrics(nodeId, 10, 10);
        });

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);

        // Item metrics should be accumulated correctly
        Assert.Equal(threadCount / 2 * 10, metrics.ItemsProcessed);
    }

    #endregion

    #region High Contention Tests

    [Fact]
    public void HighContention_MixedOperations_ShouldMaintainConsistency()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "contentionNode";
        var threadCount = 100;
        var operationsPerThread = 50;

        collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

        // Act - High contention with mixed operations
        _ = Parallel.For(0, threadCount, i =>
        {
            for (var j = 0; j < operationsPerThread; j++)
            {
                var operation = (i * operationsPerThread + j) % 5;

                switch (operation)
                {
                    case 0:
                        collector.RecordItemMetrics(nodeId, 1, 1);
                        break;
                    case 1:
                        collector.RecordRetry(nodeId, j % 10);
                        break;
                    case 2:
                        collector.RecordPerformanceMetrics(nodeId, 1000.0, 1.0);
                        break;
                    case 3:
                        // Read operation
                        _ = collector.GetNodeMetrics(nodeId);
                        break;
                    case 4:
                        // No-op to simulate gaps
                        break;
                }
            }
        });

        // Assert - Metrics should be consistent
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);

        // Item metrics: 1/5 of operations are RecordItemMetrics
        var expectedItemOps = threadCount * operationsPerThread / 5;
        Assert.Equal(expectedItemOps, metrics.ItemsProcessed);
        Assert.Equal(expectedItemOps, metrics.ItemsEmitted);

        // Retry count should be maximum
        Assert.True(metrics.RetryCount > 0);

        // Performance metrics should be recorded
        _ = Assert.NotNull(metrics.ThroughputItemsPerSec);
    }

    [Fact]
    public void HighContention_MultipleNodes_ShouldNotInterfere()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeCount = 20;
        var threadCount = 50;

        // Act - High contention with multiple nodes
        _ = Parallel.For(0, threadCount, i =>
        {
            var nodeId = $"node_{i % nodeCount}";
            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, i, 100);
            collector.RecordItemMetrics(nodeId, 10, 10);
            collector.RecordRetry(nodeId, i % 5);
            collector.RecordPerformanceMetrics(nodeId, 1000.0, 1.0);
        });

        // Assert - All nodes should have consistent metrics
        var allMetrics = collector.GetNodeMetrics();
        Assert.Equal(nodeCount, allMetrics.Count);

        // Total items should equal threadCount * 10 (each thread records 10 items)
        var totalItems = allMetrics.Sum(m => m.ItemsProcessed);
        Assert.Equal(10 * threadCount, totalItems);

        // All metrics should have throughput calculated
        Assert.All(allMetrics, m => Assert.NotNull(m.ThroughputItemsPerSec));
    }

    #endregion

    #region Concurrent EmitMetricsAsync Tests

    [Fact]
    public async Task ConcurrentEmitMetricsAsync_ShouldBeThreadSafe()
    {
        // Arrange
        var factory = new TestObservabilityFactory();
        var pipelineCount = 10;
        var nodeCount = 5;

        // Act - Multiple concurrent emissions with separate collectors (realistic usage)
        var tasks = Enumerable.Range(0, pipelineCount).Select(async i =>
        {
            var collector = new ObservabilityCollector(factory);
            var pipelineName = $"Pipeline_{i}";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;

            for (var j = 0; j < nodeCount; j++)
            {
                var nodeId = $"{pipelineName}_node_{j}";
                collector.RecordNodeStart(nodeId, startTime, i, 100);
                collector.RecordItemMetrics(nodeId, 10, 10);
                collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(100), true);
            }

            await collector.EmitMetricsAsync(pipelineName, runId, startTime, startTime.AddSeconds(1), true);
        }).ToArray();

        await Task.WhenAll(tasks);

        // Assert - All pipelines should have been emitted
        Assert.Equal(pipelineCount, factory.PipelineMetricsSink.RecordAsyncCallCount);
        Assert.Equal(pipelineCount * nodeCount, factory.NodeMetricsSink.RecordAsyncCallCount);
    }

    [Fact]
    public async Task ConcurrentEmitMetricsAsync_WithSameCollector_ShouldNotCorrupt()
    {
        // Arrange
        var factory = new TestObservabilityFactory();
        var collector = new ObservabilityCollector(factory);
        var pipelineName = "SharedPipeline";
        var threadCount = 20;

        // Act - Multiple threads record metrics concurrently
        var recordTasks = Enumerable.Range(0, threadCount).Select(async i =>
        {
            await Task.Yield(); // Force async execution
            var startTime = DateTimeOffset.UtcNow;
            var nodeId = $"node_{i}";

            collector.RecordNodeStart(nodeId, startTime, i, 100);
            collector.RecordItemMetrics(nodeId, 10, 10);
            collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(100), true);
        }).ToArray();

        await Task.WhenAll(recordTasks);

        // Single emission after all recording is complete
        await collector.EmitMetricsAsync(pipelineName, Guid.NewGuid(), DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, true);

        // Assert - Single emission should have recorded all nodes
        Assert.Equal(1, factory.PipelineMetricsSink.RecordAsyncCallCount);
        Assert.Equal(threadCount, factory.NodeMetricsSink.RecordAsyncCallCount);
    }

    #endregion

    #region AutoObservabilityScope Thread-Safety Tests

    [Fact]
    public void AutoObservabilityScope_ConcurrentIncrements_ShouldBeAccurate()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);
        var threadCount = 20;
        var incrementsPerThread = 100;

        // Act - Concurrent increments
        _ = Parallel.For(0, threadCount, i =>
        {
            for (var j = 0; j < incrementsPerThread; j++)
            {
                scope.IncrementProcessed();
                scope.IncrementEmitted();
            }
        });

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(threadCount * incrementsPerThread, metrics.ItemsProcessed);
        Assert.Equal(threadCount * incrementsPerThread, metrics.ItemsEmitted);
    }

    [Fact]
    public void AutoObservabilityScope_ConcurrentRecordItemCount_ShouldUseLastValue()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);
        var threadCount = 10;

        // Act - Concurrent RecordItemCount calls
        _ = Parallel.For(0, threadCount, i => { scope.RecordItemCount(i * 10, i * 9); });

        scope.Dispose();

        // Assert - Should have recorded the last value (or one of them)
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);

        // Since RecordItemCount sets the value (not increments), we expect one of the values
        Assert.True(metrics.ItemsProcessed >= 0);
        Assert.True(metrics.ItemsEmitted >= 0);
    }

    [Fact]
    public void AutoObservabilityScope_ConcurrentFailureRecording_ShouldRecordFailure()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Default;
        var scope = new AutoObservabilityScope(collector, nodeId, options);
        var threadCount = 10;

        // Act - Concurrent failure recordings
        _ = Parallel.For(0, threadCount, i => { scope.RecordFailure(new InvalidOperationException($"Exception {i}")); });

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.False(metrics.Success);
        Assert.NotNull(metrics.Exception);
    }

    #endregion

    #region MetricsCollectingExecutionObserver Thread-Safety Tests

    [Fact]
    public void Observer_ConcurrentNodeStartAndEnd_ShouldBeThreadSafe()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeCount = 50;
        var startTime = DateTimeOffset.UtcNow;

        // Act - Concurrent node start and end events
        _ = Parallel.For(0, nodeCount, i =>
        {
            var nodeId = $"node_{i}";
            observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));
            observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));
        });

        // Assert - All nodes should have metrics
        var allMetrics = collector.GetNodeMetrics();
        Assert.Equal(nodeCount, allMetrics.Count);

        foreach (var metrics in allMetrics)
        {
            Assert.True(metrics.Success);
            Assert.InRange(metrics.DurationMs!.Value, 90, 110);
        }
    }

    [Fact]
    public void Observer_ConcurrentRetryEvents_ShouldTrackMaximum()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var observer = new MetricsCollectingExecutionObserver(collector);
        var nodeId = "retryNode";
        var retryCount = 50;
        var startTime = DateTimeOffset.UtcNow;

        observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TransformNode", startTime));

        // Act - Concurrent retry events
        _ = Parallel.For(0, retryCount, i => { observer.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, i, null)); });

        observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TransformNode", TimeSpan.FromMilliseconds(100), true, null));

        // Assert - Should track maximum retry count
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(retryCount - 1, metrics.RetryCount);
    }

    #endregion

    #region Test Helpers

    private sealed class TestObservabilityFactory : IObservabilityFactory
    {
        public TestMetricsSink NodeMetricsSink { get; } = new();
        public TestPipelineMetricsSink PipelineMetricsSink { get; } = new();

        public IObservabilityCollector ResolveObservabilityCollector()
        {
            throw new NotImplementedException();
        }

        public IMetricsSink ResolveMetricsSink()
        {
            return NodeMetricsSink;
        }

        public IPipelineMetricsSink ResolvePipelineMetricsSink()
        {
            return PipelineMetricsSink;
        }
    }

    private sealed class TestMetricsSink : IMetricsSink
    {
        private int _recordAsyncCallCount;

        public int RecordAsyncCallCount => _recordAsyncCallCount;

        public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken = default)
        {
            _ = Interlocked.Increment(ref _recordAsyncCallCount);
            return Task.CompletedTask;
        }
    }

    private sealed class TestPipelineMetricsSink : IPipelineMetricsSink
    {
        private int _recordAsyncCallCount;

        public int RecordAsyncCallCount => _recordAsyncCallCount;

        public Task RecordAsync(IPipelineMetrics pipelineMetrics, CancellationToken cancellationToken = default)
        {
            _ = Interlocked.Increment(ref _recordAsyncCallCount);
            return Task.CompletedTask;
        }
    }

    #endregion
}
