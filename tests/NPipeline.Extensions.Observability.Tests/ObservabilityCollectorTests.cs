using System.Collections.Concurrent;
using NPipeline.Observability;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests
{
    /// <summary>
    ///     Comprehensive tests for <see cref="ObservabilityCollector" />.
    /// </summary>
    public sealed class ObservabilityCollectorTests
    {
        private static readonly TestObservabilityFactory s_defaultFactory = new();

        #region Basic Node Recording Tests

        [Fact]
        public void RecordNodeStart_ShouldCreateNewNodeMetrics()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            var timestamp = DateTimeOffset.UtcNow;
            var threadId = 1;
            var initialMemoryMb = 100L;

            // Act
            collector.RecordNodeStart(nodeId, timestamp, threadId, initialMemoryMb);

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(nodeId, metrics.NodeId);
            Assert.Equal(timestamp, metrics.StartTime);
            Assert.Equal(threadId, metrics.ThreadId);
            Assert.True(metrics.Success); // Default success is true
            Assert.Equal(0, metrics.ItemsProcessed);
            Assert.Equal(0, metrics.ItemsEmitted);
            Assert.Equal(0, metrics.RetryCount);
        }

        [Fact]
        public void RecordNodeEnd_ShouldUpdateExistingNodeMetrics()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            var startTime = DateTimeOffset.UtcNow.AddSeconds(-1);
            var endTime = DateTimeOffset.UtcNow;
            var exception = new InvalidOperationException("Test exception");
            var peakMemoryMb = 200L;
            var processorTimeMs = 150L;

            collector.RecordNodeStart(nodeId, startTime, 1, 100);

            // Act
            collector.RecordNodeEnd(nodeId, endTime, false, exception, peakMemoryMb, processorTimeMs);

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(endTime, metrics.EndTime);
            Assert.False(metrics.Success);
            Assert.Equal(exception, metrics.Exception);
            Assert.InRange(metrics.DurationMs!.Value, 900, 1100); // Approximately 1 second
            Assert.Equal(peakMemoryMb, metrics.PeakMemoryUsageMb);
            Assert.Equal(processorTimeMs, metrics.ProcessorTimeMs);
        }

        [Fact]
        public void RecordNodeEnd_WithoutStart_ShouldNotThrow()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "nonExistentNode";

            // Act & Assert
            var exception = Record.Exception(() => collector.RecordNodeEnd(nodeId, DateTimeOffset.UtcNow, true));
            Assert.Null(exception);
        }

        [Fact]
        public void RecordItemMetrics_ShouldAccumulateItems()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            collector.RecordItemMetrics(nodeId, 50, 45);
            collector.RecordItemMetrics(nodeId, 30, 28);

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(80, metrics.ItemsProcessed);
            Assert.Equal(73, metrics.ItemsEmitted);
        }

        [Fact]
        public void RecordRetry_ShouldTrackMaximumRetryCount()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            collector.RecordRetry(nodeId, 1, "First retry");
            collector.RecordRetry(nodeId, 3, "Third retry");
            collector.RecordRetry(nodeId, 2, "Second retry");

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(3, metrics.RetryCount); // Should track maximum
        }

        [Fact]
        public void RecordPerformanceMetrics_ShouldStoreThroughput()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            collector.RecordPerformanceMetrics(nodeId, 1000.5, 1.0);

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(1000.5, metrics.ThroughputItemsPerSec);
            Assert.Equal(1.0, metrics.AverageItemProcessingMs);
        }

        #endregion

        #region Thread-Safety Tests

        [Fact]
        public void ConcurrentRetryOperations_ShouldBeThreadSafe()
        {
            // Arrange
            var collector = new ObservabilityCollector(new TestObservabilityFactory());
            var nodeId = "testNode";
            var retryCount = 50;

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            _ = Parallel.For(0, retryCount, i =>
            {
                collector.RecordRetry(nodeId, i, $"Retry {i}");
            });

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            // Should track maximum retry count
            Assert.Equal(retryCount - 1, metrics.RetryCount);
        }

        [Fact]
        public void ConcurrentRetryOperations_WithSameRetryCount_ShouldNotLoseUpdates()
        {
            // Arrange
            var collector = new ObservabilityCollector(new TestObservabilityFactory());
            var nodeId = "testNode";
            var threadCount = 20;
            var retryCount = 5;

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            // All threads try to set the same retry count
            _ = Parallel.For(0, threadCount, i =>
            {
                collector.RecordRetry(nodeId, retryCount, $"Thread {i}");
            });

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            // Should have recorded the retry count (not lost due to race condition)
            Assert.Equal(retryCount, metrics.RetryCount);
        }

        [Fact]
        public void ConcurrentRetryOperations_WithIncreasingRetryCount_ShouldTrackMaximum()
        {
            // Arrange
            var collector = new ObservabilityCollector(new TestObservabilityFactory());
            var nodeId = "testNode";
            var threadCount = 10;

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            // Each thread tries to set a different retry count
            _ = Parallel.For(0, threadCount, i =>
            {
                var currentRetryCount = i + 1; // 1, 2, 3, ..., 10
                collector.RecordRetry(nodeId, currentRetryCount, $"Retry {currentRetryCount}");
            });

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            // Should track the maximum retry count (10)
            Assert.Equal(threadCount, metrics.RetryCount);
        }

        [Fact]
        public void SequentialRetryOperations_ShouldTrackCorrectly()
        {
            // Arrange
            var collector = new ObservabilityCollector(new TestObservabilityFactory());
            var nodeId = "testNode";

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            collector.RecordRetry(nodeId, 1, "First retry");
            collector.RecordRetry(nodeId, 3, "Third retry");
            collector.RecordRetry(nodeId, 2, "Second retry");
            collector.RecordRetry(nodeId, 5, "Fifth retry");

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            // Should track maximum (5)
            Assert.Equal(5, metrics.RetryCount);
        }

        [Fact]
        public void RetryOperations_WithDecreasingRetryCount_ShouldTrackMaximum()
        {
            // Arrange
            var collector = new ObservabilityCollector(new TestObservabilityFactory());
            var nodeId = "testNode";

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            // Record retries in decreasing order
            collector.RecordRetry(nodeId, 10, "Retry 10");
            collector.RecordRetry(nodeId, 8, "Retry 8");
            collector.RecordRetry(nodeId, 6, "Retry 6");
            collector.RecordRetry(nodeId, 4, "Retry 4");
            collector.RecordRetry(nodeId, 2, "Retry 2");

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            // Should track maximum (10)
            Assert.Equal(10, metrics.RetryCount);
        }

        [Fact]
        public void ConcurrentRetryOperations_WithDifferentNodes_ShouldNotInterfere()
        {
            // Arrange
            var collector = new ObservabilityCollector(new TestObservabilityFactory());
            var nodeCount = 5;
            var retryCount = 10;

            // Act
            _ = Parallel.For(0, nodeCount, i =>
            {
                var nodeId = $"node_{i}";
                collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);
                _ = Parallel.For(0, retryCount, j =>
                {
                    collector.RecordRetry(nodeId, j, $"Retry {j}");
                });
            });

            // Assert
            for (var i = 0; i < nodeCount; i++)
            {
                var nodeId = $"node_{i}";
                var metrics = collector.GetNodeMetrics(nodeId);
                Assert.NotNull(metrics);
                // Each node should have tracked the maximum retry count
                Assert.Equal(retryCount - 1, metrics.RetryCount);
            }
        }

        [Fact]
        public void HighContentionRetryOperations_ShouldNotLoseUpdates()
        {
            // Arrange
            var collector = new ObservabilityCollector(new TestObservabilityFactory());
            var nodeId = "testNode";
            var threadCount = 100;

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            // High contention scenario with many threads
            _ = Parallel.For(0, threadCount, i =>
            {
                collector.RecordRetry(nodeId, i, $"Retry {i}");
            });

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            // Should track the maximum retry count even under high contention
            Assert.Equal(threadCount - 1, metrics.RetryCount);
        }

        [Fact]
        public void ConcurrentNodeOperations_ShouldBeThreadSafe()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeCount = 100;
            var operationsPerNode = 10;
            var exceptions = new ConcurrentBag<Exception>();

            // Act
            _ = Parallel.For(0, nodeCount, i =>
            {
                try
                {
                    var nodeId = $"node_{i}";
                    var startTime = DateTimeOffset.UtcNow;

                    collector.RecordNodeStart(nodeId, startTime, i, 100);

                    for (var j = 0; j < operationsPerNode; j++)
                    {
                        collector.RecordItemMetrics(nodeId, 10, 10);
                    }

                    collector.RecordRetry(nodeId, i % 5, $"Retry {i}");
                    collector.RecordPerformanceMetrics(nodeId, 1000.0 + i, 1.0);

                    var endTime = startTime.AddMilliseconds(100);
                    collector.RecordNodeEnd(nodeId, endTime, success: true, null, 200, 150);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            });

            // Assert
            Assert.Empty(exceptions);
            var allMetrics = collector.GetNodeMetrics();
            Assert.Equal(nodeCount, allMetrics.Count);

            foreach (var metrics in allMetrics)
            {
                Assert.Equal(100, metrics.ItemsProcessed); // 10 operations * 10 items
                Assert.Equal(100, metrics.ItemsEmitted);
                Assert.InRange(metrics.DurationMs!.Value, 90, 110);
            }
        }

        [Fact]
        public void ConcurrentItemMetricsAccumulation_ShouldBeAccurate()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "sharedNode";
            var threadCount = 20;
            var itemsPerThread = 100;

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            _ = Parallel.For(0, threadCount, i =>
            {
                collector.RecordItemMetrics(nodeId, itemsPerThread, itemsPerThread);
            });

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(threadCount * itemsPerThread, metrics.ItemsProcessed);
            Assert.Equal(threadCount * itemsPerThread, metrics.ItemsEmitted);
        }

        [Fact]
        public void ConcurrentRetryTracking_ShouldTrackMaximum()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "retryNode";
            var threadCount = 10;

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act
            _ = Parallel.For(0, threadCount, i =>
            {
                collector.RecordRetry(nodeId, i, $"Thread {i}");
            });

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(threadCount - 1, metrics.RetryCount); // Maximum retry count
        }

        #endregion

        #region Thread-Safe Retry Counting Tests

        [Fact]
        public void RecordRetry_ConcurrentCalls_MaintainsCorrectMax()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "TestNode";

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act - Simulate concurrent retries with different counts
            _ = Parallel.For(0, 100, i =>
            {
                collector.RecordRetry(nodeId, i + 1, $"Retry {i + 1}");
            });

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(100, metrics.RetryCount); // Should be the maximum retry count
        }

        [Fact]
        public void RecordRetry_ConcurrentCallsWithSameCount_MaintainsCorrectValue()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "TestNode";
            var retryCount = 5;

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act - Multiple threads recording the same retry count
            _ = Parallel.For(0, 50, i =>
            {
                collector.RecordRetry(nodeId, retryCount, $"Thread {i}");
            });

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(retryCount, metrics.RetryCount);
        }

        [Fact]
        public void RecordRetry_ConcurrentCallsWithVaryingCounts_MaintainsMaximum()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "TestNode";

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act - Simulate retries with varying counts in random order
            var retryCounts = new[] { 1, 3, 2, 5, 4, 10, 8, 6, 4, 2 };
            _ = Parallel.ForEach(retryCounts, retryCount =>
            {
                collector.RecordRetry(nodeId, retryCount, $"Retry attempt {retryCount}");
            });

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(10, metrics.RetryCount); // Should be the maximum value
        }

        [Fact]
        public void RecordRetry_ConcurrentCallsWithDecreasingCounts_MaintainsInitialMaximum()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "TestNode";

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act - Record high retry count first, then lower ones
            collector.RecordRetry(nodeId, 10, "Initial high count");

            _ = Parallel.For(0, 50, i =>
            {
                collector.RecordRetry(nodeId, i + 1, $"Retry {i + 1}");
            });

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(50, metrics.RetryCount); // The parallel loop records 1-50, so max is 50
        }

        [Fact]
        public async Task RecordRetry_HighContention_NoLostUpdates()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "TestNode";

            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);

            // Act - Simulate high contention with many concurrent updates
            var tasks = Enumerable.Range(0, 200).Select(i =>
                Task.Run(() => collector.RecordRetry(nodeId, i + 1, $"Retry {i + 1}"))
            ).ToArray();

            await Task.WhenAll(tasks);

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(200, metrics.RetryCount); // Should capture the maximum without lost updates
        }

        [Fact]
        public void RecordRetry_MultipleNodes_ConcurrentCallsMaintainCorrectValues()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeIds = new[] { "Node1", "Node2", "Node3" };

            // Act - Record retries for multiple nodes concurrently
            _ = Parallel.ForEach(nodeIds, nodeId =>
            {
                collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);
                _ = Parallel.For(0, 50, i =>
                {
                    collector.RecordRetry(nodeId, i + 1, $"Retry {i + 1}");
                });
            });

            // Assert - Each node should have its own maximum
            foreach (var nodeId in nodeIds)
            {
                var metrics = collector.GetNodeMetrics(nodeId);
                Assert.NotNull(metrics);
                Assert.Equal(50, metrics.RetryCount);
            }
        }

        #endregion

        #region Metrics Retrieval Tests

        [Fact]
        public void GetNodeMetrics_WithExistingNode_ShouldReturnMetrics()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);
            collector.RecordNodeEnd(nodeId, DateTimeOffset.UtcNow.AddMilliseconds(100), true);

            // Act
            var metrics = collector.GetNodeMetrics(nodeId);

            // Assert
            Assert.NotNull(metrics);
            Assert.Equal(nodeId, metrics.NodeId);
        }

        [Fact]
        public void GetNodeMetrics_WithNonExistentNode_ShouldReturnNull()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);

            // Act
            var metrics = collector.GetNodeMetrics("nonExistentNode");

            // Assert
            Assert.Null(metrics);
        }

        [Fact]
        public void GetNodeMetrics_ShouldReturnAllNodeMetrics()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeIds = new[] { "node1", "node2", "node3" };

            foreach (var nodeId in nodeIds)
            {
                collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow, 1, 100);
                collector.RecordNodeEnd(nodeId, DateTimeOffset.UtcNow.AddMilliseconds(100), true);
            }

            // Act
            var allMetrics = collector.GetNodeMetrics();

            // Assert
            Assert.Equal(nodeIds.Length, allMetrics.Count);
            Assert.All(allMetrics, m => Assert.Contains(m.NodeId, nodeIds));
        }

        [Fact]
        public void GetNodeMetrics_WithEmptyCollector_ShouldReturnEmptyList()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);

            // Act
            var allMetrics = collector.GetNodeMetrics();

            // Assert
            Assert.Empty(allMetrics);
        }

        #endregion

        #region Pipeline Metrics Tests

        [Fact]
        public void CreatePipelineMetrics_ShouldAggregateNodeMetrics()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var pipelineName = "TestPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddSeconds(5);

            collector.RecordNodeStart("node1", startTime, 1, 100);
            collector.RecordItemMetrics("node1", 100, 95);
            collector.RecordNodeEnd("node1", startTime.AddSeconds(2), true);

            collector.RecordNodeStart("node2", startTime.AddSeconds(2), 2, 100);
            collector.RecordItemMetrics("node2", 95, 90);
            collector.RecordNodeEnd("node2", endTime, true);

            // Act
            var pipelineMetrics = collector.CreatePipelineMetrics(pipelineName, runId, startTime, endTime, true);

            // Assert
            Assert.Equal(pipelineName, pipelineMetrics.PipelineName);
            Assert.Equal(runId, pipelineMetrics.RunId);
            Assert.Equal(startTime, pipelineMetrics.StartTime);
            Assert.Equal(endTime, pipelineMetrics.EndTime);
            Assert.Equal(5000, pipelineMetrics.DurationMs);
            Assert.True(pipelineMetrics.Success);
            Assert.Equal(195, pipelineMetrics.TotalItemsProcessed); // 100 + 95
            Assert.Equal(2, pipelineMetrics.NodeMetrics.Count);
        }

        [Fact]
        public void CreatePipelineMetrics_WithFailure_ShouldIncludeException()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var pipelineName = "FailedPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddSeconds(1);
            var exception = new InvalidOperationException("Pipeline failed");

            collector.RecordNodeStart("node1", startTime, 1, 100);
            collector.RecordNodeEnd("node1", endTime, false, exception);

            // Act
            var pipelineMetrics = collector.CreatePipelineMetrics(pipelineName, runId, startTime, endTime, false, exception);

            // Assert
            Assert.False(pipelineMetrics.Success);
            Assert.Equal(exception, pipelineMetrics.Exception);
        }

        [Fact]
        public void CreatePipelineMetrics_WithoutEndTime_ShouldHaveNullDuration()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var pipelineName = "RunningPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;

            // Act
            var pipelineMetrics = collector.CreatePipelineMetrics(pipelineName, runId, startTime, null, true);

            // Assert
            Assert.Null(pipelineMetrics.EndTime);
            Assert.Null(pipelineMetrics.DurationMs);
        }

        [Fact]
        public void CreatePipelineMetrics_WithEmptyCollector_ShouldReturnValidMetrics()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var pipelineName = "EmptyPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddSeconds(1);

            // Act
            var pipelineMetrics = collector.CreatePipelineMetrics(pipelineName, runId, startTime, endTime, true);

            // Assert
            Assert.Equal(pipelineName, pipelineMetrics.PipelineName);
            Assert.Equal(runId, pipelineMetrics.RunId);
            Assert.Equal(0, pipelineMetrics.TotalItemsProcessed);
            Assert.Empty(pipelineMetrics.NodeMetrics);
        }

        #endregion

        #region EmitMetricsAsync Tests

        [Fact]
        public async Task EmitMetricsAsync_ShouldCreatePipelineMetrics()
        {
            // Arrange
            var factory = new TestObservabilityFactory();
            var collector = new ObservabilityCollector(factory);
            var pipelineName = "TestPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddSeconds(5);

            collector.RecordNodeStart("node1", startTime, 1, 100);
            collector.RecordItemMetrics("node1", 100, 95);
            collector.RecordNodeEnd("node1", startTime.AddSeconds(2), true);

            // Act
            await collector.EmitMetricsAsync(pipelineName, runId, startTime, endTime, true);

            // Assert
            Assert.Equal(1, factory.NodeMetricsSink.RecordAsyncCallCount);
            Assert.Equal(1, factory.PipelineMetricsSink.RecordAsyncCallCount);
        }

        [Fact]
        public async Task EmitMetricsAsync_WithFailure_ShouldIncludeException()
        {
            // Arrange
            var factory = new TestObservabilityFactory();
            var collector = new ObservabilityCollector(factory);
            var pipelineName = "FailedPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddSeconds(1);
            var exception = new InvalidOperationException("Pipeline failed");

            collector.RecordNodeStart("node1", startTime, 1, 100);
            collector.RecordNodeEnd("node1", endTime, false, exception);

            // Act
            await collector.EmitMetricsAsync(pipelineName, runId, startTime, endTime, false, exception);

            // Assert
            Assert.Equal(1, factory.NodeMetricsSink.RecordAsyncCallCount);
            Assert.Equal(1, factory.PipelineMetricsSink.RecordAsyncCallCount);
            Assert.Equal(exception, factory.PipelineMetricsSink.LastException);
        }

        [Fact]
        public async Task EmitMetricsAsync_WithMultipleNodes_ShouldRecordAllNodeMetrics()
        {
            // Arrange
            var factory = new TestObservabilityFactory();
            var collector = new ObservabilityCollector(factory);
            var pipelineName = "MultiNodePipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddSeconds(5);

            collector.RecordNodeStart("node1", startTime, 1, 100);
            collector.RecordItemMetrics("node1", 100, 95);
            collector.RecordNodeEnd("node1", startTime.AddSeconds(2), true);

            collector.RecordNodeStart("node2", startTime.AddSeconds(2), 2, 100);
            collector.RecordItemMetrics("node2", 95, 90);
            collector.RecordNodeEnd("node2", endTime, true);

            // Act
            await collector.EmitMetricsAsync(pipelineName, runId, startTime, endTime, true);

            // Assert
            Assert.Equal(2, factory.NodeMetricsSink.RecordAsyncCallCount);
            Assert.Equal(1, factory.PipelineMetricsSink.RecordAsyncCallCount);
        }

        [Fact]
        public async Task EmitMetricsAsync_WithNoSinks_ShouldCompleteSuccessfully()
        {
            // Arrange
            var factory = new TestObservabilityFactory(hasNodeMetricsSink: false, hasPipelineMetricsSink: false);
            var collector = new ObservabilityCollector(factory);
            var pipelineName = "NoSinksPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddSeconds(1);

            collector.RecordNodeStart("node1", startTime, 1, 100);
            collector.RecordNodeEnd("node1", endTime, true);

            // Act & Assert
            var exception = await Record.ExceptionAsync(() =>
                collector.EmitMetricsAsync(pipelineName, runId, startTime, endTime, true));
            Assert.Null(exception);
        }

        #endregion

        #region Edge Cases

        [Fact]
        public void MultipleCallsToRecordNodeStart_ShouldUpdateStartTime()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            var firstStart = DateTimeOffset.UtcNow;
            var secondStart = firstStart.AddSeconds(1);

            // Act
            collector.RecordNodeStart(nodeId, firstStart, 1, 100);
            collector.RecordNodeStart(nodeId, secondStart, 2, 150);

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(secondStart, metrics.StartTime); // Should use latest start time
            Assert.Equal(2, metrics.ThreadId);
        }

        [Fact]
        public void RecordNodeEnd_BeforeStart_ShouldNotCalculateDuration()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            var endTime = DateTimeOffset.UtcNow;

            // Act
            collector.RecordNodeEnd(nodeId, endTime, true);

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.Null(metrics); // Node wasn't started, so no metrics exist
        }

        [Fact]
        public void RecordItemMetrics_BeforeStart_ShouldNotThrow()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";

            // Act & Assert
            var exception = Record.Exception(() => collector.RecordItemMetrics(nodeId, 10, 10));
            Assert.Null(exception);
        }

        [Fact]
        public void RecordRetry_BeforeStart_ShouldNotThrow()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";

            // Act & Assert
            var exception = Record.Exception(() => collector.RecordRetry(nodeId, 1, "Retry"));
            Assert.Null(exception);
        }

        [Fact]
        public void RecordPerformanceMetrics_BeforeStart_ShouldNotThrow()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";

            // Act & Assert
            var exception = Record.Exception(() => collector.RecordPerformanceMetrics(nodeId, 1000.0, 1.0));
            Assert.Null(exception);
        }

        [Fact]
        public void NullParameters_ShouldBeHandled()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            var timestamp = DateTimeOffset.UtcNow;

            // Act & Assert
            collector.RecordNodeStart(nodeId, timestamp, null, null);
            collector.RecordNodeEnd(nodeId, timestamp.AddMilliseconds(100), true, null, null, null);
            collector.RecordRetry(nodeId, 1, null);

            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Null(metrics.ThreadId);
            Assert.Null(metrics.PeakMemoryUsageMb);
            Assert.Null(metrics.ProcessorTimeMs);
        }

        [Fact]
        public void LargeNumberOfNodes_ShouldHandleEfficiently()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeCount = 1000;
            var startTime = DateTimeOffset.UtcNow;

            // Act
            for (var i = 0; i < nodeCount; i++)
            {
                var nodeId = $"node_{i}";
                collector.RecordNodeStart(nodeId, startTime.AddMilliseconds(i), 1, 100);
                collector.RecordItemMetrics(nodeId, 10, 10);
                collector.RecordNodeEnd(nodeId, startTime.AddMilliseconds(i + 100), true);
            }

            // Assert
            var allMetrics = collector.GetNodeMetrics();
            Assert.Equal(nodeCount, allMetrics.Count);
        }

        [Fact]
        public void ZeroItemsProcessed_ShouldStillCalculateThroughput()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddSeconds(1);

            collector.RecordNodeStart(nodeId, startTime, 1, 100);
            collector.RecordItemMetrics(nodeId, 0, 0);
            collector.RecordNodeEnd(nodeId, endTime, true);

            // Act
            collector.RecordPerformanceMetrics(nodeId, 0.0, 0.0);

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(0, metrics.ItemsProcessed);
            Assert.Equal(0.0, metrics.ThroughputItemsPerSec);
            Assert.Equal(0.0, metrics.AverageItemProcessingMs);
        }

        #endregion

        #region Performance Metrics Calculation Tests

        [Fact]
        public void DurationCalculation_ShouldBeAccurate()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            var startTime = DateTimeOffset.UtcNow;
            var expectedDuration = 500L; // 500ms
            var endTime = startTime.AddMilliseconds(expectedDuration);

            // Act
            collector.RecordNodeStart(nodeId, startTime, 1, 100);
            collector.RecordNodeEnd(nodeId, endTime, true);

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(expectedDuration, metrics.DurationMs);
        }

        [Fact]
        public void ThroughputCalculation_ShouldBeCorrect()
        {
            // Arrange
            var collector = new ObservabilityCollector(s_defaultFactory);
            var nodeId = "testNode";
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddSeconds(1);
            var itemsProcessed = 1000L;
            var expectedThroughput = 1000.0; // 1000 items per second

            collector.RecordNodeStart(nodeId, startTime, 1, 100);
            collector.RecordItemMetrics(nodeId, itemsProcessed, itemsProcessed);
            collector.RecordNodeEnd(nodeId, endTime, true);

            // Act
            collector.RecordPerformanceMetrics(nodeId, expectedThroughput, 1.0);

            // Assert
            var metrics = collector.GetNodeMetrics(nodeId);
            Assert.NotNull(metrics);
            Assert.Equal(expectedThroughput, metrics.ThroughputItemsPerSec);
            Assert.Equal(1.0, metrics.AverageItemProcessingMs);
        }

        #endregion

        #region Test Helpers

        private sealed class TestObservabilityFactory : IObservabilityFactory
        {
            public TestMetricsSink NodeMetricsSink { get; } = new();
            public TestPipelineMetricsSink PipelineMetricsSink { get; } = new();

            public TestObservabilityFactory(bool hasNodeMetricsSink = true, bool hasPipelineMetricsSink = true)
            {
                NodeMetricsSink.IsEnabled = hasNodeMetricsSink;
                PipelineMetricsSink.IsEnabled = hasPipelineMetricsSink;
            }

            public IObservabilityCollector ResolveObservabilityCollector()
            {
                throw new NotImplementedException();
            }

            public IMetricsSink? ResolveMetricsSink()
            {
                return NodeMetricsSink.IsEnabled ? NodeMetricsSink : null;
            }

            public IPipelineMetricsSink? ResolvePipelineMetricsSink()
            {
                return PipelineMetricsSink.IsEnabled ? PipelineMetricsSink : null;
            }
        }

        private sealed class TestMetricsSink : IMetricsSink
        {
            public bool IsEnabled { get; set; } = true;
            public int RecordAsyncCallCount { get; private set; }
            public INodeMetrics? LastNodeMetrics { get; private set; }

            public Task RecordAsync(INodeMetrics metrics, CancellationToken cancellationToken = default)
            {
                if (!IsEnabled)
                {
                    return Task.CompletedTask;
                }

                RecordAsyncCallCount++;
                LastNodeMetrics = metrics;
                return Task.CompletedTask;
            }
        }

        private sealed class TestPipelineMetricsSink : IPipelineMetricsSink
        {
            public bool IsEnabled { get; set; } = true;
            public int RecordAsyncCallCount { get; private set; }
            public IPipelineMetrics? LastPipelineMetrics { get; private set; }
            public Exception? LastException { get; private set; }

            public Task RecordAsync(IPipelineMetrics metrics, CancellationToken cancellationToken = default)
            {
                if (!IsEnabled)
                {
                    return Task.CompletedTask;
                }

                RecordAsyncCallCount++;
                LastPipelineMetrics = metrics;
                LastException = metrics.Exception;
                return Task.CompletedTask;
            }
        }

        #endregion
    }
}