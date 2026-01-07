using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NPipeline.Execution;
using NPipeline.Observability;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests
{
    /// <summary>
    ///     Comprehensive integration tests for the NPipeline Observability extension.
    /// </summary>
    public sealed class IntegrationTests
    {
        #region End-to-End Pipeline Execution Tests

        [Fact]
        public async Task EndToEndPipelineExecution_WithMetricsCollection_ShouldCollectAllMetrics()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            var pipelineName = "TestPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;

            // Act - Simulate pipeline execution
            collector.RecordNodeStart("node1", startTime);
            await Task.Delay(10);
            collector.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("node1", 100, 95);

            collector.RecordNodeStart("node2", DateTimeOffset.UtcNow);
            await Task.Delay(15);
            collector.RecordNodeEnd("node2", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("node2", 95, 90);
            collector.RecordPerformanceMetrics("node2", 6333.33, 0.158);

            var endTime = DateTimeOffset.UtcNow;
            var pipelineMetrics = collector.CreatePipelineMetrics(pipelineName, runId, startTime, endTime, true);

            // Assert
            Assert.NotNull(pipelineMetrics);
            Assert.Equal(pipelineName, pipelineMetrics.PipelineName);
            Assert.Equal(runId, pipelineMetrics.RunId);
            Assert.True(pipelineMetrics.Success);
            Assert.Equal(2, pipelineMetrics.NodeMetrics.Count);
            Assert.Equal(195, pipelineMetrics.TotalItemsProcessed);

            var node1Metrics = collector.GetNodeMetrics("node1");
            Assert.NotNull(node1Metrics);
            Assert.Equal("node1", node1Metrics.NodeId);
            Assert.True(node1Metrics.Success);
            Assert.Equal(100, node1Metrics.ItemsProcessed);
            Assert.Equal(95, node1Metrics.ItemsEmitted);

            var node2Metrics = collector.GetNodeMetrics("node2");
            Assert.NotNull(node2Metrics);
            Assert.Equal("node2", node2Metrics.NodeId);
            Assert.True(node2Metrics.Success);
            Assert.Equal(95, node2Metrics.ItemsProcessed);
            Assert.Equal(90, node2Metrics.ItemsEmitted);
            _ = Assert.NotNull(node2Metrics.ThroughputItemsPerSec);
            Assert.Equal(0.158, node2Metrics.AverageItemProcessingMs!.Value, 3);
        }

        [Fact]
        public async Task EndToEndPipelineExecution_WithObserver_ShouldCollectMetrics()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();
            var observer = new MetricsCollectingExecutionObserver(collector);

            var startTime = DateTimeOffset.UtcNow;

            // Act - Simulate node execution with observer
            observer.OnNodeStarted(new NodeExecutionStarted("node1", "TestNode", startTime));
            await Task.Delay(10);
            collector.RecordItemMetrics("node1", 100, 90);
            observer.OnNodeCompleted(new NodeExecutionCompleted("node1", "TestNode", TimeSpan.FromMilliseconds(10), true, null));

            observer.OnNodeStarted(new NodeExecutionStarted("node2", "TestNode", startTime));
            await Task.Delay(10);
            collector.RecordItemMetrics("node2", 50, 50);
            observer.OnNodeCompleted(new NodeExecutionCompleted("node2", "TestNode", TimeSpan.FromMilliseconds(10), true, null));

            // Assert
            var node1Metrics = collector.GetNodeMetrics("node1");
            Assert.NotNull(node1Metrics);
            Assert.Equal("node1", node1Metrics.NodeId);
            Assert.True(node1Metrics.Success);
            Assert.Equal(100, node1Metrics.ItemsProcessed);
            Assert.Equal(90, node1Metrics.ItemsEmitted);

            var node2Metrics = collector.GetNodeMetrics("node2");
            Assert.NotNull(node2Metrics);
            Assert.Equal("node2", node2Metrics.NodeId);
            Assert.True(node2Metrics.Success);
            Assert.Equal(50, node2Metrics.ItemsProcessed);
            Assert.Equal(50, node2Metrics.ItemsEmitted);
        }

        #endregion

        #region Error Condition Tests

        [Fact]
        public async Task MetricsCollection_WithNodeFailure_ShouldRecordException()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            var expectedException = new InvalidOperationException("Node failed");

            // Act
            collector.RecordNodeStart("failingNode", DateTimeOffset.UtcNow);
            await Task.Delay(10);
            collector.RecordNodeEnd("failingNode", DateTimeOffset.UtcNow, false, expectedException);
            collector.RecordItemMetrics("failingNode", 50, 0);

            // Assert
            var metrics = collector.GetNodeMetrics("failingNode");
            Assert.NotNull(metrics);
            Assert.False(metrics.Success);
            Assert.NotNull(metrics.Exception);
            Assert.Equal(expectedException.Message, metrics.Exception.Message);
            Assert.Equal(50, metrics.ItemsProcessed);
            Assert.Equal(0, metrics.ItemsEmitted);
        }

        [Fact]
        public async Task MetricsCollection_WithPipelineFailure_ShouldRecordException()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            var pipelineName = "FailingPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;
            var expectedException = new Exception("Pipeline failed");

            // Act
            collector.RecordNodeStart("node1", startTime);
            await Task.Delay(10);
            collector.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("node1", 100, 100);

            collector.RecordNodeStart("node2", DateTimeOffset.UtcNow);
            await Task.Delay(10);
            collector.RecordNodeEnd("node2", DateTimeOffset.UtcNow, false, expectedException);

            var endTime = DateTimeOffset.UtcNow;
            var pipelineMetrics = collector.CreatePipelineMetrics(pipelineName, runId, startTime, endTime, false, expectedException);

            // Assert
            Assert.NotNull(pipelineMetrics);
            Assert.False(pipelineMetrics.Success);
            Assert.NotNull(pipelineMetrics.Exception);
            Assert.Equal(expectedException.Message, pipelineMetrics.Exception.Message);
            Assert.Equal(2, pipelineMetrics.NodeMetrics.Count);
            Assert.Equal(100, pipelineMetrics.TotalItemsProcessed);
        }

        [Fact]
        public async Task MetricsCollection_WithRetries_ShouldRecordRetryCount()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            // Act - Simulate retries
            collector.RecordNodeStart("retryNode", DateTimeOffset.UtcNow);
            collector.RecordRetry("retryNode", 1, "Temporary failure");
            await Task.Delay(10);
            collector.RecordRetry("retryNode", 2, "Another temporary failure");
            await Task.Delay(10);
            collector.RecordNodeEnd("retryNode", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("retryNode", 100, 100);

            // Assert
            var metrics = collector.GetNodeMetrics("retryNode");
            Assert.NotNull(metrics);
            Assert.True(metrics.Success);
            Assert.Equal(2, metrics.RetryCount);
            Assert.Equal(100, metrics.ItemsProcessed);
        }

        #endregion

        #region Multiple Observers Tests

        [Fact]
        public async Task MultipleObservers_CompositeScenario_ShouldCollectMetricsFromAll()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            var observer1 = new MetricsCollectingExecutionObserver(collector);
            var observer2 = new MetricsCollectingExecutionObserver(collector);

            var startTime = DateTimeOffset.UtcNow;

            // Act - Both observers record events
            observer1.OnNodeStarted(new NodeExecutionStarted("node1", "TestNode", startTime));
            await Task.Delay(10);
            collector.RecordItemMetrics("node1", 100, 90);
            observer1.OnNodeCompleted(new NodeExecutionCompleted("node1", "TestNode", TimeSpan.FromMilliseconds(10), true, null));

            observer2.OnNodeStarted(new NodeExecutionStarted("node2", "TestNode", startTime));
            await Task.Delay(15);
            collector.RecordItemMetrics("node2", 50, 50);
            observer2.OnNodeCompleted(new NodeExecutionCompleted("node2", "TestNode", TimeSpan.FromMilliseconds(15), true, null));

            // Assert - Both nodes should be recorded
            var allMetrics = collector.GetNodeMetrics();
            Assert.Equal(2, allMetrics.Count);

            var node1Metrics = collector.GetNodeMetrics("node1");
            Assert.NotNull(node1Metrics);
            Assert.Equal(100, node1Metrics.ItemsProcessed);

            var node2Metrics = collector.GetNodeMetrics("node2");
            Assert.NotNull(node2Metrics);
            Assert.Equal(50, node2Metrics.ItemsProcessed);
        }

        #endregion

        #region Custom Metrics Sinks Tests

        [Fact]
        public async Task CustomMetricsSink_ShouldReceiveMetrics()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<CustomMetricsSink>>();
            var customSink = new CustomMetricsSink(loggerMock);
            var factory = new TestObservabilityFactory();
            var collector = new ObservabilityCollector(factory);

            // Act
            collector.RecordNodeStart("node1", DateTimeOffset.UtcNow);
            await Task.Delay(10);
            collector.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("node1", 100, 95);

            // Manually call to custom sink
            var metrics = collector.GetNodeMetrics("node1");
            if (metrics != null)
            {
                await customSink.RecordAsync(metrics);
            }

            // Assert
            Assert.True(customSink.WasCalled);
            Assert.Equal("node1", customSink.LastNodeId);
        }

        [Fact]
        public async Task CustomPipelineMetricsSink_ShouldReceiveMetrics()
        {
            // Arrange
            var loggerMock = A.Fake<ILogger<CustomPipelineMetricsSink>>();
            var customSink = new CustomPipelineMetricsSink(loggerMock);
            var factory = new TestObservabilityFactory();
            var collector = new ObservabilityCollector(factory);

            var pipelineName = "TestPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;

            // Act
            collector.RecordNodeStart("node1", startTime);
            await Task.Delay(10);
            collector.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("node1", 100, 95);

            var endTime = DateTimeOffset.UtcNow;
            var pipelineMetrics = collector.CreatePipelineMetrics(pipelineName, runId, startTime, endTime, true);
            await customSink.RecordAsync(pipelineMetrics);

            // Assert
            Assert.True(customSink.WasCalled);
            Assert.Equal(pipelineName, customSink.LastPipelineName);
        }

        #endregion

        #region Performance Under Load Tests

        [Fact]
        public async Task PerformanceUnderLoad_BasicValidation_ShouldHandleConcurrentOperations()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            var nodeCount = 100;
            var tasks = new List<Task>();

            // Act - Simulate concurrent node operations
            for (var i = 0; i < nodeCount; i++)
            {
                var nodeId = $"node{i}";
                tasks.Add(Task.Run(async () =>
                {
                    collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow);
                    await Task.Delay(1);
                    collector.RecordNodeEnd(nodeId, DateTimeOffset.UtcNow, true);
                    collector.RecordItemMetrics(nodeId, 10, 10);
                }));
            }

            await Task.WhenAll(tasks);

            // Assert
            var allMetrics = collector.GetNodeMetrics();
            Assert.Equal(nodeCount, allMetrics.Count);

            // Verify all nodes were recorded correctly
            for (var i = 0; i < nodeCount; i++)
            {
                var metrics = collector.GetNodeMetrics($"node{i}");
                Assert.NotNull(metrics);
                Assert.True(metrics.Success);
                Assert.Equal(10, metrics.ItemsProcessed);
            }
        }

        [Fact]
        public async Task PerformanceUnderLoad_WithObserver_ShouldHandleConcurrentEvents()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();
            var observer = new MetricsCollectingExecutionObserver(collector);

            var eventCount = 50;
            var tasks = new List<Task>();

            // Act - Simulate concurrent observer events
            for (var i = 0; i < eventCount; i++)
            {
                var nodeId = $"node{i}";
                tasks.Add(Task.Run(async () =>
                {
                    observer.OnNodeStarted(new NodeExecutionStarted(nodeId, "TestNode", DateTimeOffset.UtcNow));
                    await Task.Delay(1);
                    observer.OnNodeCompleted(new NodeExecutionCompleted(nodeId, "TestNode", TimeSpan.FromMilliseconds(1), true, null));
                }));
            }

            await Task.WhenAll(tasks);

            // Assert
            var allMetrics = collector.GetNodeMetrics();
            Assert.Equal(eventCount, allMetrics.Count);
        }

        [Fact]
        public async Task PerformanceUnderLoad_MetricsAccuracy_ShouldMaintainAccuracy()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            var iterations = 20;
            var itemsPerNode = 1000;

            // Act
            for (var i = 0; i < iterations; i++)
            {
                var nodeId = $"node{i}";
                collector.RecordNodeStart(nodeId, DateTimeOffset.UtcNow);
                await Task.Delay(5);
                collector.RecordNodeEnd(nodeId, DateTimeOffset.UtcNow, true);
                collector.RecordItemMetrics(nodeId, itemsPerNode, itemsPerNode);
                collector.RecordPerformanceMetrics(nodeId, 200000, 0.005);
            }

            // Assert
            var allMetrics = collector.GetNodeMetrics();
            Assert.Equal(iterations, allMetrics.Count);

            var totalItems = allMetrics.Sum(m => m.ItemsProcessed);
            Assert.Equal(iterations * itemsPerNode, totalItems);

            // Verify performance metrics are recorded
            foreach (var metrics in allMetrics)
            {
                _ = Assert.NotNull(metrics.ThroughputItemsPerSec);
                Assert.True(metrics.ThroughputItemsPerSec > 0);
                Assert.True(metrics.DurationMs > 0);
            }
        }

        #endregion

        #region DI Integration Tests

        [Fact]
        public async Task DIIntegration_EndToEnd_ShouldWorkWithDependencyInjection()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();

            // Act - Create multiple scopes to test scoped lifetime
            var scope1 = provider.CreateScope();
            var collector1 = scope1.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            collector1.RecordNodeStart("node1", DateTimeOffset.UtcNow);
            await Task.Delay(10);
            collector1.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);

            var scope2 = provider.CreateScope();
            var collector2 = scope2.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            collector2.RecordNodeStart("node2", DateTimeOffset.UtcNow);
            await Task.Delay(10);
            collector2.RecordNodeEnd("node2", DateTimeOffset.UtcNow, true);

            // Assert
            Assert.NotSame(collector1, collector2); // Different scopes, different instances
            Assert.NotNull(collector1.GetNodeMetrics("node1"));
            Assert.Null(collector1.GetNodeMetrics("node2")); // node2 not in scope1
            Assert.Null(collector2.GetNodeMetrics("node1")); // node1 not in scope2
            Assert.NotNull(collector2.GetNodeMetrics("node2"));
        }

        [Fact]
        public async Task DIIntegration_WithFactory_ShouldResolveCollector()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var factory = scope.ServiceProvider.GetRequiredService<IObservabilityFactory>();

            // Act
            var collector = factory.ResolveObservabilityCollector();
            Assert.NotNull(collector);

            collector.RecordNodeStart("node1", DateTimeOffset.UtcNow);
            await Task.Delay(10);
            collector.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);

            // Assert
            var metrics = collector.GetNodeMetrics("node1");
            Assert.NotNull(metrics);
        }

        #endregion

        #region End-to-End Metrics Flow to Sinks Tests

        [Fact]
        public async Task EndToEndMetricsFlow_WithDefaultSinks_ShouldEmitToAllSinks()
        {
            // Arrange
            var services = new ServiceCollection();
            _ = services.AddNPipelineObservability();
            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            var pipelineName = "TestPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;

            // Act - Simulate pipeline execution
            collector.RecordNodeStart("node1", startTime);
            await Task.Delay(10);
            collector.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("node1", 100, 95);

            collector.RecordNodeStart("node2", DateTimeOffset.UtcNow);
            await Task.Delay(15);
            collector.RecordNodeEnd("node2", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("node2", 95, 90);

            var endTime = DateTimeOffset.UtcNow;

            // Emit metrics - this should call all registered sinks
            await collector.EmitMetricsAsync(pipelineName, runId, startTime, endTime, true);

            // Assert
            // Verify metrics were collected
            var allMetrics = collector.GetNodeMetrics();
            Assert.Equal(2, allMetrics.Count);
            Assert.Equal(195, allMetrics.Sum(m => m.ItemsProcessed));

            // Verify each node has correct metrics
            var node1Metrics = collector.GetNodeMetrics("node1");
            Assert.NotNull(node1Metrics);
            Assert.Equal(100, node1Metrics.ItemsProcessed);
            Assert.Equal(95, node1Metrics.ItemsEmitted);

            var node2Metrics = collector.GetNodeMetrics("node2");
            Assert.NotNull(node2Metrics);
            Assert.Equal(95, node2Metrics.ItemsProcessed);
            Assert.Equal(90, node2Metrics.ItemsEmitted);
        }

        [Fact]
        public async Task EndToEndMetricsFlow_WithCustomSinks_ShouldReceiveCorrectMetrics()
        {
            // Arrange
            var services = new ServiceCollection();

            // Register custom sinks using factory delegates
            var nodeSink = new TestNodeMetricsSink();
            var pipelineSink = new TestPipelineMetricsSink();

            _ = services.AddNPipelineObservability(
                sp => nodeSink,
                sp => pipelineSink);

            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            var pipelineName = "CustomSinkPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;

            // Act - Simulate pipeline execution
            collector.RecordNodeStart("transform1", startTime);
            await Task.Delay(10);
            collector.RecordNodeEnd("transform1", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("transform1", 50, 45);

            collector.RecordNodeStart("transform2", DateTimeOffset.UtcNow);
            await Task.Delay(15);
            collector.RecordNodeEnd("transform2", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("transform2", 45, 40);

            var endTime = DateTimeOffset.UtcNow;

            // Emit metrics
            await collector.EmitMetricsAsync(pipelineName, runId, startTime, endTime, true);

            // Assert - Verify sinks received metrics
            Assert.Equal(2, nodeSink.ReceivedMetrics.Count);
            var pipelineMetrics = Assert.Single(pipelineSink.ReceivedMetrics);

            // Verify node sink received correct data
            var transform1Metrics = nodeSink.ReceivedMetrics.FirstOrDefault(m => m.NodeId == "transform1");
            Assert.NotNull(transform1Metrics);
            Assert.Equal(50, transform1Metrics.ItemsProcessed);
            Assert.Equal(45, transform1Metrics.ItemsEmitted);

            var transform2Metrics = nodeSink.ReceivedMetrics.FirstOrDefault(m => m.NodeId == "transform2");
            Assert.NotNull(transform2Metrics);
            Assert.Equal(45, transform2Metrics.ItemsProcessed);
            Assert.Equal(40, transform2Metrics.ItemsEmitted);

            // Verify pipeline sink received correct data
            Assert.Equal(pipelineName, pipelineMetrics.PipelineName);
            Assert.Equal(runId, pipelineMetrics.RunId);
            Assert.Equal(95, pipelineMetrics.TotalItemsProcessed); // 50 + 45
            Assert.Equal(2, pipelineMetrics.NodeMetrics.Count);
        }

        [Fact]
        public async Task EndToEndMetricsFlow_WithPipelineFailure_ShouldEmitFailureMetrics()
        {
            // Arrange
            var services = new ServiceCollection();

            var nodeSink = new TestNodeMetricsSink();
            var pipelineSink = new TestPipelineMetricsSink();

            _ = services.AddNPipelineObservability(
                sp => nodeSink,
                sp => pipelineSink);

            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            var pipelineName = "FailingPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;
            var expectedException = new InvalidOperationException("Pipeline failed");

            // Act - Simulate pipeline execution with failure
            collector.RecordNodeStart("node1", startTime);
            await Task.Delay(10);
            collector.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("node1", 100, 100);

            collector.RecordNodeStart("node2", DateTimeOffset.UtcNow);
            await Task.Delay(10);
            collector.RecordNodeEnd("node2", DateTimeOffset.UtcNow, false, expectedException);
            collector.RecordItemMetrics("node2", 50, 0);

            var endTime = DateTimeOffset.UtcNow;

            // Emit metrics with failure
            await collector.EmitMetricsAsync(pipelineName, runId, startTime, endTime, false, expectedException);

            // Assert - Verify sinks received failure metrics
            Assert.Equal(2, nodeSink.ReceivedMetrics.Count);
            var failurePipelineMetrics = Assert.Single(pipelineSink.ReceivedMetrics);

            // Verify node2 has failure recorded
            var node2Metrics = nodeSink.ReceivedMetrics.FirstOrDefault(m => m.NodeId == "node2");
            Assert.NotNull(node2Metrics);
            Assert.False(node2Metrics.Success);
            Assert.NotNull(node2Metrics.Exception);
            Assert.Equal(expectedException.Message, node2Metrics.Exception.Message);

            // Verify pipeline sink received failure
            Assert.False(failurePipelineMetrics.Success);
            Assert.NotNull(failurePipelineMetrics.Exception);
            Assert.Equal(expectedException.Message, failurePipelineMetrics.Exception.Message);
            Assert.Equal(150, failurePipelineMetrics.TotalItemsProcessed); // 100 + 50
        }

        [Fact]
        public async Task EndToEndMetricsFlow_WithRetries_ShouldRecordRetryCountInSink()
        {
            // Arrange
            var services = new ServiceCollection();

            var nodeSink = new TestNodeMetricsSink();
            var pipelineSink = new TestPipelineMetricsSink();

            _ = services.AddNPipelineObservability(
                sp => nodeSink,
                sp => pipelineSink);

            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            var pipelineName = "RetryPipeline";
            var runId = Guid.NewGuid();
            var startTime = DateTimeOffset.UtcNow;

            // Act - Simulate pipeline execution with retries
            collector.RecordNodeStart("retryNode", startTime);
            collector.RecordRetry("retryNode", 1, "Temporary failure");
            await Task.Delay(10);
            collector.RecordRetry("retryNode", 2, "Another temporary failure");
            await Task.Delay(10);
            collector.RecordNodeEnd("retryNode", DateTimeOffset.UtcNow, true);
            collector.RecordItemMetrics("retryNode", 100, 100);

            var endTime = DateTimeOffset.UtcNow;

            // Emit metrics
            await collector.EmitMetricsAsync(pipelineName, runId, startTime, endTime, true);

            // Assert - Verify retry count was recorded and sent to sink
            var nodeMetrics = Assert.Single(nodeSink.ReceivedMetrics);

            Assert.Equal("retryNode", nodeMetrics.NodeId);
            Assert.Equal(2, nodeMetrics.RetryCount);
            Assert.True(nodeMetrics.Success);
        }

        [Fact]
        public async Task EndToEndMetricsFlow_MultiplePipelineRuns_ShouldIsolateMetrics()
        {
            // Arrange
            var services = new ServiceCollection();

            var nodeSink = new TestNodeMetricsSink();
            var pipelineSink = new TestPipelineMetricsSink();

            _ = services.AddNPipelineObservability(
                sp => nodeSink,
                sp => pipelineSink);

            var provider = services.BuildServiceProvider();

            // Act - Run first pipeline
            var scope1 = provider.CreateScope();
            var collector1 = scope1.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            collector1.RecordNodeStart("node1", DateTimeOffset.UtcNow);
            await Task.Delay(10);
            collector1.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);
            collector1.RecordItemMetrics("node1", 100, 95);

            await collector1.EmitMetricsAsync("Pipeline1", Guid.NewGuid(), DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, true);

            // Run second pipeline
            var scope2 = provider.CreateScope();
            var collector2 = scope2.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            collector2.RecordNodeStart("node1", DateTimeOffset.UtcNow);
            await Task.Delay(10);
            collector2.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);
            collector2.RecordItemMetrics("node1", 50, 45);

            await collector2.EmitMetricsAsync("Pipeline2", Guid.NewGuid(), DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, true);

            // Assert - Verify each pipeline run is isolated
            Assert.Equal(2, nodeSink.ReceivedMetrics.Count);
            Assert.Equal(2, pipelineSink.ReceivedMetrics.Count);

            // Verify first pipeline metrics
            var pipeline1Metrics = pipelineSink.ReceivedMetrics[0];
            Assert.Equal("Pipeline1", pipeline1Metrics.PipelineName);
            Assert.Equal(100, pipeline1Metrics.TotalItemsProcessed);

            // Verify second pipeline metrics
            var pipeline2Metrics = pipelineSink.ReceivedMetrics[1];
            Assert.Equal("Pipeline2", pipeline2Metrics.PipelineName);
            Assert.Equal(50, pipeline2Metrics.TotalItemsProcessed);
        }

        #endregion

        #region Helper Classes

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

        public sealed class TestNodeMetricsSink : IMetricsSink
        {
            public List<INodeMetrics> ReceivedMetrics { get; } = [];

            public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken = default)
            {
                ReceivedMetrics.Add(nodeMetrics);
                return Task.CompletedTask;
            }
        }

        public sealed class TestPipelineMetricsSink : IPipelineMetricsSink
        {
            public List<IPipelineMetrics> ReceivedMetrics { get; } = [];

            public Task RecordAsync(IPipelineMetrics pipelineMetrics, CancellationToken cancellationToken = default)
            {
                ReceivedMetrics.Add(pipelineMetrics);
                return Task.CompletedTask;
            }
        }

        public sealed class CustomMetricsSink(ILogger<CustomMetricsSink> logger) : IMetricsSink
        {
            public bool WasCalled { get; private set; }
            public string? LastNodeId { get; private set; }

            public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken = default)
            {
                WasCalled = true;
                LastNodeId = nodeMetrics.NodeId;
                logger.LogInformation("Custom sink received metrics for node {NodeId}", nodeMetrics.NodeId);
                return Task.CompletedTask;
            }
        }

        public sealed class CustomPipelineMetricsSink(ILogger<CustomPipelineMetricsSink> logger) : IPipelineMetricsSink
        {
            public bool WasCalled { get; private set; }
            public string? LastPipelineName { get; private set; }

            public Task RecordAsync(IPipelineMetrics pipelineMetrics, CancellationToken cancellationToken = default)
            {
                WasCalled = true;
                LastPipelineName = pipelineMetrics.PipelineName;
                logger.LogInformation("Custom sink received pipeline metrics for {PipelineName}", pipelineMetrics.PipelineName);
                return Task.CompletedTask;
            }
        }

        #endregion
    }
}