using NPipeline.Observability;
using NPipeline.Observability.Configuration;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Comprehensive tests for <see cref="AutoObservabilityScope" />.
/// </summary>
public sealed class AutoObservabilityScopeTests
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
        var nodeId = "testNode";
        var options = ObservabilityOptions.Default;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new AutoObservabilityScope(collector, nodeId, options));
    }

    [Fact]
    public void Constructor_WithNullNodeId_ShouldThrowArgumentNullException()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        string nodeId = null!;
        var options = ObservabilityOptions.Default;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new AutoObservabilityScope(collector, nodeId, options));
    }

    [Fact]
    public void Constructor_WithNullOptions_ShouldThrowArgumentNullException()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        ObservabilityOptions options = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new AutoObservabilityScope(collector, nodeId, options));
    }

    [Fact]
    public void Constructor_WithValidParameters_ShouldCreateInstance()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Default;

        // Act
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Assert
        // AutoObservabilityScope is a struct, so it's always instantiated
        Assert.NotNull(scope.ToString());
    }

    #endregion

    #region RecordItemCount Tests

    [Fact]
    public void RecordItemCount_ShouldUpdateMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        scope.RecordItemCount(100, 95);
        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(100, metrics.ItemsProcessed);
        Assert.Equal(95, metrics.ItemsEmitted);
    }

    [Fact]
    public void RecordItemCount_MultipleCalls_ShouldUseLastValue()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        scope.RecordItemCount(50, 45);
        scope.RecordItemCount(30, 28);
        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);

        // RecordItemCount sets the value, not increments
        Assert.Equal(30, metrics.ItemsProcessed);
        Assert.Equal(28, metrics.ItemsEmitted);
    }

    [Fact]
    public void RecordItemCount_WithRecordItemCountsDisabled_ShouldNotRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = false };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        scope.RecordItemCount(100, 95);
        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.ItemsProcessed);
        Assert.Equal(0, metrics.ItemsEmitted);
    }

    #endregion

    #region IncrementProcessed Tests

    [Fact]
    public void IncrementProcessed_ShouldUpdateMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        for (var i = 0; i < 50; i++)
        {
            scope.IncrementProcessed();
        }

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(50, metrics.ItemsProcessed);
    }

    [Fact]
    public void IncrementProcessed_MultipleCalls_ShouldAccumulate()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        for (var i = 0; i < 30; i++)
        {
            scope.IncrementProcessed();
        }

        for (var i = 0; i < 20; i++)
        {
            scope.IncrementProcessed();
        }

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(50, metrics.ItemsProcessed);
    }

    [Fact]
    public void IncrementProcessed_WithRecordItemCountsDisabled_ShouldNotRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = false };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        for (var i = 0; i < 50; i++)
        {
            scope.IncrementProcessed();
        }

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.ItemsProcessed);
    }

    #endregion

    #region IncrementEmitted Tests

    [Fact]
    public void IncrementEmitted_ShouldUpdateMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        for (var i = 0; i < 45; i++)
        {
            scope.IncrementEmitted();
        }

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(45, metrics.ItemsEmitted);
    }

    [Fact]
    public void IncrementEmitted_MultipleCalls_ShouldAccumulate()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        for (var i = 0; i < 28; i++)
        {
            scope.IncrementEmitted();
        }

        for (var i = 0; i < 17; i++)
        {
            scope.IncrementEmitted();
        }

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(45, metrics.ItemsEmitted);
    }

    [Fact]
    public void IncrementEmitted_WithRecordItemCountsDisabled_ShouldNotRecord()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = false };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        for (var i = 0; i < 45; i++)
        {
            scope.IncrementEmitted();
        }

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.ItemsEmitted);
    }

    #endregion

    #region RecordFailure Tests

    [Fact]
    public void RecordFailure_ShouldSetSuccessToFalse()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Default;
        var scope = new AutoObservabilityScope(collector, nodeId, options);
        var exception = new InvalidOperationException("Test exception");

        // Act
        scope.RecordFailure(exception);
        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.False(metrics.Success);
    }

    [Fact]
    public void RecordFailure_MultipleCalls_ShouldUseLastException()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Default;
        var scope = new AutoObservabilityScope(collector, nodeId, options);
        var exception1 = new InvalidOperationException("Exception 1");
        var exception2 = new InvalidOperationException("Exception 2");

        // Act
        scope.RecordFailure(exception1);
        scope.RecordFailure(exception2);
        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.False(metrics.Success);
        Assert.NotNull(metrics.Exception);
        Assert.Equal("Exception 2", metrics.Exception.Message);
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public void Dispose_ShouldRecordMetricsToCollector()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true, RecordTiming = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        scope.RecordItemCount(100, 95);

        for (var i = 0; i < 10; i++)
        {
            scope.IncrementProcessed();
        }

        for (var i = 0; i < 5; i++)
        {
            scope.IncrementEmitted();
        }

        // Add a small delay to ensure duration is measurable
        Thread.Sleep(1);

        // Act
        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);

        // RecordItemCount sets the value, then IncrementProcessed adds to it
        // So we expect 100 (set by RecordItemCount) + 10 (increments) = 110
        Assert.Equal(110, metrics.ItemsProcessed);

        // Similarly for emitted: 95 + 5 = 100
        Assert.Equal(100, metrics.ItemsEmitted);

        // DurationMs is now recorded when RecordTiming is enabled
        _ = Assert.NotNull(metrics.DurationMs);
        Assert.True(metrics.DurationMs >= 0);
    }

    [Fact]
    public void Dispose_MultipleTimes_ShouldNotThrow()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Default;
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act & Assert
        var exception = Record.Exception(() =>
        {
            scope.Dispose();
            scope.Dispose();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_WithoutRecordTiming_ShouldNotRecordDuration()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordTiming = false };
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Null(metrics.DurationMs);
    }

    #endregion

    #region Thread-Safety Tests

    [Fact]
    public void ConcurrentItemOperations_ShouldBeThreadSafe()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = new ObservabilityOptions { RecordItemCounts = true };
        var scope = new AutoObservabilityScope(collector, nodeId, options);
        var threadCount = 20;
        var itemsPerThread = 100;

        // Act
        _ = Parallel.For(0, threadCount, i =>
        {
            for (var j = 0; j < itemsPerThread; j++)
            {
                scope.IncrementProcessed();
                scope.IncrementEmitted();
            }
        });

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(threadCount * itemsPerThread, metrics.ItemsProcessed);
        Assert.Equal(threadCount * itemsPerThread, metrics.ItemsEmitted);
    }

    [Fact]
    public void ConcurrentFailureRecording_ShouldUseLastRecordedException()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Default;
        var scope = new AutoObservabilityScope(collector, nodeId, options);
        var failureCount = 50;

        // Act
        _ = Parallel.For(0, failureCount, i => { scope.RecordFailure(new InvalidOperationException($"Exception {i}")); });
        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.False(metrics.Success);

        // The last exception recorded wins
        Assert.NotNull(metrics.Exception);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public void FullWorkflow_ShouldCollectCompleteMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Full;
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        scope.RecordItemCount(100, 95);

        for (var i = 0; i < 10; i++)
        {
            scope.IncrementProcessed();
        }

        for (var i = 0; i < 5; i++)
        {
            scope.IncrementEmitted();
        }

        // Add a small delay to ensure duration is measurable
        Thread.Sleep(1);

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(110, metrics.ItemsProcessed);
        Assert.Equal(100, metrics.ItemsEmitted);
        Assert.NotNull(metrics.DurationMs);
        Assert.True(metrics.DurationMs.Value > 0);
    }

    [Fact]
    public void MinimalOptions_ShouldOnlyRecordEnabledMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Minimal;
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        scope.RecordItemCount(100, 95);

        // Add a small delay to ensure duration is measurable
        Thread.Sleep(1);

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.ItemsProcessed); // RecordItemCounts is false
        Assert.Equal(0, metrics.ItemsEmitted);
        Assert.NotNull(metrics.DurationMs); // RecordTiming is true
    }

    [Fact]
    public void DisabledOptions_ShouldNotRecordAnyMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";
        var options = ObservabilityOptions.Disabled;
        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        scope.RecordItemCount(100, 95);

        for (var i = 0; i < 10; i++)
        {
            scope.IncrementProcessed();
        }

        for (var i = 0; i < 5; i++)
        {
            scope.IncrementEmitted();
        }

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(0, metrics.ItemsProcessed);
        Assert.Equal(0, metrics.ItemsEmitted);
        Assert.Null(metrics.DurationMs);
    }

    [Fact]
    public void WithPerformanceMetrics_ShouldCalculateThroughputAndAvgTime()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";

        var options = new ObservabilityOptions
        {
            RecordItemCounts = true,
            RecordPerformanceMetrics = true,
            RecordTiming = true,
        };

        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        for (var i = 0; i < 100; i++)
        {
            scope.IncrementProcessed();
        }

        // Add a small delay to ensure duration is measurable
        Thread.Sleep(1);

        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.Equal(100, metrics.ItemsProcessed);
        Assert.NotNull(metrics.ThroughputItemsPerSec);
        Assert.True(metrics.ThroughputItemsPerSec.Value > 0);
        Assert.NotNull(metrics.AverageItemProcessingMs);
        Assert.True(metrics.AverageItemProcessingMs.Value > 0);
    }

    [Fact]
    public void WithMemoryRecording_ShouldRecordMemoryMetrics()
    {
        // Arrange
        var collector = new ObservabilityCollector(new TestObservabilityFactory());
        var nodeId = "testNode";

        var options = new ObservabilityOptions
        {
            RecordTiming = true,
            RecordMemoryUsage = true,
        };

        var scope = new AutoObservabilityScope(collector, nodeId, options);

        // Act
        scope.Dispose();

        // Assert
        var metrics = collector.GetNodeMetrics(nodeId);
        Assert.NotNull(metrics);
        Assert.NotNull(metrics.PeakMemoryUsageMb);
        Assert.True(metrics.PeakMemoryUsageMb.Value >= 0);
    }

    #endregion
}
