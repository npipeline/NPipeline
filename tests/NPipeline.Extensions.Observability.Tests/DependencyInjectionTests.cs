using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NPipeline.Observability;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Comprehensive tests for <see cref="ObservabilityServiceCollectionExtensions" />.
/// </summary>
public sealed class DependencyInjectionTests
{
    #region AddNPipelineObservability Default Tests

    [Fact]
    public void AddNPipelineObservability_Default_ShouldRegisterAllServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        _ = services.AddNPipelineObservability();

        // Assert
        var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetService<IObservabilityCollector>());
        Assert.NotNull(provider.GetService<IMetricsSink>());
        Assert.NotNull(provider.GetService<IPipelineMetricsSink>());
        Assert.NotNull(provider.GetService<IObservabilityFactory>());
    }

    [Fact]
    public void AddNPipelineObservability_Default_ShouldUseLoggingSinks()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();

        // Act
        var provider = services.BuildServiceProvider();
        var metricsSink = provider.GetRequiredService<IMetricsSink>();
        var pipelineMetricsSink = provider.GetRequiredService<IPipelineMetricsSink>();

        // Assert
        _ = Assert.IsType<LoggingMetricsSink>(metricsSink);
        _ = Assert.IsType<LoggingPipelineMetricsSink>(pipelineMetricsSink);
    }

    [Fact]
    public void AddNPipelineObservability_Default_WithNullServices_ShouldThrowArgumentNullException()
    {
        // Arrange
        IServiceCollection services = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => services.AddNPipelineObservability());
    }

    [Fact]
    public void AddNPipelineObservability_Default_ShouldRegisterCollectorAsScoped()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();

        // Act
        var provider = services.BuildServiceProvider();
        var scope1 = provider.CreateScope();
        var scope2 = provider.CreateScope();
        var collector1 = scope1.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        var collector2 = scope1.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        var collector3 = scope2.ServiceProvider.GetRequiredService<IObservabilityCollector>();

        // Assert
        Assert.Same(collector1, collector2); // Same scope, same instance
        Assert.NotSame(collector1, collector3); // Different scope, different instance
    }

    [Fact]
    public void AddNPipelineObservability_Default_ShouldRegisterFactoryAsScoped()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();

        // Act
        var provider = services.BuildServiceProvider();
        var scope1 = provider.CreateScope();
        var scope2 = provider.CreateScope();
        var factory1 = scope1.ServiceProvider.GetRequiredService<IObservabilityFactory>();
        var factory2 = scope1.ServiceProvider.GetRequiredService<IObservabilityFactory>();
        var factory3 = scope2.ServiceProvider.GetRequiredService<IObservabilityFactory>();

        // Assert
        Assert.Same(factory1, factory2); // Same scope, same instance
        Assert.NotSame(factory1, factory3); // Different scope, different instance
    }

    [Fact]
    public void AddNPipelineObservability_Default_ShouldRegisterSinksAsTransient()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();

        // Act
        var provider = services.BuildServiceProvider();
        var sink1 = provider.GetRequiredService<IMetricsSink>();
        var sink2 = provider.GetRequiredService<IMetricsSink>();
        var pipelineSink1 = provider.GetRequiredService<IPipelineMetricsSink>();
        var pipelineSink2 = provider.GetRequiredService<IPipelineMetricsSink>();

        // Assert
        Assert.NotSame(sink1, sink2); // Transient, different instances
        Assert.NotSame(pipelineSink1, pipelineSink2); // Transient, different instances
    }

    #endregion

    #region AddNPipelineObservability With Custom Sinks Tests

    [Fact]
    public void AddNPipelineObservability_WithCustomSinks_ShouldRegisterAllServices()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddLogging(); // Add logging services

        // Act
        _ = services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>();

        // Assert
        var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetService<IObservabilityCollector>());
        Assert.NotNull(provider.GetService<IMetricsSink>());
        Assert.NotNull(provider.GetService<IPipelineMetricsSink>());
        Assert.NotNull(provider.GetService<IObservabilityFactory>());
    }

    [Fact]
    public void AddNPipelineObservability_WithCustomSinks_ShouldUseCustomSinks()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddLogging(); // Add logging services
        _ = services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>();

        // Act
        var provider = services.BuildServiceProvider();
        var metricsSink = provider.GetRequiredService<IMetricsSink>();
        var pipelineMetricsSink = provider.GetRequiredService<IPipelineMetricsSink>();

        // Assert
        _ = Assert.IsType<CustomMetricsSink>(metricsSink);
        _ = Assert.IsType<CustomPipelineMetricsSink>(pipelineMetricsSink);
    }

    [Fact]
    public void AddNPipelineObservability_WithCustomSinks_WithNullServices_ShouldThrowArgumentNullException()
    {
        // Arrange
        IServiceCollection services = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>());
    }

    [Fact]
    public void AddNPipelineObservability_WithCustomSinks_ShouldUseDefaultCollector()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>();

        // Act
        var provider = services.BuildServiceProvider();
        var collector = provider.GetRequiredService<IObservabilityCollector>();

        // Assert
        _ = Assert.IsType<ObservabilityCollector>(collector);
    }

    [Fact]
    public void AddNPipelineObservability_WithCustomSinks_ShouldRegisterCollectorAsScoped()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>();

        // Act
        var provider = services.BuildServiceProvider();
        var scope1 = provider.CreateScope();
        var scope2 = provider.CreateScope();
        var collector1 = scope1.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        var collector2 = scope2.ServiceProvider.GetRequiredService<IObservabilityCollector>();

        // Assert
        Assert.NotSame(collector1, collector2); // Different scope, different instance
    }

    #endregion

    #region AddNPipelineObservability With Factory Delegates Tests

    [Fact]
    public void AddNPipelineObservability_WithFactoryDelegates_ShouldRegisterAllServices()
    {
        // Arrange
        var services = new ServiceCollection();
        var loggerMock1 = A.Fake<ILogger<CustomMetricsSink>>();
        var loggerMock2 = A.Fake<ILogger<CustomPipelineMetricsSink>>();

        // Act
        _ = services.AddNPipelineObservability(
            _ => new CustomMetricsSink(loggerMock1),
            _ => new CustomPipelineMetricsSink(loggerMock2));

        // Assert
        var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetService<IObservabilityCollector>());
        Assert.NotNull(provider.GetService<IMetricsSink>());
        Assert.NotNull(provider.GetService<IPipelineMetricsSink>());
        Assert.NotNull(provider.GetService<IObservabilityFactory>());
    }

    [Fact]
    public void AddNPipelineObservability_WithFactoryDelegates_ShouldUseFactoryCreatedSinks()
    {
        // Arrange
        var services = new ServiceCollection();
        var loggerMock1 = A.Fake<ILogger<CustomMetricsSink>>();
        var loggerMock2 = A.Fake<ILogger<CustomPipelineMetricsSink>>();

        // Act
        _ = services.AddNPipelineObservability(
            _ => new CustomMetricsSink(loggerMock1),
            _ => new CustomPipelineMetricsSink(loggerMock2));

        // Assert
        var provider = services.BuildServiceProvider();
        var metricsSink = provider.GetRequiredService<IMetricsSink>();
        var pipelineMetricsSink = provider.GetRequiredService<IPipelineMetricsSink>();

        _ = Assert.IsType<CustomMetricsSink>(metricsSink);
        _ = Assert.IsType<CustomPipelineMetricsSink>(pipelineMetricsSink);
    }

    [Fact]
    public void AddNPipelineObservability_WithFactoryDelegates_WithNullServices_ShouldThrowArgumentNullException()
    {
        // Arrange
        IServiceCollection services = null!;
        var loggerMock1 = A.Fake<ILogger<CustomMetricsSink>>();
        var loggerMock2 = A.Fake<ILogger<CustomPipelineMetricsSink>>();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => services.AddNPipelineObservability(
            _ => new CustomMetricsSink(loggerMock1),
            _ => new CustomPipelineMetricsSink(loggerMock2)));
    }

    [Fact]
    public void AddNPipelineObservability_WithFactoryDelegates_WithNullMetricsSinkFactory_ShouldThrowArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();
        var loggerMock = A.Fake<ILogger<CustomPipelineMetricsSink>>();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => services.AddNPipelineObservability(
            null!,
            _ => new CustomPipelineMetricsSink(loggerMock)));
    }

    [Fact]
    public void AddNPipelineObservability_WithFactoryDelegates_WithNullPipelineMetricsSinkFactory_ShouldThrowArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();
        var loggerMock = A.Fake<ILogger<CustomMetricsSink>>();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => services.AddNPipelineObservability(
            _ => new CustomMetricsSink(loggerMock),
            null!));
    }

    [Fact]
    public void AddNPipelineObservability_WithFactoryDelegates_ShouldUseDefaultCollector()
    {
        // Arrange
        var services = new ServiceCollection();
        var loggerMock1 = A.Fake<ILogger<CustomMetricsSink>>();
        var loggerMock2 = A.Fake<ILogger<CustomPipelineMetricsSink>>();

        // Act
        _ = services.AddNPipelineObservability(
            _ => new CustomMetricsSink(loggerMock1),
            _ => new CustomPipelineMetricsSink(loggerMock2));

        // Assert
        var provider = services.BuildServiceProvider();
        var collector = provider.GetRequiredService<IObservabilityCollector>();

        _ = Assert.IsType<ObservabilityCollector>(collector);
    }

    [Fact]
    public void AddNPipelineObservability_WithFactoryDelegates_ShouldCallFactoryOnEachResolution()
    {
        // Arrange
        var services = new ServiceCollection();
        var callCount = 0;
        var loggerMock = A.Fake<ILogger<CustomMetricsSink>>();
        var loggerMock2 = A.Fake<ILogger<CustomPipelineMetricsSink>>();

        // Act
        _ = services.AddNPipelineObservability(
            _ =>
            {
                callCount++;
                return new CustomMetricsSink(loggerMock);
            },
            _ => new CustomPipelineMetricsSink(loggerMock2));

        // Assert
        var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IMetricsSink>();
        _ = provider.GetRequiredService<IMetricsSink>();
        _ = provider.GetRequiredService<IMetricsSink>();

        Assert.Equal(3, callCount); // Factory called 3 times
    }

    #endregion

    #region AddNPipelineObservability With Custom Collector Tests

    [Fact]
    public void AddNPipelineObservability_WithCustomCollector_ShouldRegisterAllServices()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddLogging(); // Add logging services

        // Act
        _ = services.AddNPipelineObservability<CustomObservabilityCollector, CustomMetricsSink, CustomPipelineMetricsSink>();

        // Assert
        var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetService<IObservabilityCollector>());
        Assert.NotNull(provider.GetService<IMetricsSink>());
        Assert.NotNull(provider.GetService<IPipelineMetricsSink>());
        Assert.NotNull(provider.GetService<IObservabilityFactory>());
    }

    [Fact]
    public void AddNPipelineObservability_WithCustomCollector_ShouldUseCustomCollector()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddLogging(); // Add logging services

        // Act
        _ = services.AddNPipelineObservability<CustomObservabilityCollector, CustomMetricsSink, CustomPipelineMetricsSink>();

        // Assert
        var provider = services.BuildServiceProvider();
        var collector = provider.GetRequiredService<IObservabilityCollector>();

        _ = Assert.IsType<CustomObservabilityCollector>(collector);
    }

    [Fact]
    public void AddNPipelineObservability_WithCustomCollector_WithNullServices_ShouldThrowArgumentNullException()
    {
        // Arrange
        IServiceCollection services = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => services.AddNPipelineObservability<CustomObservabilityCollector, CustomMetricsSink, CustomPipelineMetricsSink>());
    }

    [Fact]
    public void AddNPipelineObservability_WithCustomCollector_ShouldRegisterCollectorAsScoped()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddLogging(); // Add logging services

        // Act
        _ = services.AddNPipelineObservability<CustomObservabilityCollector, CustomMetricsSink, CustomPipelineMetricsSink>();

        // Assert
        var provider = services.BuildServiceProvider();
        var scope1 = provider.CreateScope();
        var scope2 = provider.CreateScope();
        var collector1 = scope1.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        var collector2 = scope2.ServiceProvider.GetRequiredService<IObservabilityCollector>();

        Assert.NotSame(collector1, collector2); // Different scope, different instance
    }

    #endregion

    #region AddNPipelineObservability With Custom Collector Factory Tests

    [Fact]
    public void AddNPipelineObservability_WithCollectorFactory_ShouldRegisterAllServices()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddLogging(); // Add logging services

        // Act
        _ = services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>(
            _ => new CustomObservabilityCollector());

        // Assert
        var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetService<IObservabilityCollector>());
        Assert.NotNull(provider.GetService<IMetricsSink>());
        Assert.NotNull(provider.GetService<IPipelineMetricsSink>());
        Assert.NotNull(provider.GetService<IObservabilityFactory>());
    }

    [Fact]
    public void AddNPipelineObservability_WithCollectorFactory_ShouldUseFactoryCreatedCollector()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddLogging(); // Add logging services

        // Act
        _ = services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>(
            _ => new CustomObservabilityCollector());

        // Assert
        var provider = services.BuildServiceProvider();
        var collector = provider.GetRequiredService<IObservabilityCollector>();

        _ = Assert.IsType<CustomObservabilityCollector>(collector);
    }

    [Fact]
    public void AddNPipelineObservability_WithCollectorFactory_WithNullServices_ShouldThrowArgumentNullException()
    {
        // Arrange
        IServiceCollection services = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>(
            _ => new CustomObservabilityCollector()));
    }

    [Fact]
    public void AddNPipelineObservability_WithCollectorFactory_WithNullCollectorFactory_ShouldThrowArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>(null!));
    }

    [Fact]
    public void AddNPipelineObservability_WithCollectorFactory_ShouldRegisterCollectorAsScoped()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddLogging(); // Add logging services

        // Act
        _ = services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>(
            _ => new CustomObservabilityCollector());

        // Assert
        var provider = services.BuildServiceProvider();
        var scope1 = provider.CreateScope();
        var scope2 = provider.CreateScope();
        var collector1 = scope1.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        var collector2 = scope2.ServiceProvider.GetRequiredService<IObservabilityCollector>();

        Assert.NotSame(collector1, collector2); // Different scope, different instance
    }

    #endregion

    #region TryAdd Behavior Tests

    [Fact]
    public void AddNPipelineObservability_WhenCalledMultipleTimes_ShouldNotOverwriteExistingRegistrations()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        _ = services.AddNPipelineObservability();
        _ = services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>();

        // Assert
        var provider = services.BuildServiceProvider();
        var metricsSink = provider.GetRequiredService<IMetricsSink>();
        var pipelineMetricsSink = provider.GetRequiredService<IPipelineMetricsSink>();

        // First registration should be used (LoggingMetricsSink)
        _ = Assert.IsType<LoggingMetricsSink>(metricsSink);
        _ = Assert.IsType<LoggingPipelineMetricsSink>(pipelineMetricsSink);
    }

    [Fact]
    public void AddNPipelineObservability_WithExistingRegistration_ShouldNotOverwrite()
    {
        // Arrange
        var services = new ServiceCollection();
        var customSink = new CustomMetricsSink(A.Fake<ILogger<CustomMetricsSink>>());
        _ = services.AddSingleton<IMetricsSink>(customSink);

        // Act
        _ = services.AddNPipelineObservability();

        // Assert
        var provider = services.BuildServiceProvider();
        var metricsSink = provider.GetRequiredService<IMetricsSink>();

        Assert.Same(customSink, metricsSink); // Existing registration should be preserved
    }

    #endregion

    #region Integration Tests

    [Fact]
    public void AddNPipelineObservability_FullIntegration_ShouldWorkEndToEnd()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();

        // Act
        var provider = services.BuildServiceProvider();
        var scope = provider.CreateScope();
        var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        var factory = scope.ServiceProvider.GetRequiredService<IObservabilityFactory>();

        // Use the collector
        collector.RecordNodeStart("node1", DateTimeOffset.UtcNow);
        collector.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);
        var metrics = collector.GetNodeMetrics("node1");

        // Use the factory
        var resolvedCollector = factory.ResolveObservabilityCollector();

        // Assert
        Assert.NotNull(metrics);
        Assert.Same(collector, resolvedCollector);
    }

    [Fact]
    public void AddNPipelineObservability_WithCustomImplementation_FullIntegration_ShouldWorkEndToEnd()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability<CustomObservabilityCollector, CustomMetricsSink, CustomPipelineMetricsSink>();

        // Act
        var provider = services.BuildServiceProvider();
        var scope = provider.CreateScope();
        var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

        // Use the custom collector
        collector.RecordNodeStart("node1", DateTimeOffset.UtcNow);
        collector.RecordNodeEnd("node1", DateTimeOffset.UtcNow, true);

        // Assert
        _ = Assert.IsType<CustomObservabilityCollector>(collector);
    }

    #endregion

    #region Helper Classes

    public sealed class CustomMetricsSink(ILogger<CustomMetricsSink> logger) : IMetricsSink
    {
        public void RecordNodeMetrics(INodeMetrics nodeMetrics, CancellationToken _)
        {
            logger.LogInformation("Custom metrics recorded for node {NodeId}", nodeMetrics.NodeId);
        }

        public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken = default)
        {
            RecordNodeMetrics(nodeMetrics, cancellationToken);
            return Task.CompletedTask;
        }
    }

    public sealed class CustomPipelineMetricsSink(ILogger<CustomPipelineMetricsSink> logger) : IPipelineMetricsSink
    {
        public void RecordPipelineMetrics(IPipelineMetrics pipelineMetrics, CancellationToken _)
        {
            logger.LogInformation("Custom pipeline metrics recorded for {PipelineName}", pipelineMetrics.PipelineName);
        }

        public Task RecordAsync(IPipelineMetrics pipelineMetrics, CancellationToken cancellationToken = default)
        {
            RecordPipelineMetrics(pipelineMetrics, cancellationToken);
            return Task.CompletedTask;
        }
    }

    public sealed class CustomObservabilityCollector : IObservabilityCollector
    {
        private readonly Dictionary<string, NodeMetrics> _nodeMetrics = [];

        public void RecordNodeStart(string nodeId, DateTimeOffset timestamp, int? threadId = null, long? initialMemoryMb = null)
        {
            _nodeMetrics[nodeId] = new NodeMetrics(
                nodeId,
                timestamp,
                null,
                null,
                true,
                0,
                0,
                null,
                0,
                initialMemoryMb,
                null,
                null,
                null,
                threadId);
        }

        public void RecordNodeEnd(string nodeId, DateTimeOffset timestamp, bool success, Exception? exception = null, long? peakMemoryMb = null,
            long? processorTimeMs = null)
        {
            if (_nodeMetrics.TryGetValue(nodeId, out var metrics))
            {
                var duration = (long)(timestamp - metrics.StartTime)!.Value.TotalMilliseconds;
                _nodeMetrics[nodeId] = metrics with
                {
                    EndTime = timestamp,
                    DurationMs = duration,
                    Success = success,
                    Exception = exception,
                    PeakMemoryUsageMb = peakMemoryMb,
                    ProcessorTimeMs = processorTimeMs
                };
            }
        }

        public void RecordItemMetrics(string nodeId, long itemsProcessed, long itemsEmitted)
        {
            if (_nodeMetrics.TryGetValue(nodeId, out var metrics))
            {
                _nodeMetrics[nodeId] = metrics with
                {
                    ItemsProcessed = itemsProcessed,
                    ItemsEmitted = itemsEmitted
                };
            }
        }

        public void RecordRetry(string nodeId, int retryCount, string? reason = null)
        {
            if (_nodeMetrics.TryGetValue(nodeId, out var metrics))
            {
                _nodeMetrics[nodeId] = metrics with { RetryCount = retryCount };
            }
        }

        public void RecordPerformanceMetrics(string nodeId, double throughputItemsPerSec, double averageItemProcessingMs)
        {
            if (_nodeMetrics.TryGetValue(nodeId, out var metrics))
            {
                _nodeMetrics[nodeId] = metrics with
                {
                    ThroughputItemsPerSec = throughputItemsPerSec,
                    AverageItemProcessingMs = averageItemProcessingMs
                };
            }
        }

        public IReadOnlyList<INodeMetrics> GetNodeMetrics()
        {
            return [.. _nodeMetrics.Values.Cast<INodeMetrics>()];
        }

        public INodeMetrics? GetNodeMetrics(string nodeId)
        {
            return _nodeMetrics.TryGetValue(nodeId, out var metrics) ? metrics : null;
        }

        public IPipelineMetrics CreatePipelineMetrics(string pipelineName, Guid runId, DateTimeOffset startTime, DateTimeOffset? endTime, bool success,
            Exception? exception = null)
        {
            long? duration = null;
            if (endTime.HasValue)
            {
                duration = (long)(endTime.Value - startTime).TotalMilliseconds;
            }

            var totalItemsProcessed = _nodeMetrics.Values.Sum(m => m.ItemsProcessed);
            return new PipelineMetrics(
                pipelineName,
                runId,
                startTime,
                endTime,
                duration,
                success,
                totalItemsProcessed,
                GetNodeMetrics(),
                exception);
        }
    }

    #endregion
}