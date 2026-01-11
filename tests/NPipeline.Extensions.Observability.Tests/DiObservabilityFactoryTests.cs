using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Comprehensive tests for <see cref="DiObservabilityFactory" />.
/// </summary>
public sealed class DiObservabilityFactoryTests
{
    #region Helper Classes

    private sealed class CustomObservabilityCollector : IObservabilityCollector
    {
        public void RecordNodeStart(string nodeId, DateTimeOffset timestamp, int? threadId = null, long? initialMemoryMb = null)
        {
            // Custom implementation
        }

        public void RecordNodeEnd(string nodeId, DateTimeOffset timestamp, bool success, Exception? exception = null, long? peakMemoryMb = null,
            long? processorTimeMs = null)
        {
            // Custom implementation
        }

        public void RecordItemMetrics(string nodeId, long itemsProcessed, long itemsEmitted)
        {
            // Custom implementation
        }

        public void RecordRetry(string nodeId, int retryCount, string? reason = null)
        {
            // Custom implementation
        }

        public void RecordPerformanceMetrics(string nodeId, double throughputItemsPerSec, double averageItemProcessingMs)
        {
            // Custom implementation
        }

        public IReadOnlyList<INodeMetrics> GetNodeMetrics()
        {
            return [];
        }

        public INodeMetrics? GetNodeMetrics(string _)
        {
            return null;
        }

        public IPipelineMetrics CreatePipelineMetrics(string pipelineName, Guid runId, DateTimeOffset startTime, DateTimeOffset? endTime, bool success,
            Exception? exception = null)
        {
            throw new NotImplementedException();
        }

        public Task EmitMetricsAsync(string pipelineName, Guid runId, DateTimeOffset startTime, DateTimeOffset? endTime, bool success,
            Exception? exception = null, CancellationToken cancellationToken = default)
        {
            // Custom implementation - no-op for test
            return Task.CompletedTask;
        }
    }

    #endregion

    #region Constructor Tests

    [Fact]
    public void Constructor_WithNullServiceProvider_ShouldThrowArgumentNullException()
    {
        // Arrange
        IServiceProvider serviceProvider = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new DiObservabilityFactory(serviceProvider));
    }

    [Fact]
    public void Constructor_WithValidServiceProvider_ShouldCreateInstance()
    {
        // Arrange
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        // Act
        var factory = new DiObservabilityFactory(serviceProvider);

        // Assert
        Assert.NotNull(factory);
    }

    #endregion

    #region ResolveObservabilityCollector Tests

    [Fact]
    public void ResolveObservabilityCollector_WhenNotRegistered_ShouldReturnNull()
    {
        // Arrange
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiObservabilityFactory(serviceProvider);

        // Act
        var collector = factory.ResolveObservabilityCollector();

        // Assert
        Assert.Null(collector);
    }

    [Fact]
    public void ResolveObservabilityCollector_WhenRegistered_ShouldReturnInstance()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddSingleton<IObservabilityFactory, DiObservabilityFactory>();
        _ = services.AddSingleton<IObservabilityCollector, ObservabilityCollector>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiObservabilityFactory(serviceProvider);

        // Act
        var collector = factory.ResolveObservabilityCollector();

        // Assert
        Assert.NotNull(collector);
        _ = Assert.IsType<ObservabilityCollector>(collector);
    }

    [Fact]
    public void ResolveObservabilityCollector_WhenRegisteredAsTransient_ShouldReturnInstance()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddSingleton<IObservabilityFactory, DiObservabilityFactory>();
        _ = services.AddTransient<IObservabilityCollector, ObservabilityCollector>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiObservabilityFactory(serviceProvider);

        // Act
        var collector = factory.ResolveObservabilityCollector();

        // Assert
        Assert.NotNull(collector);
        _ = Assert.IsType<ObservabilityCollector>(collector);
    }

    [Fact]
    public void ResolveObservabilityCollector_WhenRegisteredAsScoped_ShouldReturnInstance()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddSingleton<IObservabilityFactory, DiObservabilityFactory>();
        _ = services.AddScoped<IObservabilityCollector, ObservabilityCollector>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiObservabilityFactory(serviceProvider);

        // Act
        var collector = factory.ResolveObservabilityCollector();

        // Assert
        Assert.NotNull(collector);
        _ = Assert.IsType<ObservabilityCollector>(collector);
    }

    [Fact]
    public void ResolveObservabilityCollector_WithCustomImplementation_ShouldReturnInstance()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddSingleton<IObservabilityFactory, DiObservabilityFactory>();
        _ = services.AddSingleton<IObservabilityCollector, CustomObservabilityCollector>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiObservabilityFactory(serviceProvider);

        // Act
        var collector = factory.ResolveObservabilityCollector();

        // Assert
        Assert.NotNull(collector);
        _ = Assert.IsType<CustomObservabilityCollector>(collector);
    }

    [Fact]
    public void ResolveObservabilityCollector_MultipleCalls_ShouldReturnSameInstance()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddSingleton<IObservabilityFactory, DiObservabilityFactory>();
        _ = services.AddSingleton<IObservabilityCollector, ObservabilityCollector>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiObservabilityFactory(serviceProvider);

        // Act
        var collector1 = factory.ResolveObservabilityCollector();
        var collector2 = factory.ResolveObservabilityCollector();

        // Assert
        Assert.NotNull(collector1);
        Assert.NotNull(collector2);
        Assert.Same(collector1, collector2); // Singleton should return same instance
    }

    [Fact]
    public void ResolveObservabilityCollector_WithTransient_ShouldReturnDifferentInstances()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddSingleton<IObservabilityFactory, DiObservabilityFactory>();
        _ = services.AddTransient<IObservabilityCollector, ObservabilityCollector>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiObservabilityFactory(serviceProvider);

        // Act
        var collector1 = factory.ResolveObservabilityCollector();
        var collector2 = factory.ResolveObservabilityCollector();

        // Assert
        Assert.NotNull(collector1);
        Assert.NotNull(collector2);
        Assert.NotSame(collector1, collector2); // Transient should return different instances
    }

    [Fact]
    public void ResolveObservabilityCollector_WithFactoryDelegate_ShouldReturnInstance()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddSingleton<IObservabilityFactory, DiObservabilityFactory>();
        _ = services.AddSingleton<IObservabilityCollector>(_ => new CustomObservabilityCollector());
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiObservabilityFactory(serviceProvider);

        // Act
        var collector = factory.ResolveObservabilityCollector();

        // Assert
        Assert.NotNull(collector);
        _ = Assert.IsType<CustomObservabilityCollector>(collector);
    }

    #endregion
}
