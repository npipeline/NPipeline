using System.Collections.Immutable;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.Lineage;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Lineage.Tests;

public class DiLineageFactoryTests
{
    [Fact]
    public void Constructor_WithValidServiceProvider_ShouldInitialize()
    {
        // Arrange
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        // Act
        var factory = new DiLineageFactory(serviceProvider);

        // Assert
        factory.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullServiceProvider_ShouldThrowArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new DiLineageFactory(null!));
    }

    [Fact]
    public void CreateLineageSink_WithRegisteredSink_ShouldReturnSink()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<TestLineageSink>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreateLineageSink(typeof(TestLineageSink));

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestLineageSink>();
    }

    [Fact]
    public void CreateLineageSink_WithUnregisteredSink_ShouldCreateViaActivatorUtilities()
    {
        // Arrange
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreateLineageSink(typeof(TestLineageSink));

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestLineageSink>();
    }

    [Fact]
    public void CreateLineageSink_WithNullType_ShouldThrowArgumentNullException()
    {
        // Arrange
        var serviceProvider = new ServiceCollection().BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => factory.CreateLineageSink(null!));
    }

    [Fact]
    public void CreateLineageSink_WithNonSinkType_ShouldReturnNull()
    {
        // Arrange
        var serviceProvider = new ServiceCollection().BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreateLineageSink(typeof(string));

        // Assert
        sink.Should().BeNull();
    }

    [Fact]
    public void CreateLineageSink_WithSinkRequiringDependencies_ShouldResolveDependencies()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ITestDependency, TestDependency>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreateLineageSink(typeof(DependentLineageSink));

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<DependentLineageSink>();
        ((DependentLineageSink)sink!).Dependency.Should().NotBeNull();
    }

    [Fact]
    public void CreateLineageSink_WithUnresolvableDependencies_ShouldReturnNull()
    {
        // Arrange
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreateLineageSink(typeof(DependentLineageSink));

        // Assert
        sink.Should().BeNull();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithRegisteredSink_ShouldReturnSink()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<TestPipelineLineageSink>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreatePipelineLineageSink(typeof(TestPipelineLineageSink));

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestPipelineLineageSink>();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithUnregisteredSink_ShouldCreateViaActivatorUtilities()
    {
        // Arrange
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreatePipelineLineageSink(typeof(TestPipelineLineageSink));

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestPipelineLineageSink>();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithNullType_ShouldThrowArgumentNullException()
    {
        // Arrange
        var serviceProvider = new ServiceCollection().BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => factory.CreatePipelineLineageSink(null!));
    }

    [Fact]
    public void CreatePipelineLineageSink_WithNonSinkType_ShouldReturnNull()
    {
        // Arrange
        var serviceProvider = new ServiceCollection().BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreatePipelineLineageSink(typeof(string));

        // Assert
        sink.Should().BeNull();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithSinkRequiringDependencies_ShouldResolveDependencies()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ITestDependency, TestDependency>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreatePipelineLineageSink(typeof(DependentPipelineLineageSink));

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<DependentPipelineLineageSink>();
        ((DependentPipelineLineageSink)sink!).Dependency.Should().NotBeNull();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithUnresolvableDependencies_ShouldReturnNull()
    {
        // Arrange
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreatePipelineLineageSink(typeof(DependentPipelineLineageSink));

        // Assert
        sink.Should().BeNull();
    }

    [Fact]
    public void ResolvePipelineLineageSinkProvider_WithRegisteredProvider_ShouldReturnProvider()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<IPipelineLineageSinkProvider, TestPipelineLineageSinkProvider>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var provider = factory.ResolvePipelineLineageSinkProvider();

        // Assert
        provider.Should().NotBeNull();
        provider.Should().BeOfType<TestPipelineLineageSinkProvider>();
    }

    [Fact]
    public void ResolvePipelineLineageSinkProvider_WithNoRegisteredProvider_ShouldReturnNull()
    {
        // Arrange
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var provider = factory.ResolvePipelineLineageSinkProvider();

        // Assert
        provider.Should().BeNull();
    }

    [Fact]
    public void ResolvePipelineLineageSinkProvider_WithScopedRegistration_ShouldReturnProvider()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddScoped<IPipelineLineageSinkProvider, TestPipelineLineageSinkProvider>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var provider = factory.ResolvePipelineLineageSinkProvider();

        // Assert
        provider.Should().NotBeNull();
        provider.Should().BeOfType<TestPipelineLineageSinkProvider>();
    }

    [Fact]
    public void ResolveLineageCollector_WithRegisteredCollector_ShouldReturnCollector()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ILineageCollector, TestLineageCollector>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var collector = factory.ResolveLineageCollector();

        // Assert
        collector.Should().NotBeNull();
        collector.Should().BeOfType<TestLineageCollector>();
    }

    [Fact]
    public void ResolveLineageCollector_WithNoRegisteredCollector_ShouldReturnNull()
    {
        // Arrange
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var collector = factory.ResolveLineageCollector();

        // Assert
        collector.Should().BeNull();
    }

    [Fact]
    public void ResolveLineageCollector_WithDefaultLineageCollector_ShouldReturnCollector()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ILineageCollector, LineageCollector>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var collector = factory.ResolveLineageCollector();

        // Assert
        collector.Should().NotBeNull();
        collector.Should().BeOfType<LineageCollector>();
    }

    [Fact]
    public void CreateLineageSink_WithThrowingConstructor_ShouldReturnNull()
    {
        // Arrange
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreateLineageSink(typeof(ThrowingLineageSink));

        // Assert
        sink.Should().BeNull();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithThrowingConstructor_ShouldReturnNull()
    {
        // Arrange
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreatePipelineLineageSink(typeof(ThrowingPipelineLineageSink));

        // Assert
        sink.Should().BeNull();
    }

    [Fact]
    public void CreateLineageSink_WithMultipleRegistrations_ShouldReturnLast()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ILineageSink, TestLineageSink>();
        services.AddSingleton<ILineageSink, TestLineageSink2>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreateLineageSink(typeof(ILineageSink));

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestLineageSink2>(); // Last registration wins
    }

    [Fact]
    public void CreatePipelineLineageSink_WithMultipleRegistrations_ShouldReturnLast()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<IPipelineLineageSink, TestPipelineLineageSink>();
        services.AddSingleton<IPipelineLineageSink, TestPipelineLineageSink2>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiLineageFactory(serviceProvider);

        // Act
        var sink = factory.CreatePipelineLineageSink(typeof(IPipelineLineageSink));

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestPipelineLineageSink2>(); // Last registration wins
    }

    // Test implementations
    private sealed class TestLineageSink : ILineageSink
    {
        public Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestLineageSink2 : ILineageSink
    {
        public Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestPipelineLineageSink : IPipelineLineageSink
    {
        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestPipelineLineageSink2 : IPipelineLineageSink
    {
        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private interface ITestDependency
    {
    }

    private sealed class TestDependency : ITestDependency
    {
    }

    private sealed class DependentLineageSink : ILineageSink
    {
        public DependentLineageSink(ITestDependency dependency)
        {
            Dependency = dependency;
        }

        public ITestDependency Dependency { get; }

        public Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class DependentPipelineLineageSink : IPipelineLineageSink
    {
        public DependentPipelineLineageSink(ITestDependency dependency)
        {
            Dependency = dependency;
        }

        public ITestDependency Dependency { get; }

        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestPipelineLineageSinkProvider : IPipelineLineageSinkProvider
    {
        public IPipelineLineageSink? Create(PipelineContext context)
        {
            return new TestPipelineLineageSink();
        }
    }

    private sealed class TestLineageCollector : ILineageCollector
    {
        public LineagePacket<T> CreateLineagePacket<T>(T item, string sourceNodeId)
        {
            return new LineagePacket<T>(item, Guid.NewGuid(), ImmutableList.Create(sourceNodeId));
        }

        public void RecordHop(Guid lineageId, LineageHop hop)
        {
        }

        public bool ShouldCollectLineage(Guid lineageId, LineageOptions? options)
        {
            return true;
        }

        public LineageInfo? GetLineageInfo(Guid lineageId)
        {
            return null;
        }

        public IReadOnlyList<LineageInfo> GetAllLineageInfo()
        {
            return [];
        }

        public void Clear()
        {
        }
    }

    private sealed class ThrowingLineageSink : ILineageSink
    {
        public ThrowingLineageSink()
        {
            throw new InvalidOperationException("Constructor throws");
        }

        public Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingPipelineLineageSink : IPipelineLineageSink
    {
        public ThrowingPipelineLineageSink()
        {
            throw new InvalidOperationException("Constructor throws");
        }

        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
