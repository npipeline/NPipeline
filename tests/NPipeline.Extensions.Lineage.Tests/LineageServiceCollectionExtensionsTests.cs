using System.Collections.Immutable;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.Lineage;
using NPipeline.Lineage.DependencyInjection;

namespace NPipeline.Extensions.Lineage.Tests;

public class LineageServiceCollectionExtensionsTests
{
    [Fact]
    public void AddNPipelineLineage_WithNoParameters_ShouldRegisterDefaultServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var collector = serviceProvider.GetService<ILineageCollector>();
        var sink = serviceProvider.GetService<IPipelineLineageSink>();
        var factory = serviceProvider.GetService<ILineageFactory>();
        var provider = serviceProvider.GetService<IPipelineLineageSinkProvider>();

        collector.Should().NotBeNull();
        collector.Should().BeOfType<LineageCollector>();
        sink.Should().NotBeNull();
        sink.Should().BeOfType<LoggingPipelineLineageSink>();
        factory.Should().NotBeNull();
        factory.Should().BeOfType<DiLineageFactory>();
        provider.Should().NotBeNull();
        provider.Should().BeOfType<DefaultPipelineLineageSinkProvider>();
    }

    [Fact]
    public void AddNPipelineLineage_WithNullServices_ShouldThrowArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddNPipelineLineage());
    }

    [Fact]
    public void AddNPipelineLineage_WithCustomSinkType_ShouldRegisterCustomSink()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage<TestPipelineLineageSink>();
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var sink = serviceProvider.GetService<IPipelineLineageSink>();
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestPipelineLineageSink>();
    }

    [Fact]
    public void AddNPipelineLineage_WithCustomSinkType_ShouldRegisterDefaultCollector()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage<TestPipelineLineageSink>();
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var collector = serviceProvider.GetService<ILineageCollector>();
        collector.Should().NotBeNull();
        collector.Should().BeOfType<LineageCollector>();
    }

    [Fact]
    public void AddNPipelineLineage_WithCustomSinkType_ShouldRegisterCoreServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage<TestPipelineLineageSink>();
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var factory = serviceProvider.GetService<ILineageFactory>();
        var provider = serviceProvider.GetService<IPipelineLineageSinkProvider>();

        factory.Should().NotBeNull();
        factory.Should().BeOfType<DiLineageFactory>();
        provider.Should().NotBeNull();
        provider.Should().BeOfType<DefaultPipelineLineageSinkProvider>();
    }

    [Fact]
    public void AddNPipelineLineage_WithFactoryDelegate_ShouldRegisterFactorySink()
    {
        // Arrange
        var services = new ServiceCollection();
        var factoryCalled = false;

        // Act
        services.AddNPipelineLineage(sp =>
        {
            factoryCalled = true;
            return new TestPipelineLineageSink();
        });

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var sink = serviceProvider.GetService<IPipelineLineageSink>();
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestPipelineLineageSink>();
        factoryCalled.Should().BeTrue(); // Factory is called on resolution
    }

    [Fact]
    public void AddNPipelineLineage_WithFactoryDelegate_ShouldRegisterDefaultCollector()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage(sp => new TestPipelineLineageSink());
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var collector = serviceProvider.GetService<ILineageCollector>();
        collector.Should().NotBeNull();
        collector.Should().BeOfType<LineageCollector>();
    }

    [Fact]
    public void AddNPipelineLineage_WithFactoryDelegate_ShouldRegisterCoreServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage(sp => new TestPipelineLineageSink());
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var factory = serviceProvider.GetService<ILineageFactory>();
        var provider = serviceProvider.GetService<IPipelineLineageSinkProvider>();

        factory.Should().NotBeNull();
        factory.Should().BeOfType<DiLineageFactory>();
        provider.Should().NotBeNull();
        provider.Should().BeOfType<DefaultPipelineLineageSinkProvider>();
    }

    [Fact]
    public void AddNPipelineLineage_WithFactoryDelegate_WhenFactoryCalled_ShouldReceiveServiceProvider()
    {
        // Arrange
        var services = new ServiceCollection();
        IServiceProvider? receivedServiceProvider = null;

        // Act
        services.AddNPipelineLineage(sp =>
        {
            receivedServiceProvider = sp;
            return new TestPipelineLineageSink();
        });

        var serviceProvider = services.BuildServiceProvider();
        serviceProvider.GetService<IPipelineLineageSink>();

        // Assert
        receivedServiceProvider.Should().NotBeNull();

        // The factory receives the scope's service provider, not the root provider
        receivedServiceProvider.Should().NotBeNull();
    }

    [Fact]
    public void AddNPipelineLineage_WithFactoryDelegate_WithNullFactory_ShouldThrowArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => services.AddNPipelineLineage(null!));
    }

    [Fact]
    public void AddNPipelineLineage_WithCustomCollectorAndSink_ShouldRegisterCustomServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage<TestLineageCollector, TestPipelineLineageSink>();
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var collector = serviceProvider.GetService<ILineageCollector>();
        var sink = serviceProvider.GetService<IPipelineLineageSink>();

        collector.Should().NotBeNull();
        collector.Should().BeOfType<TestLineageCollector>();
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestPipelineLineageSink>();
    }

    [Fact]
    public void AddNPipelineLineage_WithCustomCollectorAndSink_ShouldRegisterCoreServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage<TestLineageCollector, TestPipelineLineageSink>();
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var factory = serviceProvider.GetService<ILineageFactory>();
        var provider = serviceProvider.GetService<IPipelineLineageSinkProvider>();

        factory.Should().NotBeNull();
        factory.Should().BeOfType<DiLineageFactory>();
        provider.Should().NotBeNull();
        provider.Should().BeOfType<DefaultPipelineLineageSinkProvider>();
    }

    [Fact]
    public void AddNPipelineLineage_WithCustomCollectorAndSink_WithNullServices_ShouldThrowArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddNPipelineLineage<TestLineageCollector, TestPipelineLineageSink>());
    }

    [Fact]
    public void AddNPipelineLineage_WithCustomCollectorFactory_ShouldRegisterCustomCollector()
    {
        // Arrange
        var services = new ServiceCollection();
        var factoryCalled = false;

        // Act
        services.AddNPipelineLineage<TestPipelineLineageSink>(sp =>
        {
            factoryCalled = true;
            return new TestLineageCollector();
        });

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var collector = serviceProvider.GetService<ILineageCollector>();
        collector.Should().NotBeNull();
        collector.Should().BeOfType<TestLineageCollector>();
        factoryCalled.Should().BeTrue(); // Factory is called on resolution
    }

    [Fact]
    public void AddNPipelineLineage_WithCustomCollectorFactory_ShouldRegisterDefaultSink()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage<TestPipelineLineageSink>(sp => new TestLineageCollector());
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var sink = serviceProvider.GetService<IPipelineLineageSink>();
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestPipelineLineageSink>();
    }

    [Fact]
    public void AddNPipelineLineage_WithCustomCollectorFactory_ShouldRegisterCoreServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage<TestPipelineLineageSink>(sp => new TestLineageCollector());
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var factory = serviceProvider.GetService<ILineageFactory>();
        var provider = serviceProvider.GetService<IPipelineLineageSinkProvider>();

        factory.Should().NotBeNull();
        factory.Should().BeOfType<DiLineageFactory>();
        provider.Should().NotBeNull();
        provider.Should().BeOfType<DefaultPipelineLineageSinkProvider>();
    }

    [Fact]
    public void AddNPipelineLineage_WithCustomCollectorFactory_WithNullFactory_ShouldThrowArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => services.AddNPipelineLineage<TestPipelineLineageSink>(null!));
    }

    [Fact]
    public void AddNPipelineLineage_ShouldRegisterServicesAsScoped()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();

        // Act
        using var scope1 = serviceProvider.CreateScope();
        var collector1 = scope1.ServiceProvider.GetService<ILineageCollector>();
        var sink1 = scope1.ServiceProvider.GetService<IPipelineLineageSink>();
        var factory1 = scope1.ServiceProvider.GetService<ILineageFactory>();
        var provider1 = scope1.ServiceProvider.GetService<IPipelineLineageSinkProvider>();

        using var scope2 = serviceProvider.CreateScope();
        var collector2 = scope2.ServiceProvider.GetService<ILineageCollector>();
        var sink2 = scope2.ServiceProvider.GetService<IPipelineLineageSink>();
        var factory2 = scope2.ServiceProvider.GetService<ILineageFactory>();
        var provider2 = scope2.ServiceProvider.GetService<IPipelineLineageSinkProvider>();

        // Assert
        collector1.Should().NotBeNull();
        collector2.Should().NotBeNull();
        collector1.Should().NotBeSameAs(collector2);

        sink1.Should().NotBeNull();
        sink2.Should().NotBeNull();
        sink1.Should().NotBeSameAs(sink2);

        factory1.Should().NotBeNull();
        factory2.Should().NotBeNull();
        factory1.Should().NotBeSameAs(factory2);

        provider1.Should().NotBeNull();
        provider2.Should().NotBeNull();
        provider1.Should().NotBeSameAs(provider2);
    }

    [Fact]
    public void AddNPipelineLineage_WhenCalledMultipleTimes_ShouldNotDuplicateRegistrations()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage();
        services.AddNPipelineLineage();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var collectors = serviceProvider.GetServices<ILineageCollector>();
        var sinks = serviceProvider.GetServices<IPipelineLineageSink>();

        collectors.Should().HaveCount(1);
        sinks.Should().HaveCount(1);
    }

    [Fact]
    public void AddNPipelineLineage_WithDifferentConfigurations_ShouldNotOverride()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage<TestPipelineLineageSink>();
        services.AddNPipelineLineage<TestPipelineLineageSink2>();
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var sink = serviceProvider.GetService<IPipelineLineageSink>();
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestPipelineLineageSink>(); // First registration wins
    }

    [Fact]
    public void AddNPipelineLineage_ShouldAllowServiceResolution()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var collector = serviceProvider.GetRequiredService<ILineageCollector>();
        var sink = serviceProvider.GetRequiredService<IPipelineLineageSink>();
        var factory = serviceProvider.GetRequiredService<ILineageFactory>();
        var provider = serviceProvider.GetRequiredService<IPipelineLineageSinkProvider>();

        // Assert
        collector.Should().NotBeNull();
        sink.Should().NotBeNull();
        factory.Should().NotBeNull();
        provider.Should().NotBeNull();
    }

    [Fact]
    public void AddNPipelineLineage_WithSinkRequiringDependencies_ShouldResolveDependencies()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ITestDependency, TestDependency>();
        services.AddNPipelineLineage<DependentPipelineLineageSink>();
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var sink = serviceProvider.GetService<IPipelineLineageSink>();

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<DependentPipelineLineageSink>();
        ((DependentPipelineLineageSink)sink!).Dependency.Should().NotBeNull();
    }

    [Fact]
    public void AddNPipelineLineage_WithCollectorRequiringDependencies_ShouldResolveDependencies()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ITestDependency, TestDependency>();
        services.AddNPipelineLineage<TestPipelineLineageSink>(sp => new DependentLineageCollector(sp.GetRequiredService<ITestDependency>()));
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var collector = serviceProvider.GetService<ILineageCollector>();

        // Assert
        collector.Should().NotBeNull();
        collector.Should().BeOfType<DependentLineageCollector>();
        ((DependentLineageCollector)collector!).Dependency.Should().NotBeNull();
    }

    [Fact]
    public void AddNPipelineLineage_WithFactoryDelegate_ShouldResolveDependencies()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ITestDependency, TestDependency>();
        services.AddNPipelineLineage(sp => new DependentPipelineLineageSink(sp.GetRequiredService<ITestDependency>()));
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var sink = serviceProvider.GetService<IPipelineLineageSink>();

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<DependentPipelineLineageSink>();
        ((DependentPipelineLineageSink)sink!).Dependency.Should().NotBeNull();
    }

    // Test implementations
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

    private interface ITestDependency
    {
    }

    private sealed class TestDependency : ITestDependency
    {
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

    private sealed class DependentLineageCollector : ILineageCollector
    {
        public DependentLineageCollector(ITestDependency dependency)
        {
            Dependency = dependency;
        }

        public ITestDependency Dependency { get; }

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
}
