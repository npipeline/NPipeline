// ReSharper disable ClassNeverInstantiated.Local

using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.DependencyInjection.Tests;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddNPipeline_WithFluentConfiguration_ShouldRegisterCoreServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipeline(builder => builder
            .AddNode<InMemorySourceNode<string>>()
            .AddNode<TestSinkNode>());

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        serviceProvider.GetService<IPipelineFactory>().Should().NotBeNull();
        serviceProvider.GetService<IPipelineRunner>().Should().NotBeNull();
        serviceProvider.GetService<INodeFactory>().Should().NotBeNull();
        serviceProvider.GetService<InMemorySourceNode<string>>().Should().NotBeNull();
        serviceProvider.GetService<TestSinkNode>().Should().NotBeNull();
    }

    [Fact]
    public void AddNPipeline_WithFluentConfiguration_ShouldSupportChaining()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipeline(builder => builder
            .AddNode<InMemorySourceNode<string>>()
            .AddNode<TestSinkNode>()
            .AddPipeline<TestPipelineDefinition>()
            .ScanAssemblies(typeof(InMemorySourceNode<>).Assembly));

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        serviceProvider.GetService<TestPipelineDefinition>().Should().NotBeNull();
        serviceProvider.GetService<InMemorySourceNode<string>>().Should().NotBeNull();
        serviceProvider.GetService<TestSinkNode>().Should().NotBeNull();
    }

    [Fact]
    public void AddNPipeline_WithFluentConfiguration_ShouldSupportCustomServiceLifetime()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipeline(builder => builder
            .AddNode<InMemorySourceNode<string>>(ServiceLifetime.Singleton)
            .AddNode<TestSinkNode>(ServiceLifetime.Singleton));

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        using var scope1 = serviceProvider.CreateScope();
        using var scope2 = serviceProvider.CreateScope();

        var source1 = scope1.ServiceProvider.GetRequiredService<InMemorySourceNode<string>>();
        var source2 = scope2.ServiceProvider.GetRequiredService<InMemorySourceNode<string>>();

        ReferenceEquals(source1, source2).Should().BeTrue();
    }

    [Fact]
    public async Task AddNPipeline_WithFluentConfiguration_ShouldWorkWithRunPipelineAsync()
    {
        // Arrange
        var services = new ServiceCollection();
        var sink = new TestSinkNode();
        services.AddSingleton(sink);

        services.AddNPipeline(builder => builder
            .AddNode<InMemorySourceNode<string>>()
            .AddNode<TestSinkNode>());

        var serviceProvider = services.BuildServiceProvider();

        // Act
        await serviceProvider.RunPipelineAsync<TestPipelineDefinition>();

        // Assert
        sink.WasCalled.Should().BeTrue();
    }

    [Fact]
    public void AddNPipeline_ShouldRegister_CoreServicesAndComponents()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipeline(Assembly.GetExecutingAssembly(), typeof(InMemorySourceNode<>).Assembly);
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        serviceProvider.GetService<IPipelineFactory>().Should().NotBeNull();
        serviceProvider.GetService<IPipelineRunner>().Should().NotBeNull();
        serviceProvider.GetService<INodeFactory>().Should().NotBeNull();
        serviceProvider.GetService<TestPipelineDefinition>().Should().NotBeNull();
        serviceProvider.GetService<InMemorySourceNode<int>>().Should().NotBeNull(); // StringSourceNode is in NPipeline.Extensions.Testing
        serviceProvider.GetService<TestSinkNode>().Should().NotBeNull();
    }

    [Fact]
    public async Task RunPipelineAsync_ShouldExecutePipeline()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        // Replace the transient registration with a singleton to access the node's state.
        var descriptor = services.Single(d => d.ServiceType == typeof(TestSinkNode));
        services.Remove(descriptor);
        var sink = new TestSinkNode();
        services.AddSingleton(sink);

        var serviceProvider = services.BuildServiceProvider();

        // Act
        await serviceProvider.RunPipelineAsync<TestPipelineDefinition>();

        // Assert
        sink.WasCalled.Should().BeTrue();
    }

    [Fact]
    public void Lifetimes_ShouldBeScoped_ForRunnerAndFactories()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();

        // Act
        using var scope1 = sp.CreateScope();
        using var scope2 = sp.CreateScope();

        var r1 = scope1.ServiceProvider.GetRequiredService<IPipelineRunner>();
        var r2 = scope2.ServiceProvider.GetRequiredService<IPipelineRunner>();

        var nf1 = scope1.ServiceProvider.GetRequiredService<INodeFactory>();
        var nf2 = scope2.ServiceProvider.GetRequiredService<INodeFactory>();

        // Test the three new focused factory interfaces instead of the old IHandlerFactory
        var ehf1 = scope1.ServiceProvider.GetRequiredService<IErrorHandlerFactory>();
        var ehf2 = scope2.ServiceProvider.GetRequiredService<IErrorHandlerFactory>();

        var lf1 = scope1.ServiceProvider.GetRequiredService<ILineageFactory>();
        var lf2 = scope2.ServiceProvider.GetRequiredService<ILineageFactory>();

        var of1 = scope1.ServiceProvider.GetRequiredService<IObservabilityFactory>();
        var of2 = scope2.ServiceProvider.GetRequiredService<IObservabilityFactory>();

        // Assert (scoped instances differ across scopes)
        ReferenceEquals(r1, r2).Should().BeFalse();
        ReferenceEquals(nf1, nf2).Should().BeFalse();
        ReferenceEquals(ehf1, ehf2).Should().BeFalse();
        ReferenceEquals(lf1, lf2).Should().BeFalse();
        ReferenceEquals(of1, of2).Should().BeFalse();
    }

    [Fact]
    public async Task PerRunScope_ShouldProvideDistinctScopedDependencies()
    {
        // Arrange
        TestScopedSink.Reset();
        var services = new ServiceCollection();
        services.AddScoped<ScopedService>();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();

        // Act
        await sp.RunPipelineAsync<ScopedPipelineDefinition>();
        await sp.RunPipelineAsync<ScopedPipelineDefinition>();

        // Assert
        TestScopedSink.InstanceIds.Should().HaveCount(2);
        TestScopedSink.InstanceIds[0].Should().NotBe(TestScopedSink.InstanceIds[1]);
    }

    [Fact]
    public void TryAdd_ShouldNotOverrideExistingRegistrations()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<IPipelineRunner, FakeRunner>();

        // Act
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();

        // Assert
        var runner = sp.GetRequiredService<IPipelineRunner>();
        runner.Should().BeOfType<FakeRunner>();
    }

    [Fact]
    public async Task DI_OwnedNodes_ShouldBeDisposedExactlyOnce()
    {
        // Arrange
        DisposableSink.DisposeCount = 0;
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();

        // Act
        await sp.RunPipelineAsync<DisposablePipelineDefinition>();

        // Assert
        DisposableSink.DisposeCount.Should().Be(1);
    }

    private sealed class TestSinkNode : SinkNode<string>
    {
        public bool WasCalled { get; private set; }

        public override Task ExecuteAsync(IDataPipe<string> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            WasCalled = true;
            return Task.CompletedTask;
        }
    }

    private sealed class TestPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<InMemorySourceNode<string>, string>("source");
            var sink = builder.AddSink<TestSinkNode, string>("sink");
            builder.Connect(source, sink);
        }
    }

    // Scoped dependency test
    public sealed class ScopedService
    {
        public Guid Id { get; } = Guid.NewGuid();
    }

    private sealed class TestScopedSink(ScopedService svc) : SinkNode<string>
    {
        public static List<Guid> InstanceIds { get; } = [];

        public static void Reset()
        {
            InstanceIds.Clear();
        }

        public override Task ExecuteAsync(IDataPipe<string> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            InstanceIds.Add(svc.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class ScopedPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<InMemorySourceNode<string>, string>("s");
            var sink = builder.AddSink<TestScopedSink, string>("t");
            builder.Connect(source, sink);
        }
    }

    // TryAdd behavior
    private sealed class FakeRunner : IPipelineRunner
    {
        public Task RunAsync<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
        {
            return Task.CompletedTask;
        }
    }

    // DI-owned disposal detection
    private sealed class DisposableSink : SinkNode<string>
    {
        public static int DisposeCount;

        public override Task ExecuteAsync(IDataPipe<string> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public override async ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref DisposeCount);
            await base.DisposeAsync();
        }
    }

    private sealed class DisposablePipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<InMemorySourceNode<string>, string>("s");
            var sink = builder.AddSink<DisposableSink, string>("t");
            builder.Connect(source, sink);
        }
    }
}
