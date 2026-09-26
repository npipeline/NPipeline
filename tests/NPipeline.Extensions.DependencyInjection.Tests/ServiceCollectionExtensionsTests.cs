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
    public void AddNPipeline_WhenSameAssemblyAddedTwice_ShouldDeduplicatePipelineDefinitionRegistry()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipeline(Assembly.GetExecutingAssembly(), Assembly.GetExecutingAssembly());
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var registry = serviceProvider.GetRequiredService<PipelineDefinitionRegistry>();
        registry.DefinitionTypes.Should().ContainSingle(t => t == typeof(TestPipelineDefinition));
        registry.DefinitionTypes.Should().ContainSingle(t => t == typeof(ScopedPipelineDefinition));
        registry.DefinitionTypes.Should().ContainSingle(t => t == typeof(DisposablePipelineDefinition));
    }

    [Fact]
    public void AddNPipeline_WithDuplicateAddPipelineCalls_ShouldDeduplicatePipelineDefinitionRegistry()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipeline(builder => builder
            .AddPipeline<TestPipelineDefinition>()
            .AddPipeline<TestPipelineDefinition>());

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var registry = serviceProvider.GetRequiredService<PipelineDefinitionRegistry>();
        registry.DefinitionTypes.Should().ContainSingle();
        registry.DefinitionTypes[0].Should().Be<TestPipelineDefinition>();
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
    public async Task RunPipelineAsync_DisposesTheContextItCreates()
    {
        // Arrange - a node registers a resource with the run's context, which only disposing the context releases.
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        var descriptor = services.Single(d => d.ServiceType == typeof(RegisteringSinkNode));
        services.Remove(descriptor);
        var sink = new RegisteringSinkNode();
        services.AddSingleton(sink);

        var serviceProvider = services.BuildServiceProvider();

        // Act
        await serviceProvider.RunPipelineAsync<RegisteringPipelineDefinition>();

        // Assert
        sink.Tracker.Disposed.Should().BeTrue("RunPipelineAsync creates the context, so it must dispose it");
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
    public void AddNPipeline_ResolvesNullLineage_ByDefault()
    {
        var services = new ServiceCollection();
        services.AddNPipeline();
        var sp = services.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var lineage = scope.ServiceProvider.GetRequiredService<ILineage>();
        Assert.IsType<NullLineage>(lineage);
    }

    [Fact]
    public void AddNPipeline_ResolvesNullObservabilitySurface_ByDefault()
    {
        var services = new ServiceCollection();
        services.AddNPipeline();
        var sp = services.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var surface = scope.ServiceProvider.GetRequiredService<IObservabilitySurface>();
        Assert.IsType<NullObservabilitySurface>(surface);
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

    [Fact]
    public async Task SingletonNode_IsNotDisposedByRunner()
    {
        // Arrange
        DisposableSink.DisposeCount = 0;
        var services = new ServiceCollection();
        services.AddSingleton<DisposableSink>();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        await using var sp = services.BuildServiceProvider();
        await using var scope = sp.CreateAsyncScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();

        // Act
        await runner.RunAsync<DisposablePipelineDefinition>(new PipelineContext());

        // Assert - the container owns the singleton, so the run must leave it alone
        DisposableSink.DisposeCount.Should().Be(0);
    }

    [Fact]
    public async Task UnregisteredNode_IsDisposed_UnderRunPipelineAsync()
    {
        // Arrange
        DisposableSink.DisposeCount = 0;
        var services = new ServiceCollection();
        services.AddNPipeline();
        await using var sp = services.BuildServiceProvider();

        // Act
        await sp.RunPipelineAsync<DisposablePipelineDefinition>();

        // Assert - the run constructed the instance, so the run disposes it
        DisposableSink.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task NodeInstanceRegisteredUnderTwoIds_IsDisposedOnce()
    {
        // Arrange
        DisposableSink.DisposeCount = 0;
        var services = new ServiceCollection();
        services.AddNPipeline();
        await using var sp = services.BuildServiceProvider();
        await using var scope = sp.CreateAsyncScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
        var shared = new DisposableSink();
        var context = new PipelineContext();
        context.SetSourceData<string>(["a", "b"], "s");
        context.NodeEnvironment.PreconfiguredNodeInstances["s"] = new InMemorySourceNode<string>();
        context.NodeEnvironment.PreconfiguredNodeInstances["t"] = shared;
        context.NodeEnvironment.PreconfiguredNodeInstances["u"] = shared;

        // Act
        await runner.RunAsync<ThreeNodeDisposablePipelineDefinition>(context);

        // Assert
        DisposableSink.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task NodeAddedWithAddTap_IsDisposed_UnderRunPipelineAsync()
    {
        // Arrange
        DisposableSink.DisposeCount = 0;
        var services = new ServiceCollection();
        services.AddNPipeline();
        await using var sp = services.BuildServiceProvider();

        // Act - the AddTap sink is a builder-created instance, so the run owns and disposes it
        await sp.RunPipelineAsync<TapPipelineDefinition>();

        // Assert
        DisposableSink.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task DeadLetterSinkResolvedFromDI_IsDisposedExactlyOnceByTheContainer()
    {
        // Arrange
        DisposableDeadLetterSink.DisposeCount = 0;
        var services = new ServiceCollection();
        services.AddNPipeline();
        services.AddScoped<DisposableDeadLetterSink>();
        await using var sp = services.BuildServiceProvider();

        // Act - RunPipelineAsync builds the context with the DI factories and its own scope
        await sp.RunPipelineAsync<DeadLetterSinkTypePipelineDefinition>();

        // Assert - the run leaves the container-owned sink alone, and the container disposes it exactly once
        DisposableDeadLetterSink.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task DeadLetterSinkNotRegisteredInDI_IsDisposedByTheRun()
    {
        // Arrange - the sink type is not registered, so the DI factory constructs it itself and the run owns it.
        DisposableDeadLetterSink.DisposeCount = 0;
        var services = new ServiceCollection();
        services.AddNPipeline();
        await using var sp = services.BuildServiceProvider();

        // Act - RunPipelineAsync creates and disposes its own scope and context
        await sp.RunPipelineAsync<DeadLetterSinkTypePipelineDefinition>();

        // Assert - the container never saw the instance, so it must be the run that releases it
        DisposableDeadLetterSink.DisposeCount.Should().Be(1);
    }

    private sealed class TestSinkNode : SinkNode<string>
    {
        public bool WasCalled { get; private set; }

        public override Task ConsumeAsync(IDataStream<string> input, PipelineContext context,
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

    private sealed class RegisteringPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<InMemorySourceNode<string>, string>("source");
            var sink = builder.AddSink<RegisteringSinkNode, string>("sink");
            builder.Connect(source, sink);
        }
    }

    public sealed class DisposalTracker : IAsyncDisposable
    {
        public bool Disposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    public sealed class RegisteringSinkNode : SinkNode<string>
    {
        public DisposalTracker Tracker { get; } = new();

        public override async Task ConsumeAsync(IDataStream<string> input, PipelineContext context, CancellationToken cancellationToken)
        {
            context.RegisterForDisposal(Tracker);

            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
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

        public override Task ConsumeAsync(IDataStream<string> input, PipelineContext context,
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
        public Task RunAsync<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new() => Task.CompletedTask;

        public Task RunAsync(IPipelineDefinition definition, PipelineContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    // DI-owned disposal detection
    private sealed class DisposableSink : SinkNode<string>, IAsyncDisposable
    {
        public static int DisposeCount;

        public async ValueTask DisposeAsync() => Interlocked.Increment(ref DisposeCount);

        public override Task ConsumeAsync(IDataStream<string> input, PipelineContext context,
            CancellationToken cancellationToken) =>
            Task.CompletedTask;
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

    private sealed class ThreeNodeDisposablePipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<InMemorySourceNode<string>, string>("s");
            var first = builder.AddSink<DisposableSink, string>("t");
            var second = builder.AddSink<DisposableSink, string>("u");
            _ = builder.Connect(source, first);
            _ = builder.Connect(source, second);
        }
    }

    private sealed class TapPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ConstantSource, string>("s");
            var tap = builder.AddTap(new DisposableSink(), "tap");
            var sink = builder.AddSink<TestSinkNode, string>("t");
            _ = builder.Connect(source, tap).Connect(tap, sink);
        }
    }

    private sealed class ConstantSource : SourceNode<string>
    {
        public override IDataStream<string> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<string>(["a", "b"], "constant");
    }

    private sealed class DisposableDeadLetterSink : IDeadLetterSink, IAsyncDisposable
    {
        public static int DisposeCount;

        public ValueTask DisposeAsync()
        {
            _ = Interlocked.Increment(ref DisposeCount);
            return ValueTask.CompletedTask;
        }

        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class DeadLetterSinkTypePipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ConstantSource, string>("s");
            var sink = builder.AddSink<TestSinkNode, string>("t");
            _ = builder.Connect(source, sink);
            _ = builder.AddDeadLetterSink<DisposableDeadLetterSink>();
        }
    }
}
