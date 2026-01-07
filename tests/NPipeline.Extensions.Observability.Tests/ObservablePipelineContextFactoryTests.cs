using Microsoft.Extensions.DependencyInjection;
using NPipeline.Execution;
using NPipeline.Extensions.Observability;
using NPipeline.Observability.DependencyInjection;
using Xunit;

namespace NPipeline.Observability.Tests;

/// <summary>
///     Tests for the <see cref="ObservablePipelineContextFactory"/> and related DI registrations.
/// </summary>
public sealed class ObservablePipelineContextFactoryTests
{
    [Fact]
    public async Task Create_ReturnsContextWithExecutionObserverSet()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();
        var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var factory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();

        // Act
        await using var context = factory.Create();

        // Assert
        Assert.NotNull(context);
        Assert.NotNull(context.ExecutionObserver);
        Assert.IsType<MetricsCollectingExecutionObserver>(context.ExecutionObserver);
    }

    [Fact]
    public async Task Create_WithCancellationToken_ReturnsContextWithCancellationToken()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();
        var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var factory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();
        using var cts = new CancellationTokenSource();

        // Act
        await using var context = factory.Create(cts.Token);

        // Assert
        Assert.Equal(cts.Token, context.CancellationToken);
        Assert.IsType<MetricsCollectingExecutionObserver>(context.ExecutionObserver);
    }

    [Fact]
    public void IExecutionObserver_IsRegisteredWithCollector()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();
        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        // Act
        var observer = scope.ServiceProvider.GetRequiredService<IExecutionObserver>();
        var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

        // Assert
        Assert.IsType<MetricsCollectingExecutionObserver>(observer);

        // Verify the observer is connected to the collector by recording an event
        var startEvent = new NodeExecutionStarted("test-node", "TestNode", DateTimeOffset.UtcNow);
        observer.OnNodeStarted(startEvent);

        var metrics = collector.GetNodeMetrics("test-node");
        Assert.NotNull(metrics);
        Assert.Equal("test-node", metrics.NodeId);
    }

    [Fact]
    public void ScopedRegistrations_CreateNewInstancesPerScope()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddNPipelineObservability();
        var provider = services.BuildServiceProvider();

        IObservabilityCollector collector1;
        IObservabilityCollector collector2;

        // Act
        using (var scope1 = provider.CreateScope())
        {
            collector1 = scope1.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        }

        using (var scope2 = provider.CreateScope())
        {
            collector2 = scope2.ServiceProvider.GetRequiredService<IObservabilityCollector>();
        }

        // Assert
        Assert.NotSame(collector1, collector2);
    }
}
