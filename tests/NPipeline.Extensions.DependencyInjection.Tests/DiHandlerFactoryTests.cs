using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.DependencyInjection.Tests;

public sealed class DiHandlerFactoryTests
{
    [Fact]
    public void CallerOwnsCreatedInstance_WithContainerResolvedSink_ReturnsFalse()
    {
        // Arrange - the container tracks what it resolves, so the run must leave it alone.
        var services = new ServiceCollection();
        services.AddSingleton<DisposableDeadLetterSink>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiHandlerFactory(serviceProvider);

        // Act
        var sink = factory.CreateDeadLetterSink(typeof(DisposableDeadLetterSink));

        // Assert
        _ = sink.Should().NotBeNull();
        factory.CallerOwnsCreatedInstance(sink!).Should().BeFalse("the container tracks the instance it resolved");
    }

    [Fact]
    public void CallerOwnsCreatedInstance_WithActivatorCreatedSink_ReturnsTrue()
    {
        // Arrange - the factory constructed the instance itself, so the caller owns it.
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiHandlerFactory(serviceProvider);

        // Act
        var sink = factory.CreateDeadLetterSink(typeof(DisposableDeadLetterSink));

        // Assert
        _ = sink.Should().NotBeNull();
        factory.CallerOwnsCreatedInstance(sink!).Should().BeTrue("the factory constructed the instance itself");
    }

    [Fact]
    public void CallerOwnsCreatedInstance_WithUnrelatedInstance_ReturnsTrue()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<DisposableDeadLetterSink>();
        var serviceProvider = services.BuildServiceProvider();
        var factory = new DiHandlerFactory(serviceProvider);
        _ = factory.CreateDeadLetterSink(typeof(DisposableDeadLetterSink)); // resolve the container-owned instance

        // Act
        var unrelated = new DisposableDeadLetterSink();

        // Assert
        factory.CallerOwnsCreatedInstance(unrelated).Should().BeTrue("an instance the factory never handed out is the caller's");
    }

    private sealed class DisposableDeadLetterSink : IDeadLetterSink, IAsyncDisposable
    {
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }
}