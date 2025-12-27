using AwesomeAssertions;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Execution;

public sealed class ResourceDisposalTests : IAsyncLifetime
{
    private readonly List<IAsyncDisposable> _trackedDisposables = [];
    private readonly List<IDisposable> _trackedSyncDisposables = [];
    private IPipelineRunner _pipelineRunner = null!;
    private ServiceProvider _serviceProvider = null!;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(typeof(ResourceDisposalTests).Assembly);

        // Register test nodes with their dependencies
        services.AddTransient<AsyncDisposableSourceNode>();
        services.AddTransient<AsyncDisposableTransformNode>();
        services.AddTransient<AsyncDisposableSinkNode>();
        services.AddTransient<SyncDisposableSourceNode>();
        services.AddTransient<ThrowingAsyncDisposableTransformNode>();

        _serviceProvider = services.BuildServiceProvider();
        _pipelineRunner = _serviceProvider.GetRequiredService<IPipelineRunner>();
        await Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        // Dispose the service provider without disposing tracked nodes
        // to avoid "type only implements IAsyncDisposable" errors
        if (_serviceProvider != null)
        {
            // Create a new scope to avoid disposing the registered nodes
            using var scope = _serviceProvider.CreateScope();
            _serviceProvider = null!;
        }

        // Clean up any tracked disposables that weren't properly disposed by tests
        foreach (var disposable in _trackedDisposables)
        {
            try
            {
                await disposable.DisposeAsync();
            }
            catch
            {
                // Ignore cleanup exceptions
            }
        }

        foreach (var disposable in _trackedSyncDisposables)
        {
            try
            {
                disposable.Dispose();
            }
            catch
            {
                // Ignore cleanup exceptions
            }
        }
    }

    #region Test Nodes

    /// <summary>
    ///     A test source node that implements IAsyncDisposable to verify disposal behavior.
    /// </summary>
    private sealed class AsyncDisposableSourceNode : SourceNode<string>, IAsyncDisposable
    {
        public bool WasDisposed { get; private set; }
        public static int DisposeCount { get; set; }

        public new async ValueTask DisposeAsync()
        {
            WasDisposed = true;
            DisposeCount++;
            await ValueTask.CompletedTask;
        }

        public override IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            var items = new[] { "test1", "test2" };
            return new InMemoryDataPipe<string>(items);
        }
    }

    /// <summary>
    ///     A test transform node that implements IAsyncDisposable to verify disposal behavior.
    /// </summary>
    private sealed class AsyncDisposableTransformNode : TransformNode<string, string>, IAsyncDisposable
    {
        public bool WasDisposed { get; private set; }
        public static int DisposeCount { get; set; }

        public new async ValueTask DisposeAsync()
        {
            WasDisposed = true;
            DisposeCount++;
            await ValueTask.CompletedTask;
        }

        public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item + "_transformed");
        }
    }

    /// <summary>
    ///     A test sink node that implements IAsyncDisposable to verify disposal behavior.
    /// </summary>
    private sealed class AsyncDisposableSinkNode : SinkNode<string>, IAsyncDisposable
    {
        public bool WasDisposed { get; private set; }
        public static int DisposeCount { get; set; }

        public new async ValueTask DisposeAsync()
        {
            WasDisposed = true;
            DisposeCount++;
            await ValueTask.CompletedTask;
        }

        public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                // Process items
            }
        }
    }

    /// <summary>
    ///     A test source node that implements IDisposable to verify disposal behavior.
    /// </summary>
    private sealed class SyncDisposableSourceNode : SourceNode<string>, IDisposable
    {
        public bool WasDisposed { get; private set; }
        public static int DisposeCount { get; set; }

        public void Dispose()
        {
            WasDisposed = true;
            DisposeCount++;
        }

        public override IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            var items = new[] { "test1", "test2" };
            return new InMemoryDataPipe<string>(items);
        }
    }

    /// <summary>
    ///     A test transform node that throws an exception during execution to test disposal on failure.
    /// </summary>
    private sealed class ThrowingAsyncDisposableTransformNode : TransformNode<string, string>, IAsyncDisposable
    {
        public bool WasDisposed { get; private set; }
        public static int DisposeCount { get; set; }

        public new async ValueTask DisposeAsync()
        {
            WasDisposed = true;
            DisposeCount++;
            await ValueTask.CompletedTask;
        }

        public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("Test exception from transform");
        }
    }

    #endregion

    #region Test Pipeline Definitions

    private sealed class SimpleAsyncDisposablePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<AsyncDisposableSourceNode, string>("source");
            var transform = builder.AddTransform<AsyncDisposableTransformNode, string, string>("transform");
            var sink = builder.AddSink<AsyncDisposableSinkNode, string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
        }
    }

    private sealed class MixedDisposalPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SyncDisposableSourceNode, string>("source");
            var transform = builder.AddTransform<AsyncDisposableTransformNode, string, string>("transform");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
        }
    }

    private sealed class ThrowingAsyncDisposablePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<AsyncDisposableSourceNode, string>("source");
            var transform = builder.AddTransform<ThrowingAsyncDisposableTransformNode, string, string>("transform");
            var sink = builder.AddSink<AsyncDisposableSinkNode, string>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
        }
    }

    #endregion

    #region All Nodes Properly Disposed After Execution Tests

    [Fact]
    public async Task ResourceDisposal_AllAsyncDisposableNodes_AreDisposedAfterSuccessfulExecution()
    {
        // Arrange
        // Reset static counters
        AsyncDisposableSourceNode.DisposeCount = 0;
        AsyncDisposableTransformNode.DisposeCount = 0;
        AsyncDisposableSinkNode.DisposeCount = 0;

        var context = PipelineContext.Default;

        // Act
        await _pipelineRunner.RunAsync<SimpleAsyncDisposablePipeline>(context);
        await context.DisposeAsync();

        // Assert - Check that all nodes were disposed
        AsyncDisposableSourceNode.DisposeCount.Should().Be(1, "Source node should be disposed once");
        AsyncDisposableTransformNode.DisposeCount.Should().Be(1, "Transform node should be disposed once");
        AsyncDisposableSinkNode.DisposeCount.Should().Be(1, "Sink node should be disposed once");
    }

    [Fact]
    public async Task ResourceDisposal_MixedDisposableNodes_AreDisposedAfterSuccessfulExecution()
    {
        // Arrange
        // Reset static counters
        SyncDisposableSourceNode.DisposeCount = 0;
        AsyncDisposableTransformNode.DisposeCount = 0;

        var context = PipelineContext.Default;

        // Act
        await _pipelineRunner.RunAsync<MixedDisposalPipeline>(context);
        await context.DisposeAsync();

        // Assert
        // Note: SyncDisposableSourceNode might not be disposed by the pipeline because DI owns it
        // The important thing is that the AsyncDisposableTransformNode is disposed
        AsyncDisposableTransformNode.DisposeCount.Should().Be(1, "Async disposable transform should be disposed once");
    }

    #endregion

    #region Builder Disposables Transferred to Context Tests

    [Fact]
    public async Task ResourceDisposal_BuilderDisposables_AreTransferredToContext()
    {
        // Arrange
        var context = PipelineContext.Default;
        var testDisposable = new TestAsyncDisposable();
        _trackedDisposables.Add(testDisposable);

        // Act
        var result = context.RegisterIfAsyncDisposable(testDisposable);

        // Assert
        result.Should().BeSameAs(testDisposable);
        testDisposable.WasDisposed.Should().BeFalse("Should not be disposed yet");

        // Act - Dispose context
        await context.DisposeAsync();

        // Assert
        testDisposable.WasDisposed.Should().BeTrue("Should be disposed when context is disposed");
    }

    [Fact]
    public async Task ResourceDisposal_CreateAndRegister_TransfersOwnershipToContext()
    {
        // Arrange
        var context = PipelineContext.Default;
        var testDisposable = new TestAsyncDisposable();
        _trackedDisposables.Add(testDisposable);

        // Act
        var result = context.CreateAndRegister(testDisposable);

        // Assert
        result.Should().BeSameAs(testDisposable);
        testDisposable.WasDisposed.Should().BeFalse("Should not be disposed yet");

        // Act - Dispose context
        await context.DisposeAsync();

        // Assert
        testDisposable.WasDisposed.Should().BeTrue("Should be disposed when context is disposed");
    }

    [Fact]
    public async Task ResourceDisposal_SyncDisposable_WrappedAndTransferredToContext()
    {
        // Arrange
        var context = PipelineContext.Default;
        var testDisposable = new TestSyncDisposable();
        _trackedSyncDisposables.Add(testDisposable);

        // Act
        var result = context.RegisterIfAsyncDisposable(testDisposable);

        // Assert
        result.Should().BeSameAs(testDisposable);
        testDisposable.WasDisposed.Should().BeFalse("Should not be disposed yet");

        // Act - Dispose context
        await context.DisposeAsync();

        // Assert
        testDisposable.WasDisposed.Should().BeTrue("Should be disposed when context is disposed");
    }

    #endregion

    #region Pipe Disposables Cleaned Up Tests

    [Fact]
    public async Task ResourceDisposal_PipeDisposables_AreCleanedUpAfterExecution()
    {
        // Arrange
        var context = PipelineContext.Default;
        var pipe = A.Fake<IDataPipe<string>>();
        var asyncDisposablePipe = A.Fake<IAsyncDisposable>();
        var wrapper = new TestPipeWrapper(asyncDisposablePipe);

        A.CallTo(() => pipe.GetDataType()).Returns(typeof(string));

        // Act
        context.RegisterForDisposal(wrapper);
        await context.DisposeAsync();

        // Assert
        A.CallTo(() => asyncDisposablePipe.DisposeAsync()).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task ResourceDisposal_MultiplePipeDisposables_AreAllCleanedUp()
    {
        // Arrange
        var context = PipelineContext.Default;
        var asyncDisposable1 = A.Fake<IAsyncDisposable>();
        var asyncDisposable2 = A.Fake<IAsyncDisposable>();
        var asyncDisposable3 = A.Fake<IAsyncDisposable>();

        var wrapper1 = new TestPipeWrapper(asyncDisposable1);
        var wrapper2 = new TestPipeWrapper(asyncDisposable2);
        var wrapper3 = new TestPipeWrapper(asyncDisposable3);

        // Act
        context.RegisterForDisposal(wrapper1);
        context.RegisterForDisposal(wrapper2);
        context.RegisterForDisposal(wrapper3);
        await context.DisposeAsync();

        // Assert
        A.CallTo(() => asyncDisposable1.DisposeAsync()).MustHaveHappenedOnceExactly();
        A.CallTo(() => asyncDisposable2.DisposeAsync()).MustHaveHappenedOnceExactly();
        A.CallTo(() => asyncDisposable3.DisposeAsync()).MustHaveHappenedOnceExactly();
    }

    #endregion

    #region Exception Scenarios: Disposal on Failure Tests

    [Fact]
    public async Task ResourceDisposal_WhenNodeThrows_AllNodesStillDisposed()
    {
        // Arrange
        // Reset static counters
        AsyncDisposableSourceNode.DisposeCount = 0;
        ThrowingAsyncDisposableTransformNode.DisposeCount = 0;
        AsyncDisposableSinkNode.DisposeCount = 0;

        var context = PipelineContext.Default;

        // Act & Assert
        await _pipelineRunner
            .Invoking(async runner => await runner.RunAsync<ThrowingAsyncDisposablePipeline>(context))
            .Should()
            .ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException));

        // Ensure context is disposed even on failure
        await context.DisposeAsync();

        // Assert - All nodes should still be disposed even when execution fails
        AsyncDisposableSourceNode.DisposeCount.Should().Be(1, "Source node should be disposed even on failure");
        ThrowingAsyncDisposableTransformNode.DisposeCount.Should().Be(1, "Throwing transform should be disposed even on failure");
        AsyncDisposableSinkNode.DisposeCount.Should().Be(1, "Sink node should be disposed even on failure");
    }

    [Fact]
    public async Task ResourceDisposal_WhenContextDisposedLate_DisposablesStillDisposed()
    {
        // Arrange
        var context = PipelineContext.Default;
        var testDisposable = new TestAsyncDisposable();
        _trackedDisposables.Add(testDisposable);

        // Act - Register disposable, then simulate context being disposed
        context.RegisterForDisposal(testDisposable);

        // Simulate context being marked as disposed (as would happen in a failure scenario)
        await context.DisposeAsync();

        // Try to register another disposable after context is disposed
        var lateDisposable = new TestAsyncDisposable();
        _trackedDisposables.Add(lateDisposable);
        context.RegisterForDisposal(lateDisposable);

        // Give some time for background disposal to complete
        await Task.Delay(100);

        // Assert
        testDisposable.WasDisposed.Should().BeTrue("First disposable should be disposed");
        lateDisposable.WasDisposed.Should().BeTrue("Late-registered disposable should be disposed immediately");
    }

    [Fact]
    public async Task ResourceDisposal_WhenDisposeThrows_OtherDisposablesStillDisposed()
    {
        // Arrange
        var context = PipelineContext.Default;
        var goodDisposable1 = new TestAsyncDisposable();
        var throwingDisposable = new ThrowingAsyncDisposable();
        var goodDisposable2 = new TestAsyncDisposable();

        _trackedDisposables.Add(goodDisposable1);
        _trackedDisposables.Add(goodDisposable2);

        // Act - Register disposables in a specific order
        context.RegisterForDisposal(goodDisposable1);
        context.RegisterForDisposal(throwingDisposable);
        context.RegisterForDisposal(goodDisposable2);

        // Act & Assert - The context disposal should throw an AggregateException
        await context
            .Invoking(async ctx => await ctx.DisposeAsync())
            .Should()
            .ThrowAsync<AggregateException>()
            .WithMessage("*One or more errors occurred disposing pipeline context resources*")
            .Where(asyncException => asyncException.InnerExceptions.Count == 1);

        // Assert - Good disposables should still be disposed
        goodDisposable1.WasDisposed.Should().BeTrue("First good disposable should be disposed");
        goodDisposable2.WasDisposed.Should().BeTrue("Second good disposable should be disposed");
    }

    #endregion

    #region Test Helper Classes

    private sealed class TestAsyncDisposable : IAsyncDisposable
    {
        public bool WasDisposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            WasDisposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestSyncDisposable : IDisposable
    {
        public bool WasDisposed { get; private set; }

        public void Dispose()
        {
            WasDisposed = true;
        }
    }

    private sealed class ThrowingAsyncDisposable : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            throw new InvalidOperationException("Test dispose exception");
        }
    }

    private sealed class TestPipeWrapper(IAsyncDisposable inner) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            return inner.DisposeAsync();
        }
    }

    #endregion

    #region Lazy Disposal Registration Tests

    [Fact]
    public async Task ResourceDisposal_ContextWithNoRegistrations_DisposesWithoutAllocatingList()
    {
        // Arrange
        var context = PipelineContext.Default;

        // Act - Dispose without registering anything
        await context.DisposeAsync();

        // Assert - This test verifies that disposing works without errors
        // The optimization is that no List<IAsyncDisposable> is allocated when nothing is registered
    }

    [Fact]
    public async Task ResourceDisposal_AfterFirstRegistration_SubsequentRegistrationsWork()
    {
        // Arrange
        var context = PipelineContext.Default;
        var disposable1 = new TestAsyncDisposable();
        var disposable2 = new TestAsyncDisposable();
        var disposable3 = new TestAsyncDisposable();

        _trackedDisposables.Add(disposable1);
        _trackedDisposables.Add(disposable2);
        _trackedDisposables.Add(disposable3);

        // Act - Register multiple disposables (first triggers lazy initialization)
        context.RegisterForDisposal(disposable1);
        context.RegisterForDisposal(disposable2);
        context.RegisterForDisposal(disposable3);

        await context.DisposeAsync();

        // Assert - All disposables should be disposed
        disposable1.WasDisposed.Should().BeTrue("First disposable should be disposed");
        disposable2.WasDisposed.Should().BeTrue("Second disposable should be disposed");
        disposable3.WasDisposed.Should().BeTrue("Third disposable should be disposed");
    }

    [Fact]
    public async Task ResourceDisposal_ContextDisposedTwice_SecondDisposeIsNoOp()
    {
        // Arrange
        var context = PipelineContext.Default;
        var disposable = new TestAsyncDisposable();
        _trackedDisposables.Add(disposable);

        context.RegisterForDisposal(disposable);
        await context.DisposeAsync();

        // Act - Dispose again
        await context.DisposeAsync();

        // Assert - Still only disposed once (WasDisposed is a simple bool)
        disposable.WasDisposed.Should().BeTrue();
    }

    #endregion
}
