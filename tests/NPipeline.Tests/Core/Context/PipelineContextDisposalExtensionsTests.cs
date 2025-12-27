using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Context;

public sealed class PipelineContextDisposalExtensionsTests
{
    #region Sync Disposable Wrapping Tests

    [Fact]
    public async Task RegisterIfAsyncDisposable_WrapsSyncDisposable_WithExceptionHandling()
    {
        // Arrange
        var context = PipelineContext.Default;
        var throwingDisposable = new ThrowingSyncDisposable();

        // Act
        context.RegisterIfAsyncDisposable(throwingDisposable);
        var act = async () => await context.DisposeAsync();

        // Assert - should not throw because AsyncDisposableWrapper catches and swallows exceptions
        await act.Should().NotThrowAsync();
    }

    #endregion

    #region RegisterIfAsyncDisposable Tests

    [Fact]
    public void RegisterIfAsyncDisposable_WithAsyncDisposable_Registers()
    {
        // Arrange
        var context = PipelineContext.Default;
        var asyncDisposable = A.Fake<IAsyncDisposable>();

        // Act
        var result = context.RegisterIfAsyncDisposable(asyncDisposable);

        // Assert
        result.Should().Be(asyncDisposable);
    }

    [Fact]
    public async Task RegisterIfAsyncDisposable_WithAsyncDisposable_DisposesSonDispose()
    {
        // Arrange
        var context = PipelineContext.Default;
        var asyncDisposable = A.Fake<IAsyncDisposable>();

        // Act
        context.RegisterIfAsyncDisposable(asyncDisposable);
        await context.DisposeAsync();

        // Assert
        A.CallTo(() => asyncDisposable.DisposeAsync()).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void RegisterIfAsyncDisposable_WithSyncDisposable_WrapsAndRegisters()
    {
        // Arrange
        var context = PipelineContext.Default;
        var syncDisposable = A.Fake<IDisposable>();

        // Act
        var result = context.RegisterIfAsyncDisposable(syncDisposable);

        // Assert
        result.Should().Be(syncDisposable);
    }

    [Fact]
    public async Task RegisterIfAsyncDisposable_WithSyncDisposable_DisposesSOnDispose()
    {
        // Arrange
        var context = PipelineContext.Default;
        var syncDisposable = A.Fake<IDisposable>();

        // Act
        context.RegisterIfAsyncDisposable(syncDisposable);
        await context.DisposeAsync();

        // Assert
        A.CallTo(() => syncDisposable.Dispose()).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void RegisterIfAsyncDisposable_WithNonDisposable_DoesNotThrow()
    {
        // Arrange
        var context = PipelineContext.Default;
        var notDisposable = "some string";

        // Act
        var act = () => context.RegisterIfAsyncDisposable(notDisposable);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void RegisterIfAsyncDisposable_ReturnsOriginalInstance()
    {
        // Arrange
        var context = PipelineContext.Default;
        var instance = new TestAsyncDisposable();

        // Act
        var result = context.RegisterIfAsyncDisposable(instance);

        // Assert
        result.Should().BeSameAs(instance);
    }

    [Fact]
    public void RegisterIfAsyncDisposable_WithMultipleInstances()
    {
        // Arrange
        var context = PipelineContext.Default;
        var disposable1 = A.Fake<IAsyncDisposable>();
        var disposable2 = A.Fake<IAsyncDisposable>();

        // Act
        context.RegisterIfAsyncDisposable(disposable1);
        context.RegisterIfAsyncDisposable(disposable2);

        // Assert - both should be registered
    }

    #endregion

    #region CreateAndRegister Tests

    [Fact]
    public void CreateAndRegister_WithAsyncDisposable_Registers()
    {
        // Arrange
        var context = PipelineContext.Default;
        var asyncDisposable = new TestAsyncDisposable();

        // Act
        var result = context.CreateAndRegister(asyncDisposable);

        // Assert
        result.Should().Be(asyncDisposable);
    }

    [Fact]
    public async Task CreateAndRegister_WithAsyncDisposable_DisposesOnContextDispose()
    {
        // Arrange
        var context = PipelineContext.Default;
        var disposable = new TestAsyncDisposable();

        // Act
        context.CreateAndRegister(disposable);
        await context.DisposeAsync();

        // Assert
        disposable.WasDisposed.Should().BeTrue();
    }

    [Fact]
    public void CreateAndRegister_WithSyncDisposable_Registers()
    {
        // Arrange
        var context = PipelineContext.Default;
        var syncDisposable = new TestSyncDisposable();

        // Act
        var result = context.CreateAndRegister(syncDisposable);

        // Assert
        result.Should().Be(syncDisposable);
    }

    [Fact]
    public async Task CreateAndRegister_WithSyncDisposable_DisposesOnContextDispose()
    {
        // Arrange
        var context = PipelineContext.Default;
        var disposable = new TestSyncDisposable();

        // Act
        context.CreateAndRegister(disposable);
        await context.DisposeAsync();

        // Assert
        disposable.WasDisposed.Should().BeTrue();
    }

    [Fact]
    public void CreateAndRegister_ReturnsOriginalInstance()
    {
        // Arrange
        var context = PipelineContext.Default;
        var instance = new TestAsyncDisposable();

        // Act
        var result = context.CreateAndRegister(instance);

        // Assert
        result.Should().BeSameAs(instance);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task RegisterIfAsyncDisposable_WithMultipleInstances_DisposesAll()
    {
        // Arrange
        var context = PipelineContext.Default;
        var asyncDisposable1 = new TestAsyncDisposable();
        var asyncDisposable2 = new TestAsyncDisposable();
        var syncDisposable = new TestSyncDisposable();

        // Act
        context.RegisterIfAsyncDisposable(asyncDisposable1);
        context.RegisterIfAsyncDisposable(asyncDisposable2);
        context.RegisterIfAsyncDisposable(syncDisposable);

        await context.DisposeAsync();

        // Assert
        asyncDisposable1.WasDisposed.Should().BeTrue();
        asyncDisposable2.WasDisposed.Should().BeTrue();
        syncDisposable.WasDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task RegisterIfAsyncDisposable_WithNonDisposable_DoesNotCauseIssues()
    {
        // Arrange
        var context = PipelineContext.Default;
        var notDisposable = "string";
        var disposable = new TestAsyncDisposable();

        // Act
        context.RegisterIfAsyncDisposable(notDisposable);
        context.RegisterIfAsyncDisposable(disposable);

        await context.DisposeAsync();

        // Assert
        disposable.WasDisposed.Should().BeTrue();
    }

    #endregion

    #region Test Helpers

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

    private sealed class ThrowingSyncDisposable : IDisposable
    {
        public void Dispose()
        {
            throw new InvalidOperationException("Test exception");
        }
    }

    #endregion
}
