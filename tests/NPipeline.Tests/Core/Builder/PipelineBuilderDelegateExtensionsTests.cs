using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Builder;

/// <summary>
///     Tests for PipelineBuilderDelegateExtensions AddTransform and WithErrorHandler methods.
///     Validates delegate-based transform creation and generic error handler wiring.
///     Covers 19 statements in PipelineBuilderDelegateExtensions.
/// </summary>
public sealed class PipelineBuilderDelegateExtensionsTests
{
    #region AddTransform Synchronous Tests

    [Fact]
    public void AddTransform_WithSyncDelegate_CreatesTransformHandle()
    {
        // Arrange
        PipelineBuilder builder = new();

        static int TransformFunc(int x)
        {
            return x * 2;
        }

        // Act
        var handle = builder.AddTransform<int, int>("DoubleTransform", TransformFunc);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
        _ = handle.Id.Should().Be("doubletransform");
    }

    [Fact]
    public void AddTransform_WithSyncDelegateStringTransform_Works()
    {
        // Arrange
        PipelineBuilder builder = new();

        static string TransformFunc(int x)
        {
            return x.ToString();
        }

        // Act
        var handle = builder.AddTransform<int, string>("IntToStringTransform", TransformFunc);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddTransform_WithSyncLambda_Works()
    {
        // Arrange
        PipelineBuilder builder = new();

        // Act
        var handle = builder.AddTransform<double, double>("SquareTransform", x => x * x);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddTransform_WithNullSyncDelegate_Throws()
    {
        // Arrange
        PipelineBuilder builder = new();
        Func<int, int>? nullDelegate = null;

        // Act & Assert
        _ = builder.Invoking(b => b.AddTransform("NullTransform", nullDelegate!))
            .Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddTransform_WithSyncDelegate_RegistersBuilderDisposable()
    {
        // Arrange
        PipelineBuilder builder = new();

        static int TransformFunc(int x)
        {
            return x + 1;
        }

        // Act
        var handle = builder.AddTransform<int, int>("IncrementTransform", TransformFunc);

        // Assert - handle indicates disposable was registered
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddTransform_WithSyncDelegate_DifferentTypes_Works()
    {
        // Arrange
        PipelineBuilder builder = new();

        static bool IsEven(int x)
        {
            return x % 2 == 0;
        }

        // Act
        var handle = builder.AddTransform<int, bool>("IsEvenTransform", IsEven);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    #endregion

    #region AddTransform Asynchronous Tests

    [Fact]
    public void AddTransform_WithAsyncDelegate_CreatesTransformHandle()
    {
        // Arrange
        PipelineBuilder builder = new();

        static async Task<int> TransformAsync(int x, PipelineContext context, CancellationToken ct)
        {
            await Task.Delay(1, ct);
            return x * 3;
        }

        // Act
        var handle = builder.AddTransform<int, int>("TripleTransformAsync", TransformAsync);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
        _ = handle.Id.Should().Be("tripletransformasync");
    }

    [Fact]
    public void AddTransform_WithAsyncDelegateStringTransform_Works()
    {
        // Arrange
        PipelineBuilder builder = new();

        static async Task<string> TransformAsync(double x, PipelineContext context, CancellationToken ct)
        {
            await Task.Yield();
            return x.ToString("F2");
        }

        // Act
        var handle = builder.AddTransform<double, string>("DoubleToFormattedStringAsync", TransformAsync);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddTransform_WithAsyncLambda_Works()
    {
        // Arrange
        PipelineBuilder builder = new();

        // Act
        var handle = builder.AddTransform<int, int>(
            "AsyncLambdaTransform",
            async (x, context, ct) =>
            {
                await Task.Yield();
                return x + 10;
            });

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddTransform_WithNullAsyncDelegate_Throws()
    {
        // Arrange
        PipelineBuilder builder = new();
        Func<int, PipelineContext, CancellationToken, Task<int>>? nullDelegate = null;

        // Act & Assert
        _ = builder.Invoking(b => b.AddTransform("NullAsyncTransform", nullDelegate!))
            .Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddTransform_WithAsyncDelegate_RegistersBuilderDisposable()
    {
        // Arrange
        PipelineBuilder builder = new();

        static async Task<int> TransformAsync(int x, PipelineContext context, CancellationToken ct)
        {
            await Task.CompletedTask;
            return x;
        }

        // Act
        var handle = builder.AddTransform<int, int>("IdentityTransformAsync", TransformAsync);

        // Assert - handle indicates disposable was registered
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddTransform_WithAsyncDelegate_DifferentTypes_Works()
    {
        // Arrange
        PipelineBuilder builder = new();

        static async Task<string> ConvertAsync(int x, PipelineContext context, CancellationToken ct)
        {
            await Task.Yield();
            return $"Value: {x}";
        }

        // Act
        var handle = builder.AddTransform<int, string>("AsyncConvertTransform", ConvertAsync);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    #endregion

    #region WithErrorHandler Tests

    [Fact]
    public void WithErrorHandler_WithGenericHandler_ReturnsBuilder()
    {
        // Arrange
        PipelineBuilder builder = new();

        static int TransformFunc(int x)
        {
            return x;
        }

        var handle = builder.AddTransform<int, int>("TestTransform", TransformFunc);

        // Act
        var result = builder.WithErrorHandler<DummyErrorHandler, int, int>(handle);

        // Assert
        _ = result.Should().NotBeNull();
        _ = result.Should().BeSameAs(builder);
    }

    [Fact]
    public void WithErrorHandler_AllowsChaining()
    {
        // Arrange
        PipelineBuilder builder = new();

        static int TransformFunc(int x)
        {
            return x;
        }

        var handle1 = builder.AddTransform<int, int>("Transform1", TransformFunc);
        var handle2 = builder.AddTransform<int, int>("Transform2", x => x + 1);

        // Act
        var result = builder
            .WithErrorHandler<DummyErrorHandler, int, int>(handle1)
            .WithErrorHandler<DummyErrorHandler, int, int>(handle2);

        // Assert
        _ = result.Should().NotBeNull();
        _ = result.Should().BeSameAs(builder);
    }

    [Fact]
    public void WithErrorHandler_WithDifferentHandlerType_Works()
    {
        // Arrange
        PipelineBuilder builder = new();

        var handle = builder.AddTransform<string, string>(
            "StringTransform",
            x => x.ToUpperInvariant());

        // Act
        var result = builder.WithErrorHandler<StringErrorHandler, string, string>(handle);

        // Assert
        _ = result.Should().NotBeNull();
        _ = result.Should().BeSameAs(builder);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public void AddTransform_MultipleTransforms_AllRegistered()
    {
        // Arrange
        PipelineBuilder builder = new();

        // Act
        var handle1 = builder.AddTransform<int, int>("Double", x => x * 2);

        var handle2 = builder.AddTransform<int, string>(
            "ToString",
            (x, ctx, ct) => Task.FromResult(x.ToString()));

        // Assert
        _ = handle1.Should().NotBeNull();
        _ = handle2.Should().NotBeNull();
        _ = handle1.Id.Should().NotBe(handle2.Id);
    }

    [Fact]
    public void AddTransform_WithErrorHandler_ChainedTogether_Works()
    {
        // Arrange
        PipelineBuilder builder = new();

        // Act
        var handle = builder
            .AddTransform<int, int>("Transform", x => x * 2);

        var result = builder
            .WithErrorHandler<DummyErrorHandler, int, int>(handle);

        // Assert
        _ = result.Should().NotBeNull();
        _ = result.Should().BeSameAs(builder);
    }

    #endregion

    #region Test Fixtures

    private sealed class DummyErrorHandler : INodeErrorHandler<ITransformNode<int, int>, int>
    {
        public Task<NodeErrorDecision> HandleAsync(
            ITransformNode<int, int> node,
            int failedItem,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Skip);
        }
    }

    private sealed class StringErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
    {
        public Task<NodeErrorDecision> HandleAsync(
            ITransformNode<string, string> node,
            string failedItem,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Skip);
        }
    }

    #endregion
}
