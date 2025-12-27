using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

/// <summary>
///     Tests for <see cref="DefaultErrorHandlerFactory" /> covering error handler and dead-letter sink creation.
/// </summary>
public sealed class DefaultErrorHandlerFactoryTests
{
    private readonly DefaultErrorHandlerFactory _factory = new();

    #region CreateErrorHandler Tests

    [Fact]
    public void CreateErrorHandler_WithValidPipelineErrorHandlerType_ReturnsInstance()
    {
        // Arrange
        var handlerType = typeof(TestPipelineErrorHandler);

        // Act
        var result = _factory.CreateErrorHandler(handlerType);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<TestPipelineErrorHandler>();
    }

    [Fact]
    public void CreateErrorHandler_WithNonImplementingType_ReturnsNull()
    {
        // Arrange
        var handlerType = typeof(string);

        // Act
        var result = _factory.CreateErrorHandler(handlerType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateErrorHandler_WithTypeWithoutParameterlessConstructor_ReturnsNull()
    {
        // Arrange
        var handlerType = typeof(TypeWithoutParameterlessConstructor);

        // Act
        var result = _factory.CreateErrorHandler(handlerType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateErrorHandler_WithAbstractType_ReturnsNull()
    {
        // Arrange
        var handlerType = typeof(AbstractPipelineErrorHandler);

        // Act
        var result = _factory.CreateErrorHandler(handlerType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateErrorHandler_WithThrowingConstructor_ReturnsNull()
    {
        // Arrange
        var handlerType = typeof(ThrowingConstructorHandler);

        // Act
        var result = _factory.CreateErrorHandler(handlerType);

        // Assert
        result.Should().BeNull();
    }

    #endregion

    #region CreateNodeErrorHandler Tests

    [Fact]
    public void CreateNodeErrorHandler_WithValidNodeErrorHandlerType_ReturnsInstance()
    {
        // Arrange
        var handlerType = typeof(TestNodeErrorHandler);

        // Act
        var result = _factory.CreateNodeErrorHandler(handlerType);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<TestNodeErrorHandler>();
    }

    [Fact]
    public void CreateNodeErrorHandler_WithNonImplementingType_ReturnsNull()
    {
        // Arrange
        var handlerType = typeof(int);

        // Act
        var result = _factory.CreateNodeErrorHandler(handlerType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateNodeErrorHandler_WithTypeWithoutParameterlessConstructor_ReturnsNull()
    {
        // Arrange
        var handlerType = typeof(TypeWithoutParameterlessConstructor);

        // Act
        var result = _factory.CreateNodeErrorHandler(handlerType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateNodeErrorHandler_MultipleInvocations_CreatesNewInstanceEachTime()
    {
        // Arrange
        var handlerType = typeof(TestNodeErrorHandler);

        // Act
        var result1 = _factory.CreateNodeErrorHandler(handlerType);
        var result2 = _factory.CreateNodeErrorHandler(handlerType);

        // Assert
        result1.Should().NotBeNull();
        result2.Should().NotBeNull();
        result1.Should().NotBeSameAs(result2);
    }

    #endregion

    #region CreateDeadLetterSink Tests

    [Fact]
    public void CreateDeadLetterSink_WithValidDeadLetterSinkType_ReturnsInstance()
    {
        // Arrange
        var sinkType = typeof(TestDeadLetterSink);

        // Act
        var result = _factory.CreateDeadLetterSink(sinkType);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<TestDeadLetterSink>();
    }

    [Fact]
    public void CreateDeadLetterSink_WithNonImplementingType_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(bool);

        // Act
        var result = _factory.CreateDeadLetterSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateDeadLetterSink_WithTypeWithoutParameterlessConstructor_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(TypeWithoutParameterlessConstructor);

        // Act
        var result = _factory.CreateDeadLetterSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateDeadLetterSink_WithThrowingConstructor_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(ThrowingSinkConstructor);

        // Act
        var result = _factory.CreateDeadLetterSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    #endregion

    #region Test Fixtures

    private sealed class TestPipelineErrorHandler : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(
            string nodeId,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
        }
    }

    private abstract class AbstractPipelineErrorHandler : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(
            string nodeId,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
        }
    }

    private sealed class ThrowingConstructorHandler : IPipelineErrorHandler
    {
        public ThrowingConstructorHandler()
        {
            throw new InvalidOperationException("Constructor deliberately failed");
        }

        public Task<PipelineErrorDecision> HandleNodeFailureAsync(
            string nodeId,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
        }
    }

    private sealed class TestNodeErrorHandler : INodeErrorHandler<ITransformNode<int, int>, int>
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

    private sealed class TestDeadLetterSink : IDeadLetterSink
    {
        public Task HandleAsync(
            string nodeId,
            object item,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingSinkConstructor : IDeadLetterSink
    {
        public ThrowingSinkConstructor()
        {
            throw new IOException("Disk failure during init");
        }

        public Task HandleAsync(
            string nodeId,
            object item,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TypeWithoutParameterlessConstructor
    {
        public TypeWithoutParameterlessConstructor(string _)
        {
        }
    }

    #endregion
}
