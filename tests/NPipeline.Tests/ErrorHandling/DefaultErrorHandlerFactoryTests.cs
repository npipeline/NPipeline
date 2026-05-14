using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

/// <summary>
///     Tests for <see cref="DefaultErrorHandlerFactory" /> dead-letter sink creation behavior.
/// </summary>
public sealed class DefaultErrorHandlerFactoryTests
{
    private readonly DefaultErrorHandlerFactory _factory = new();

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
    public void CreateDeadLetterSink_WithAbstractType_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(AbstractDeadLetterSink);

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

    private sealed class TestDeadLetterSink : IDeadLetterSink
    {
        public Task HandleAsync(
            DeadLetterEnvelope envelope,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private abstract class AbstractDeadLetterSink : IDeadLetterSink
    {
        public abstract Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken);
    }

    private sealed class ThrowingSinkConstructor : IDeadLetterSink
    {
        public ThrowingSinkConstructor()
        {
            throw new IOException("Disk failure during init");
        }

        public Task HandleAsync(
            DeadLetterEnvelope envelope,
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
