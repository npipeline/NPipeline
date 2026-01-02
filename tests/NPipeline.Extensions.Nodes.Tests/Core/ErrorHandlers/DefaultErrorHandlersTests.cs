using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.Nodes.Core.ErrorHandlers;
using NPipeline.Extensions.Nodes.Core.Exceptions;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using FakeItEasy;

namespace NPipeline.Extensions.Nodes.Tests.Core.ErrorHandlers;

public sealed class DefaultErrorHandlersTests
{
    [Fact]
    public async Task DefaultValidationErrorHandler_WithValidationException_ShouldReturnConfiguredDecision()
    {
        // Arrange
        var handler = new DefaultValidationErrorHandler<string>(NodeErrorDecision.Skip);
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new ValidationException("Name", "NotEmpty", "", "Name cannot be empty");
        var context = PipelineContext.Default;

        // Act
        var decision = await handler.HandleAsync(node, "test", exception, context, CancellationToken.None);

        // Assert
        decision.Should().Be(NodeErrorDecision.Skip);
    }

    [Fact]
    public async Task DefaultValidationErrorHandler_WithOtherException_ShouldReturnFail()
    {
        // Arrange
        var handler = new DefaultValidationErrorHandler<string>(NodeErrorDecision.Skip);
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new InvalidOperationException("Unexpected error");
        var context = PipelineContext.Default;

        // Act
        var decision = await handler.HandleAsync(node, "test", exception, context, CancellationToken.None);

        // Assert
        decision.Should().Be(NodeErrorDecision.Fail);
    }

    [Fact]
    public async Task DefaultValidationErrorHandler_WithRetryDecision_ShouldReturnRetry()
    {
        // Arrange
        var handler = new DefaultValidationErrorHandler<string>(NodeErrorDecision.Retry);
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new ValidationException("Age", "Range", 150, "Age out of range");
        var context = PipelineContext.Default;

        // Act
        var decision = await handler.HandleAsync(node, "test", exception, context, CancellationToken.None);

        // Assert
        decision.Should().Be(NodeErrorDecision.Retry);
    }

    [Fact]
    public async Task DefaultFilteringErrorHandler_WithFilteringException_ShouldReturnConfiguredDecision()
    {
        // Arrange
        var handler = new DefaultFilteringErrorHandler<string>(NodeErrorDecision.Skip);
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new FilteringException("Item does not meet criteria");
        var context = PipelineContext.Default;

        // Act
        var decision = await handler.HandleAsync(node, "test", exception, context, CancellationToken.None);

        // Assert
        decision.Should().Be(NodeErrorDecision.Skip);
    }

    [Fact]
    public async Task DefaultFilteringErrorHandler_WithOtherException_ShouldReturnFail()
    {
        // Arrange
        var handler = new DefaultFilteringErrorHandler<string>(NodeErrorDecision.Skip);
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new InvalidOperationException("Unexpected error");
        var context = PipelineContext.Default;

        // Act
        var decision = await handler.HandleAsync(node, "test", exception, context, CancellationToken.None);

        // Assert
        decision.Should().Be(NodeErrorDecision.Fail);
    }

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithTypeConversionException_ShouldReturnConfiguredDecision()
    {
        // Arrange
        var handler = new DefaultTypeConversionErrorHandler<string, int>(NodeErrorDecision.Skip);
        var node = A.Fake<ITransformNode<string, int>>();
        var exception = new TypeConversionException(typeof(string), typeof(int), "abc", "Cannot convert");
        var context = PipelineContext.Default;

        // Act
        var decision = await handler.HandleAsync(node, "test", exception, context, CancellationToken.None);

        // Assert
        decision.Should().Be(NodeErrorDecision.Skip);
    }

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithOtherException_ShouldReturnFail()
    {
        // Arrange
        var handler = new DefaultTypeConversionErrorHandler<string, int>(NodeErrorDecision.Skip);
        var node = A.Fake<ITransformNode<string, int>>();
        var exception = new InvalidOperationException("Unexpected error");
        var context = PipelineContext.Default;

        // Act
        var decision = await handler.HandleAsync(node, "test", exception, context, CancellationToken.None);

        // Assert
        decision.Should().Be(NodeErrorDecision.Fail);
    }

    [Fact]
    public async Task DefaultValidationErrorHandler_WithCancellation_ShouldComplete()
    {
        // Arrange
        var handler = new DefaultValidationErrorHandler<string>(NodeErrorDecision.Skip);
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new ValidationException("Field", "Rule", "value", "message");
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act - Should not throw despite cancellation
        var decision = await handler.HandleAsync(node, "test", exception, context, cts.Token);

        // Assert
        decision.Should().Be(NodeErrorDecision.Skip);
    }

    [Fact]
    public async Task DefaultFilteringErrorHandler_WithCancellation_ShouldComplete()
    {
        // Arrange
        var handler = new DefaultFilteringErrorHandler<string>(NodeErrorDecision.Skip);
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new FilteringException("Filtered");
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act - Should not throw despite cancellation
        var decision = await handler.HandleAsync(node, "test", exception, context, cts.Token);

        // Assert
        decision.Should().Be(NodeErrorDecision.Skip);
    }

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithCancellation_ShouldComplete()
    {
        // Arrange
        var handler = new DefaultTypeConversionErrorHandler<string, int>(NodeErrorDecision.Skip);
        var node = A.Fake<ITransformNode<string, int>>();
        var exception = new TypeConversionException(typeof(string), typeof(int), "test", "error");
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act - Should not throw despite cancellation
        var decision = await handler.HandleAsync(node, "test", exception, context, cts.Token);

        // Assert
        decision.Should().Be(NodeErrorDecision.Skip);
    }
}
