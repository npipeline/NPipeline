using AwesomeAssertions;
using FakeItEasy;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Extensions.Nodes.Tests;

public sealed class DefaultErrorHandlersTests
{
    [Fact]
    public async Task DefaultValidationErrorHandler_WithValidationException_ShouldReturnConfiguredDecision()
    {
        var handler = new DefaultValidationErrorHandler<string>();
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new ValidationException("Name", "NotEmpty", "", "Name cannot be empty");
        var context = PipelineContext.Default;

        var decision = await handler.DecideItemFailureAsync(node, "test", exception, context, "test-node", 0, CancellationToken.None);

        decision.Should().Be(ResilienceDecision.Skip);
    }

    [Fact]
    public async Task DefaultValidationErrorHandler_WithOtherException_ShouldReturnFail()
    {
        var handler = new DefaultValidationErrorHandler<string>();
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new InvalidOperationException("Unexpected error");
        var context = PipelineContext.Default;

        var decision = await handler.DecideItemFailureAsync(node, "test", exception, context, "test-node", 0, CancellationToken.None);

        decision.Should().Be(ResilienceDecision.Fail);
    }

    [Fact]
    public async Task DefaultValidationErrorHandler_WithRetryDecision_ShouldReturnRetry()
    {
        var handler = new DefaultValidationErrorHandler<string>(ResilienceDecision.Retry);
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new ValidationException("Age", "Range", 150, "Age out of range");
        var context = PipelineContext.Default;

        var decision = await handler.DecideItemFailureAsync(node, "test", exception, context, "test-node", 0, CancellationToken.None);

        decision.Should().Be(ResilienceDecision.Retry);
    }

    [Fact]
    public async Task DefaultFilteringErrorHandler_WithFilteringException_ShouldReturnConfiguredDecision()
    {
        var handler = new DefaultFilteringErrorHandler<string>();
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new FilteringException("Item does not meet criteria");
        var context = PipelineContext.Default;

        var decision = await handler.DecideItemFailureAsync(node, "test", exception, context, "test-node", 0, CancellationToken.None);

        decision.Should().Be(ResilienceDecision.Skip);
    }

    [Fact]
    public async Task DefaultFilteringErrorHandler_WithOtherException_ShouldReturnFail()
    {
        var handler = new DefaultFilteringErrorHandler<string>();
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new InvalidOperationException("Unexpected error");
        var context = PipelineContext.Default;

        var decision = await handler.DecideItemFailureAsync(node, "test", exception, context, "test-node", 0, CancellationToken.None);

        decision.Should().Be(ResilienceDecision.Fail);
    }

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithTypeConversionException_ShouldReturnConfiguredDecision()
    {
        var handler = new DefaultTypeConversionErrorHandler<string, int>();
        var node = A.Fake<ITransformNode<string, int>>();
        var exception = new TypeConversionException(typeof(string), typeof(int), "abc", "Cannot convert");
        var context = PipelineContext.Default;

        var decision = await handler.DecideItemFailureAsync(node, "test", exception, context, "test-node", 0, CancellationToken.None);

        decision.Should().Be(ResilienceDecision.Skip);
    }

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithOtherException_ShouldReturnFail()
    {
        var handler = new DefaultTypeConversionErrorHandler<string, int>();
        var node = A.Fake<ITransformNode<string, int>>();
        var exception = new InvalidOperationException("Unexpected error");
        var context = PipelineContext.Default;

        var decision = await handler.DecideItemFailureAsync(node, "test", exception, context, "test-node", 0, CancellationToken.None);

        decision.Should().Be(ResilienceDecision.Fail);
    }

    [Fact]
    public async Task DefaultValidationErrorHandler_WithCancellation_ShouldComplete()
    {
        var handler = new DefaultValidationErrorHandler<string>();
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new ValidationException("Field", "Rule", "value", "message");
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        var decision = await handler.DecideItemFailureAsync(node, "test", exception, context, "test-node", 0, cts.Token);

        decision.Should().Be(ResilienceDecision.Skip);
    }

    [Fact]
    public async Task DefaultFilteringErrorHandler_WithCancellation_ShouldComplete()
    {
        var handler = new DefaultFilteringErrorHandler<string>();
        var node = A.Fake<ITransformNode<string, string>>();
        var exception = new FilteringException("Filtered");
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        var decision = await handler.DecideItemFailureAsync(node, "test", exception, context, "test-node", 0, cts.Token);

        decision.Should().Be(ResilienceDecision.Skip);
    }

    [Fact]
    public async Task DefaultTypeConversionErrorHandler_WithCancellation_ShouldComplete()
    {
        var handler = new DefaultTypeConversionErrorHandler<string, int>();
        var node = A.Fake<ITransformNode<string, int>>();
        var exception = new TypeConversionException(typeof(string), typeof(int), "test", "error");
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        var decision = await handler.DecideItemFailureAsync(node, "test", exception, context, "test-node", 0, cts.Token);

        decision.Should().Be(ResilienceDecision.Skip);
    }
}
