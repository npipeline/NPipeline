using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Tests.ErrorHandling;

/// <summary>
///     Tests for the fluent resilience policy builder API.
/// </summary>
public sealed class FluentErrorHandlerTests
{
    [Fact]
    public async Task RetryAlways_CreatesPolicyThatRetriesUpToMaxAttempts()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.RetryAlways<TestTransformNode, string>();

        // Act & Assert - first 3 attempts should retry
        for (var i = 0; i < 3; i++)
        {
            var decision = await DecideAsync(policy, new InvalidOperationException(), i);
            decision.Should().Be(ResilienceDecision.Retry, $"attempt {i + 1} should retry");
        }

        // 4th attempt should dead-letter
        var finalDecision = await DecideAsync(policy, new InvalidOperationException(), 3);
        finalDecision.Should().Be(ResilienceDecision.DeadLetter, "after max retries, should dead-letter");
    }

    [Fact]
    public async Task RetryOn_CreatesPolicyThatTargetsSingleExceptionType()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.RetryOn<TestTransformNode, string, TimeoutException>(
            maxRetries: 2,
            exhaustedDecision: ResilienceDecision.Skip);

        // Act & Assert - matching exception retries then transitions to configured exhaustion decision
        (await DecideAsync(policy, new TimeoutException(), 0)).Should().Be(ResilienceDecision.Retry);
        (await DecideAsync(policy, new TimeoutException(), 1)).Should().Be(ResilienceDecision.Retry);
        (await DecideAsync(policy, new TimeoutException(), 2)).Should().Be(ResilienceDecision.Skip);

        // Non-matching exception falls back to default behavior (Fail)
        (await DecideAsync(policy, new InvalidOperationException())).Should().Be(ResilienceDecision.Fail);
    }

    [Fact]
    public async Task SkipAlways_CreatesPolicyThatSkipsAllErrors()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.SkipAlways<TestTransformNode, string>();

        // Act
        var decision = await DecideAsync(policy, new Exception());

        // Assert
        decision.Should().Be(ResilienceDecision.Skip);
    }

    [Fact]
    public async Task DeadLetterAlways_CreatesPolicyThatDeadLettersAllErrors()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.DeadLetterAlways<TestTransformNode, string>();

        // Act
        var decision = await DecideAsync(policy, new Exception());

        // Assert
        decision.Should().Be(ResilienceDecision.DeadLetter);
    }

    [Fact]
    public async Task FluentBuilder_OnSpecificException_MatchesCorrectly()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .On<TimeoutException>().Retry()
            .On<ArgumentException>().Skip()
            .OnAny().DeadLetter()
            .Build();

        // Act & Assert - TimeoutException should retry
        (await DecideAsync(policy, new TimeoutException())).Should().Be(ResilienceDecision.Retry);

        // ArgumentException should skip
        (await DecideAsync(policy, new ArgumentException())).Should().Be(ResilienceDecision.Skip);

        // InvalidOperationException should dead-letter (catch-all)
        (await DecideAsync(policy, new InvalidOperationException())).Should().Be(ResilienceDecision.DeadLetter);
    }

    [Fact]
    public async Task FluentBuilder_WhenPredicate_MatchesBasedOnCustomLogic()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .When(ex => ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase)).Retry(2)
            .When(ex => ex.Message.Contains("invalid", StringComparison.OrdinalIgnoreCase)).Skip()
            .OnAny().Fail()
            .Build();

        // Act & Assert - Message with "timeout" should retry
        (await DecideAsync(policy, new Exception("Connection timeout"))).Should().Be(ResilienceDecision.Retry);

        // Message with "invalid" should skip
        (await DecideAsync(policy, new Exception("Invalid data"))).Should().Be(ResilienceDecision.Skip);

        // Other messages should fail
        (await DecideAsync(policy, new Exception("Something else"))).Should().Be(ResilienceDecision.Fail);
    }

    [Fact]
    public async Task FluentBuilder_Otherwise_SetsDefaultBehavior()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .On<TimeoutException>().Retry(2)
            .Otherwise(ResilienceDecision.Skip)
            .Build();

        // Act & Assert - TimeoutException should retry
        (await DecideAsync(policy, new TimeoutException())).Should().Be(ResilienceDecision.Retry);

        // Other exceptions should skip (Otherwise behavior)
        (await DecideAsync(policy, new Exception())).Should().Be(ResilienceDecision.Skip);
    }

    [Fact]
    public async Task FluentBuilder_RetryOnShortcut_ReducesBoilerplate()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .RetryOn<TimeoutException>(1, ResilienceDecision.Skip)
            .OnAny().Fail()
            .Build();

        // Act & Assert - typed shortcut retries once then returns custom exhaustion decision
        (await DecideAsync(policy, new TimeoutException(), 0)).Should().Be(ResilienceDecision.Retry);
        (await DecideAsync(policy, new TimeoutException(), 1)).Should().Be(ResilienceDecision.Skip);

        // Catch-all remains available for other exceptions
        (await DecideAsync(policy, new InvalidOperationException())).Should().Be(ResilienceDecision.Fail);
    }

    [Fact]
    public async Task FluentBuilder_RetryWhenShortcut_UsesPredicate()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .RetryWhen(ex => ex.Message.Contains("transient", StringComparison.OrdinalIgnoreCase),
                maxRetries: 1,
                exhaustedDecision: ResilienceDecision.DeadLetter)
            .Otherwise(ResilienceDecision.Fail)
            .Build();

        // Act & Assert - predicate match retries then dead-letters
        (await DecideAsync(policy, new Exception("transient network"), 0)).Should().Be(ResilienceDecision.Retry);
        (await DecideAsync(policy, new Exception("transient network"), 1)).Should().Be(ResilienceDecision.DeadLetter);

        // Predicate miss follows otherwise decision
        (await DecideAsync(policy, new Exception("permanent failure"))).Should().Be(ResilienceDecision.Fail);
    }

    [Fact]
    public async Task FluentBuilder_MultipleRules_EvaluatesInOrder()
    {
        // Arrange - More specific rules should come before more general ones
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .On<TimeoutException>().Retry() // More specific - checked first
            .On<InvalidOperationException>().Skip() // More specific - checked second
            .OnAny().DeadLetter() // Catch-all - checked last
            .Build();

        // Act & Assert - TimeoutException should match the first rule (Retry)
        (await DecideAsync(policy, new TimeoutException())).Should().Be(ResilienceDecision.Retry);

        // InvalidOperationException should match the second rule (Skip)
        (await DecideAsync(policy, new InvalidOperationException(), item: "test2")).Should().Be(ResilienceDecision.Skip);

        // ArgumentException should match the catch-all (DeadLetter)
        (await DecideAsync(policy, new ArgumentException(), item: "test3")).Should().Be(ResilienceDecision.DeadLetter);
    }

    [Fact]
    public async Task FluentBuilder_RetryWithExhaustion_TransitionsToDeadLetter()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .OnAny().Retry(2)
            .Build();

        var exception = new Exception();

        // Act & Assert - First 2 attempts should retry
        (await DecideAsync(policy, exception, 0)).Should().Be(ResilienceDecision.Retry);
        (await DecideAsync(policy, exception, 1)).Should().Be(ResilienceDecision.Retry);

        // 3rd attempt should dead-letter
        (await DecideAsync(policy, exception, 2)).Should().Be(ResilienceDecision.DeadLetter);
    }

    [Fact]
    public async Task FluentBuilder_DerivedExceptionTypes_AreMatched()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .On<ArgumentException>().Skip()
            .OnAny().Fail()
            .Build();

        // Act & Assert - ArgumentNullException derives from ArgumentException
        var decision = await DecideAsync(policy, new ArgumentNullException());
        decision.Should().Be(ResilienceDecision.Skip, "derived exception types should match parent type rules");
    }

    [Fact]
    public async Task FluentBuilder_NoRules_DefaultsToFail()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .Build();

        // Act
        var decision = await DecideAsync(policy, new Exception());

        // Assert
        decision.Should().Be(ResilienceDecision.Fail, "default behavior when no rules match is Fail");
    }

    [Fact]
    public async Task FluentBuilder_ComplexScenario_HandlesMultipleExceptionTypes()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .On<TimeoutException>().Retry()
            .On<IOException>().Retry(5)
            .On<ArgumentException>().Skip()
            .On<InvalidOperationException>().Fail()
            .OnAny().DeadLetter()
            .Build();

        // Act & Assert
        (await DecideAsync(policy, new TimeoutException())).Should().Be(ResilienceDecision.Retry);
        (await DecideAsync(policy, new IOException())).Should().Be(ResilienceDecision.Retry);
        (await DecideAsync(policy, new ArgumentException())).Should().Be(ResilienceDecision.Skip);
        (await DecideAsync(policy, new InvalidOperationException())).Should().Be(ResilienceDecision.Fail);
        (await DecideAsync(policy, new NotSupportedException())).Should().Be(ResilienceDecision.DeadLetter);
    }

    [Fact]
    public async Task FluentBuilder_FailDecision_StopsPipeline()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .On<InvalidOperationException>().Fail()
            .Build();

        // Act
        var decision = await DecideAsync(policy, new InvalidOperationException());

        // Assert
        decision.Should().Be(ResilienceDecision.Fail);
    }

    [Fact]
    public async Task FluentBuilder_RetryDecision_UsesFailureRetryAttempt()
    {
        // Arrange
        var policy = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .OnAny().Retry(2)
            .Build();

        // Act & Assert - Retry twice based on retry attempt.
        (await DecideAsync(policy, new Exception(), 0, "item1")).Should().Be(ResilienceDecision.Retry);
        (await DecideAsync(policy, new Exception(), 1, "item1")).Should().Be(ResilienceDecision.Retry);

        // Third attempt exhausts retries and dead-letters.
        (await DecideAsync(policy, new Exception(), 2, "item1")).Should().Be(ResilienceDecision.DeadLetter);
    }

    [Fact]
    public void Build_WithCatchAllRuleNotLast_ThrowsInvalidOperationException()
    {
        // Arrange - Create a builder with OnAny() before other rules (incorrect pattern)
        var builder = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .OnAny().DeadLetter() // Catch-all placed first
            .On<TimeoutException>().Retry(); // This rule becomes unreachable

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());

        ex.Message.Should().Contain("catch-all")
            .And.Contain("must be last")
            .And.Contain("position 1");
    }

    [Fact]
    public void Build_WithCatchAllRuleLastIsValid()
    {
        // Arrange - Create a builder with OnAny() as the last rule (correct pattern)
        var builder = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .On<TimeoutException>().Retry()
            .On<ArgumentException>().Skip()
            .OnAny().DeadLetter();

        // Act & Assert - Should not throw
        var policy = builder.Build();
        policy.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithMultipleCatchAllRules_ThrowsForEarliestIncorrectPosition()
    {
        // Arrange - Create a builder with multiple OnAny() rules (only the last is valid)
        var builder = ResiliencePolicyBuilder.ForNode<TestTransformNode, string>()
            .OnAny().DeadLetter() // First catch-all at position 1 (incorrect)
            .On<TimeoutException>().Retry()
            .OnAny().Fail(); // Second catch-all at position 3 (would be correct if first weren't there)

        // Act & Assert - Should throw for the first incorrect OnAny()
        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        ex.Message.Should().Contain("position 1");
    }

    private static Task<ResilienceDecision> DecideAsync(
        IResiliencePolicy policy,
        Exception exception,
        int retryAttempt = 0,
        string item = "test")
    {
        var context = PipelineContext.Default;

        return policy.DecideItemFailureAsync<string, string>(
            new TestTransformNode(),
            item,
            exception,
            context,
            "test-node",
            retryAttempt,
            CancellationToken.None);
    }

    /// <summary>
    ///     Simple test transform node for testing resilience policies.
    /// </summary>
    private sealed class TestTransformNode : TransformNode<string, string>
    {
        public override Task<string> TransformAsync(string input, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(input);
        }
    }
}
