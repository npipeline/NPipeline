using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Execution.Strategies;

public sealed class ResilientExecutionStrategyCircuitBreakerTests
{
    [Fact]
    public async Task ExecuteAsync_WithCircuitBreakerEnabled_ShouldCreateCircuitBreakerManager()
    {
        // Arrange
        var context = CreatePipelineContextWithCircuitBreaker();
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);
        await using var input = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>([1, 2, 3], "test-input");
        var node = new TestTransformNode();

        // Act
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Assert
            _ = result.Should().NotBeNull();
            _ = context.Items.Should().ContainKey(PipelineContextKeys.CircuitBreakerManager);
            _ = context.Items[PipelineContextKeys.CircuitBreakerManager].Should().BeAssignableTo<ICircuitBreakerManager>();
        }
    }

    [Fact]
    public async Task ExecuteAsync_WithCircuitBreakerDisabled_ShouldNotCreateCircuitBreakerManager()
    {
        // Arrange
        var context = CreatePipelineContextWithoutCircuitBreaker();
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);
        await using var input = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>([1, 2, 3], "test-input");
        var node = new TestTransformNode();

        // Act
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Assert
            _ = result.Should().NotBeNull();
            _ = context.Items.Should().NotContainKey(PipelineContextKeys.CircuitBreakerManager);
        }
    }

    [Fact]
    public async Task ExecuteAsync_WithCircuitBreakerTripped_ShouldThrowCircuitBreakerOpenException()
    {
        // Arrange
        var context = CreatePipelineContextWithCircuitBreaker();
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);

        // Increase input items to ensure we have enough failures to trip circuit breaker
        await using var input = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "test-input");
        var node = new FailingTransformNode(10); // Fail more times to ensure circuit breaker trips

        // Act & Assert
        // ExecuteAsync returns a lazy IDataPipe, so we need to consume it to trigger circuit breaker
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Try to consume the result - this should trigger the circuit breaker
            var exception = await Assert.ThrowsAsync<NodeExecutionException>(async () =>
            {
                var output = new List<string>();

                await foreach (var item in result.WithCancellation(CancellationToken.None))
                {
                    output.Add(item);
                }
            });

            // Verify the inner exception is CircuitBreakerOpenException
            _ = exception.InnerException.Should().BeOfType<CircuitBreakerOpenException>();
        }
    }

    [Fact]
    public async Task ExecuteAsync_WhenCircuitBreakerRecovers_ShouldAllowExecutionAfterOpenDuration()
    {
        // Arrange
        var options = new PipelineCircuitBreakerOptions(
            1,
            TimeSpan.FromMilliseconds(200),
            TimeSpan.FromSeconds(1),
            true,
            CircuitBreakerThresholdType.ConsecutiveFailures,
            0.5,
            1,
            2);

        var context = CreatePipelineContextWithCircuitBreaker(options);
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);
        var node = new RecoveringTransformNode(1);

        using (context.ScopedNode("recovery-node"))
        {
            // Act 1: trigger breaker
            await using (var initialInput = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>([1], "first"))
            {
                await using var result = await resilientStrategy.ExecuteAsync(initialInput, node, context, CancellationToken.None);

                _ = await Assert.ThrowsAsync<NodeExecutionException>(async () =>
                {
                    await foreach (var _ in result.WithCancellation(CancellationToken.None))
                    {
                        // exhaust
                    }
                });
            }

            // Allow the breaker to transition to half-open by polling its state
            // Wait for initial delay plus additional buffer for timer callback execution
            await Task.Delay(options.OpenDuration + TimeSpan.FromMilliseconds(500));

            // Poll to ensure the circuit breaker has transitioned to HalfOpen
            var manager = context.Items[PipelineContextKeys.CircuitBreakerManager] as ICircuitBreakerManager;
            var circuitBreaker = manager?.GetCircuitBreaker("recovery-node", options);

            // Add a small retry loop to account for timing variations
            var maxRetries = 5;
            var retryCount = 0;

            while (retryCount < maxRetries && circuitBreaker?.State != CircuitBreakerState.HalfOpen)
            {
                await Task.Delay(50);
                retryCount++;
            }

            // Act 2: half-open should permit execution and recover to closed
            await using var recoveryInput = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>([2], "second");
            await using var recoveryResult = await resilientStrategy.ExecuteAsync(recoveryInput, node, context, CancellationToken.None);

            var outputs = new List<string>();

            await foreach (var item in recoveryResult.WithCancellation(CancellationToken.None))
            {
                outputs.Add(item);
            }

            // Assert
            _ = outputs.Should().HaveCount(1);
            _ = outputs[0].Should().Be("processed-2");
        }
    }

    private static PipelineContext CreatePipelineContextWithCircuitBreaker(
        PipelineCircuitBreakerOptions? options = null,
        CircuitBreakerMemoryManagementOptions? memoryOptions = null)
    {
        var context = new PipelineContextBuilder()
            .WithErrorHandler(new TestErrorHandler(PipelineErrorDecision.RestartNode))
            .Build();

        context.Items[PipelineContextKeys.CircuitBreakerOptions] = (options ?? new PipelineCircuitBreakerOptions(
            3,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5))).Validate();

        if (memoryOptions is not null)
            context.Items[PipelineContextKeys.CircuitBreakerMemoryOptions] = memoryOptions.Validate();

        return context;
    }

    private static PipelineContext CreatePipelineContextWithoutCircuitBreaker()
    {
        return new PipelineContextBuilder()
            .WithErrorHandler(new TestErrorHandler(PipelineErrorDecision.RestartNode))
            .Build();
    }

    private sealed class TestTransformNode : TransformNode<int, string>
    {
        public override async Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            return $"processed-{item}";
        }
    }

    private sealed class FailingTransformNode(int failCount = 1) : TransformNode<int, string>
    {
        private int _currentAttempt;

        public override async Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            _currentAttempt++;

            return _currentAttempt > failCount
                ? $"processed-{item}"
                : throw new InvalidOperationException($"Simulated failure {_currentAttempt}");
        }
    }

    private sealed class RecoveringTransformNode(int failuresBeforeSuccess) : TransformNode<int, string>
    {
        private int _attempts;

        public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempts++;

            return _attempts > failuresBeforeSuccess
                ? Task.FromResult($"processed-{item}")
                : throw new InvalidOperationException($"Simulated failure {_attempts}");
        }
    }

    private sealed class TestErrorHandler(PipelineErrorDecision decision) : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception error, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(decision);
        }
    }
}
