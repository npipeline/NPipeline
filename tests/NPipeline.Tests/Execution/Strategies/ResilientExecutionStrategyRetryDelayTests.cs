using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Configuration.RetryDelay;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Execution.Strategies;

public sealed class ResilientExecutionStrategyRetryDelayTests
{
    [Fact]
    public async Task ExecuteAsync_WithExponentialBackoff_ShouldUseCorrectDelays()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(
                TimeSpan.FromMilliseconds(10),
                2.0,
                TimeSpan.FromSeconds(1)),
            new NoJitterConfiguration());

        var context = CreatePipelineContextWithRetryDelay(delayConfig);
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);

        await using var input = new NPipeline.DataFlow.DataPipes.ListDataPipe<int>([1, 2, 3], "test-input");
        var node = new FailingTransformNode(2); // Fail twice, then succeed

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Consume the result to trigger retries
            var outputs = new List<string>();
            await foreach (var item in result.WithCancellation(CancellationToken.None))
            {
                outputs.Add(item);
            }

            stopwatch.Stop();

            // Assert
            _ = outputs.Should().HaveCount(3);
            _ = stopwatch.ElapsedMilliseconds.Should().BeGreaterThanOrEqualTo(30); // 10ms + 20ms minimum delays
            _ = stopwatch.ElapsedMilliseconds.Should().BeLessThan(200); // Should not take too long
        }
    }

    [Fact]
    public async Task ExecuteAsync_WithLinearBackoff_ShouldUseCorrectDelays()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new LinearBackoffConfiguration(
                TimeSpan.FromMilliseconds(10),
                TimeSpan.FromMilliseconds(5),
                TimeSpan.FromSeconds(1)),
            new NoJitterConfiguration());

        var context = CreatePipelineContextWithRetryDelay(delayConfig);
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);

        await using var input = new NPipeline.DataFlow.DataPipes.ListDataPipe<int>([1, 2, 3], "test-input");
        var node = new FailingTransformNode(2); // Fail twice, then succeed

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Consume the result to trigger retries
            var outputs = new List<string>();
            await foreach (var item in result.WithCancellation(CancellationToken.None))
            {
                outputs.Add(item);
            }

            stopwatch.Stop();

            // Assert
            _ = outputs.Should().HaveCount(3);
            _ = stopwatch.ElapsedMilliseconds.Should().BeGreaterThanOrEqualTo(30); // 10ms + 15ms minimum delays
            _ = stopwatch.ElapsedMilliseconds.Should().BeLessThan(200); // Should not take too long
        }
    }

    [Fact]
    public async Task ExecuteAsync_WithFixedDelay_ShouldUseCorrectDelays()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new FixedDelayConfiguration(TimeSpan.FromMilliseconds(20)),
            new NoJitterConfiguration());

        var context = CreatePipelineContextWithRetryDelay(delayConfig);
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);

        await using var input = new NPipeline.DataFlow.DataPipes.ListDataPipe<int>([1, 2, 3], "test-input");
        var node = new FailingTransformNode(2); // Fail twice, then succeed

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Consume the result to trigger retries
            var outputs = new List<string>();
            await foreach (var item in result.WithCancellation(CancellationToken.None))
            {
                outputs.Add(item);
            }

            stopwatch.Stop();

            // Assert
            _ = outputs.Should().HaveCount(3);
            _ = stopwatch.ElapsedMilliseconds.Should().BeGreaterThanOrEqualTo(40); // 20ms + 20ms delays
            _ = stopwatch.ElapsedMilliseconds.Should().BeLessThan(200); // Should not take too long
        }
    }

    [Fact]
    public async Task ExecuteAsync_WithJitter_ShouldUseVariableDelays()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(
                TimeSpan.FromMilliseconds(10),
                2.0,
                TimeSpan.FromSeconds(1)),
            new FullJitterConfiguration());

        var context = CreatePipelineContextWithRetryDelay(delayConfig);
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);

        await using var input = new NPipeline.DataFlow.DataPipes.ListDataPipe<int>([1, 2, 3], "test-input");
        var node = new FailingTransformNode(2); // Fail twice, then succeed

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Consume the result to trigger retries
            var outputs = new List<string>();
            await foreach (var item in result.WithCancellation(CancellationToken.None))
            {
                outputs.Add(item);
            }

            stopwatch.Stop();

            // Assert
            _ = outputs.Should().HaveCount(3);
            _ = stopwatch.ElapsedMilliseconds.Should().BeGreaterThan(0);
            _ = stopwatch.ElapsedMilliseconds.Should().BeLessThan(200); // Should not take too long
        }
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellationDuringDelay_ShouldRespectCancellation()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(
                TimeSpan.FromSeconds(1),
                2.0,
                TimeSpan.FromSeconds(10)),
            new NoJitterConfiguration());

        var context = CreatePipelineContextWithRetryDelay(delayConfig);
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);

        await using var input = new NPipeline.DataFlow.DataPipes.ListDataPipe<int>([1, 2, 3], "test-input");
        var node = new FailingTransformNode(10); // Always fail

        using var cts = new CancellationTokenSource();

        // Act & Assert
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, cts.Token);

            var executeTask = Task.Run(async () =>
            {
                var outputs = new List<string>();
                await foreach (var item in result.WithCancellation(cts.Token))
                {
                    outputs.Add(item);
                }
            });

            // Wait a bit then cancel
            await Task.Delay(50);
            cts.Cancel();

            _ = await Assert.ThrowsAsync<OperationCanceledException>(() => executeTask);
        }
    }

    [Fact]
    public async Task ExecuteAsync_WithMaxRetriesExceeded_ShouldStopRetrying()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(
                TimeSpan.FromMilliseconds(1),
                2.0,
                TimeSpan.FromSeconds(1)),
            new NoJitterConfiguration());

        var context = CreatePipelineContextWithRetryDelay(delayConfig, maxRetries: 2);
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);

        await using var input = new NPipeline.DataFlow.DataPipes.ListDataPipe<int>([1, 2, 3], "test-input");
        var node = new FailingTransformNode(10); // Always fail

        // Act & Assert
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            _ = await Assert.ThrowsAsync<NodeExecutionException>(async () =>
            {
                var outputs = new List<string>();
                await foreach (var item in result.WithCancellation(CancellationToken.None))
                {
                    outputs.Add(item);
                }
            });
        }
    }

    [Fact]
    public async Task ExecuteAsync_WithNoDelayStrategy_ShouldNotDelay()
    {
        // Arrange
        var context = CreatePipelineContextWithRetryDelay(null, maxRetries: 2);
        var innerStrategy = new SequentialExecutionStrategy();
        var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);

        await using var input = new NPipeline.DataFlow.DataPipes.ListDataPipe<int>([1, 2, 3], "test-input");
        var node = new FailingTransformNode(2); // Fail twice, then succeed

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        using (context.ScopedNode("test-node"))
        {
            await using var result = await resilientStrategy.ExecuteAsync(input, node, context, CancellationToken.None);

            // Consume the result to trigger retries
            var outputs = new List<string>();
            await foreach (var item in result.WithCancellation(CancellationToken.None))
            {
                outputs.Add(item);
            }

            stopwatch.Stop();

            // Assert
            _ = outputs.Should().HaveCount(3);
            _ = stopwatch.ElapsedMilliseconds.Should().BeLessThan(50); // Should be very fast without delays
        }
    }

    private static PipelineContext CreatePipelineContextWithRetryDelay(
        RetryDelayStrategyConfiguration? delayConfig,
        int maxRetries = 3)
    {
        var retryOptions = new PipelineRetryOptions(maxRetries, 5, 10, null, delayConfig);
        return new PipelineContextBuilder()
            .WithErrorHandler(new TestErrorHandler(PipelineErrorDecision.RestartNode))
            .WithRetry(retryOptions)
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

    private sealed class TestErrorHandler(PipelineErrorDecision decision) : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception error, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(decision);
        }
    }
}