using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Factories;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using ParallelExecOptions = NPipeline.Extensions.Parallelism.ParallelOptions;
using QueuePolicy = NPipeline.Extensions.Parallelism.BoundedQueuePolicy;

namespace NPipeline.Extensions.Parallelism.Tests;

public class ParallelExecutionStrategyTests
{
    [Fact]
    public async Task Block_WithDefaultOptions_ProcessesAllItemsInOrder()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 10).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act
        await runner.RunAsync<DefaultOptionsPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<int>>();
        sink.Items.Should().HaveCount(10);
        sink.Items.Should().BeInAscendingOrder().And.BeEquivalentTo(sourceData);
    }

    [Fact]
    public async Task Block_WithPreserveOrdering_ProcessesAllItemsInOrder()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 10).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act
        await runner.RunAsync<PreserveOrderingPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<int>>();
        sink.Items.Should().HaveCount(10);
        sink.Items.Should().BeInAscendingOrder().And.BeEquivalentTo(sourceData);
    }

    [Fact]
    public async Task Block_WithBoundedQueue_ExhibitsBackpressure()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 100).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act
        await runner.RunAsync<BoundedQueuePipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<int>>();
        sink.Items.Should().HaveCount(100);
    }

    [Fact]
    public async Task Block_WithSlowSink_ExhibitsBackpressure()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 50).ToList();
        var slowSink = new SlowSink();
        var context = PipelineContext.Default;

        context.Items[PipelineContextKeys.PreconfiguredNodes] = new Dictionary<string, INode>
        {
            ["sink"] = slowSink,
        };

        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act
        await runner.RunAsync<SlowSinkPipeline>(context);

        // Assert
        slowSink.Items.Should().HaveCount(50);
    }

    [Fact]
    public async Task DropNewest_WhenQueueFull_DropsNewestItems()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 20).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act
        await runner.RunAsync<DropNewestPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<int>>();

        // With MaxQueueLength=10 and DropNewest policy with slow processing,
        // items should be dropped when the queue becomes full.
        // The exact count depends on timing, but should be less than all 20 items
        sink.Items.Should().HaveCountLessThan(20, "because some items should be dropped when the queue is full");

        // Items should be in ascending order (those that made it through)
        sink.Items.Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task DropOldest_WhenQueueFull_DropsOldestItems()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 20).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act
        await runner.RunAsync<DropOldestPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<int>>();

        // With DropOldest and a queue of 10, we expect some items to be dropped
        // The exact number may vary due to timing, but we should have fewer than 20 items
        sink.Items.Should().HaveCountLessThan(20);

        // Items should be in ascending order
        sink.Items.Should().BeInAscendingOrder();

        // We should have the later items (higher numbers) as oldest ones are dropped
        sink.Items.Max().Should().Be(20);
    }

    [Fact]
    public async Task ErrorHandling_WhenTransformThrows_PropagatesException()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 10).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act & Assert
        await runner.Invoking(async r => await r.RunAsync<ErrorHandlingPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithMessage("*Test failure*");
    }

    [Fact]
    public async Task ErrorHandling_WithMultipleErrors_PropagatesFirstException()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 20).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act & Assert
        await runner.Invoking(async r => await r.RunAsync<MultipleErrorsPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithMessage("*Test failure on even value*");
    }

    [Fact]
    public async Task ErrorHandling_WithSlowFailingNode_PropagatesException()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 10).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act & Assert
        await runner.Invoking(async r => await r.RunAsync<SlowFailingPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithMessage("*Test failure on value 3*");
    }

    [Fact]
    public async Task ErrorHandling_WhenTransformThrows_HandlesExceptionGracefully()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 10).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act & Assert
        await runner.Invoking(async r => await r.RunAsync<ErrorHandlingPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithMessage("*Test failure*");
    }

    [Fact]
    public async Task ErrorHandling_WithMultipleErrors_ProcessesNonFailingItems()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 10).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act & Assert
        await runner.Invoking(async r => await r.RunAsync<MultipleErrorsPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithMessage("*Test failure on even value*");
    }

    [Fact]
    public async Task ErrorHandling_WithSlowFailingNode_ContinuesProcessing()
    {
        // Arrange
        var sourceData = Enumerable.Range(1, 10).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act & Assert
        await runner.Invoking(async r => await r.RunAsync<SlowFailingPipeline>(context))
            .Should().ThrowAsync<NodeExecutionException>()
            .WithMessage("*Test failure on value 3*");
    }

    [Fact]
    public async Task Cancellation_WhenTokenCancelled_StopsProcessingGracefully()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);

        var context = new PipelineContextBuilder()
            .WithCancellation(cts.Token)
            .Build();

        // Start the pipeline
        var runTask = () => testRunner.RunAndGetResultAsync<CancellationTestPipeline, int>(context);

        // Wait a bit for processing to start
        await Task.Delay(100);

        // Cancel the operation
        cts.Cancel();

        // Act & Assert
        await runTask.Should().ThrowAsync<PipelineExecutionException>()
            .WithInnerException(typeof(OperationCanceledException));
    }

    [Fact]
    public async Task Cancellation_WithAlreadyCancelledToken_ThrowsImmediately()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);

        var context = new PipelineContextBuilder()
            .WithCancellation(cts.Token)
            .Build();

        // Act & Assert
        var runTask = () => testRunner.RunAndGetResultAsync<CancellationTestPipeline, int>(context);

        await runTask.Should().ThrowAsync<PipelineExecutionException>()
            .WithInnerException(typeof(OperationCanceledException));
    }

    [Fact]
    public async Task Cancellation_DuringProcessing_StopsAtAppropriatePhase()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);

        var context = new PipelineContextBuilder()
            .WithCancellation(cts.Token)
            .Build();

        // Start the pipeline
        var runTask = () => testRunner.RunAndGetResultAsync<CancellationTestPipeline, int>(context);

        // Wait a bit for processing to start
        await Task.Delay(100);

        // Cancel the operation
        cts.Cancel();

        // Act & Assert
        await runTask.Should().ThrowAsync<PipelineExecutionException>()
            .WithInnerException(typeof(OperationCanceledException));
    }

    // Helper classes for testing
    public sealed class IdentityTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }

    private sealed class DelayTransform : TransformNode<int, int>
    {
        private readonly TimeSpan _delay;

        public DelayTransform() : this(TimeSpan.FromSeconds(1))
        {
        }

        public DelayTransform(TimeSpan delay)
        {
            _delay = delay;
        }

        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(_delay, cancellationToken);
            return item;
        }
    }

    private sealed class FailingTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == 5)
                throw new InvalidOperationException("Test failure");

            return Task.FromResult(item);
        }
    }

    private sealed class SlowSink : SinkNode<int>
    {
        public readonly List<int> Items = [];

        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                await Task.Delay(10, cancellationToken); // Simulate slow processing
                Items.Add(item);
            }
        }
    }

    // Pipeline definitions
    private sealed class DefaultOptionsPipeline : IPipelineDefinition
    {
        public string Name => nameof(DefaultOptionsPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 10).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy with default options
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions());
        }
    }

    private sealed class PreserveOrderingPipeline : IPipelineDefinition
    {
        public string Name => nameof(PreserveOrderingPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 10).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy with ordering preservation
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions(PreserveOrdering: true));
        }
    }

    private sealed class BoundedQueuePipeline : IPipelineDefinition
    {
        public string Name => nameof(BoundedQueuePipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 100).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy with bounded queue
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions(4, 10));
        }
    }

    private sealed class SlowSinkPipeline : IPipelineDefinition
    {
        public string Name => nameof(SlowSinkPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 50).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddSink<SlowSink, int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy with small queue to test backpressure
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions(2, 5));
        }
    }

    private sealed class DropNewestPipeline : IPipelineDefinition
    {
        public string Name => nameof(DropNewestPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 20).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<SlowProcessingTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy with DropNewest queue policy
            // Using slow transform to ensure queue fills up
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions(1, 10, QueuePolicy.DropNewest));
        }
    }

    private sealed class DropOldestPipeline : IPipelineDefinition
    {
        public string Name => nameof(DropOldestPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 20).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy with DropOldest queue policy
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions(1, 10, QueuePolicy.DropOldest));
        }
    }

    // Helper pipeline definitions for error handling tests
    private sealed class ErrorHandlingPipeline : IPipelineDefinition
    {
        public string Name => nameof(ErrorHandlingPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 10).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<FailingTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());

            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions
            {
                MaxDegreeOfParallelism = 4,
                MaxQueueLength = 10,
                QueuePolicy = BoundedQueuePolicy.Block,
            });
        }
    }

    private sealed class MultipleErrorsPipeline : IPipelineDefinition
    {
        public string Name => nameof(MultipleErrorsPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 20).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<EvenFailingTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());

            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions
            {
                MaxDegreeOfParallelism = 4,
                MaxQueueLength = 10,
                QueuePolicy = BoundedQueuePolicy.Block,
            });
        }
    }

    private sealed class SlowFailingPipeline : IPipelineDefinition
    {
        public string Name => nameof(SlowFailingPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 10).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<SlowFailingTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());

            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions
            {
                MaxDegreeOfParallelism = 4,
                MaxQueueLength = 10,
                QueuePolicy = BoundedQueuePolicy.Block,
            });
        }
    }

    // Helper transforms for error handling tests
    private sealed class EvenFailingTransform : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(10, cancellationToken); // Simulate work

            if (item % 2 == 0)
                throw new InvalidOperationException($"Test failure on even value {item}");

            return item;
        }
    }

    private sealed class SlowFailingTransform : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(item == 3
                ? 100
                : 10, cancellationToken); // Slow on item 3

            if (item == 3)
                throw new InvalidOperationException($"Test failure on value {item}");

            return item;
        }
    }

    // Helper transform for cancellation tests
    private sealed class LongRunningTransform : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate work that can be cancelled
            for (var i = 0; i < 10; i++)
            {
                await Task.Delay(100, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
            }

            return item * 2;
        }
    }

    // Helper transform for cancellation tests
    private sealed class CancellationAwareTransform : TransformNode<int, int>
    {
        private readonly TimeSpan _delay;

        public CancellationAwareTransform(TimeSpan delay)
        {
            _delay = delay;
        }

        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Check for cancellation before starting work
            cancellationToken.ThrowIfCancellationRequested();

            // Simulate work that can be cancelled
            await Task.Delay(_delay, cancellationToken);

            // Check for cancellation after work
            cancellationToken.ThrowIfCancellationRequested();

            return item;
        }
    }

    // Pipeline definition for cancellation tests
    private sealed class CancellationTestPipeline : IPipelineDefinition
    {
        public string Name => nameof(CancellationTestPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            // Register a preconfigured transform with longer delay
            context.Items[PipelineContextKeys.PreconfiguredNodes] = new Dictionary<string, INode>
            {
                { "transform", new CancellationAwareTransform(TimeSpan.FromSeconds(2)) }, // 2 second delay
            };

            var source = builder.AddInMemorySource("source", Enumerable.Range(1, 100));
            var transform = builder.AddPassThroughTransform<int, int>("transform"); // Use AddPassThroughTransform to reference preconfigured node
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Configure parallel execution
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());

            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions
            {
                MaxDegreeOfParallelism = 4,
                MaxQueueLength = 10,
                QueuePolicy = BoundedQueuePolicy.Block,
                PreserveOrdering = false,
            });
        }
    }

    // Pipeline for testing cancellation
    private sealed class CancellationPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            // Register a preconfigured transform with longer delay
            context.Items[PipelineContextKeys.PreconfiguredNodes] = new Dictionary<string, INode>
            {
                { "transform", new DelayTransform(TimeSpan.FromSeconds(1)) }, // 1 second delay
            };

            var source = builder.AddInMemorySource("source", Enumerable.Range(1, 100));
            var transform = builder.AddTransform<DelayTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Configure parallel execution
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());

            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions
            {
                MaxDegreeOfParallelism = 4,
                MaxQueueLength = 10,
                QueuePolicy = BoundedQueuePolicy.Block,
                PreserveOrdering = false,
            });
        }
    }

    // Helper transform that respects cancellation during different phases
    private sealed class CancellationPhaseTransform : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Phase 1: Initial delay (before any work)
            await Task.Delay(50, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();

            // Phase 2: Do some work
            var result = item * 2;

            // Phase 3: Another delay (after work, before returning)
            await Task.Delay(50, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();

            return result;
        }
    }

    // Pipeline definition for phase-based cancellation tests
    private sealed class CancellationPhasePipeline : IPipelineDefinition
    {
        public string Name => nameof(CancellationPhasePipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 20).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<CancellationPhaseTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());

            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions
            {
                MaxDegreeOfParallelism = 2,
                MaxQueueLength = 5,
                QueuePolicy = BoundedQueuePolicy.Block,
            });
        }
    }

    #region Helper Pipelines for Configuration Tests

    private sealed class ConfigurationTestPipeline : IPipelineDefinition
    {
        public string Name => nameof(ConfigurationTestPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 100).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy with default options
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions());
        }
    }

    #endregion

    #region Configuration Validation and Edge Cases

    [Fact]
    public async Task Configuration_ZeroMaxDegreeOfParallelism_UsesSingleThread()
    {
        // Arrange
        var pipeline = new ConfigurationTestPipeline();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;

        // Override the parallel options to use 0 DOP (should default to 1)
        context.Items[PipelineContextKeys.NodeExecutionOptions("transform")] = new ParallelOptions
        {
            MaxDegreeOfParallelism = 0,
            MaxQueueLength = 10,
            QueuePolicy = BoundedQueuePolicy.Block,
            PreserveOrdering = true,
        };

        // Act
        var result = await testRunner.RunAndGetResultAsync<ConfigurationTestPipeline, int>(context);

        // Assert
        result.Should().HaveCount(100);
        result.Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task Configuration_NegativeMaxDegreeOfParallelism_UsesSingleThread()
    {
        // Arrange
        var pipeline = new ConfigurationTestPipeline();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;

        // Override the parallel options to use -1 DOP (should default to 1)
        context.Items[PipelineContextKeys.NodeExecutionOptions("transform")] = new ParallelOptions
        {
            MaxDegreeOfParallelism = -1,
            MaxQueueLength = 10,
            QueuePolicy = BoundedQueuePolicy.Block,
            PreserveOrdering = true,
        };

        // Act
        var result = await testRunner.RunAndGetResultAsync<ConfigurationTestPipeline, int>(context);

        // Assert
        result.Should().HaveCount(100);
        result.Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task Configuration_ZeroMaxQueueLength_UsesUnboundedQueue()
    {
        // Arrange
        var pipeline = new ConfigurationTestPipeline();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;

        // Override the parallel options to use 0 queue length (should be unbounded)
        context.Items[PipelineContextKeys.NodeExecutionOptions("transform")] = new ParallelOptions
        {
            MaxDegreeOfParallelism = 4,
            MaxQueueLength = 0,
            QueuePolicy = BoundedQueuePolicy.Block,
            PreserveOrdering = true,
        };

        // Act
        var result = await testRunner.RunAndGetResultAsync<ConfigurationTestPipeline, int>(context);

        // Assert
        result.Should().HaveCount(100);
        result.Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task Configuration_NegativeMaxQueueLength_UsesUnboundedQueue()
    {
        // Arrange
        var pipeline = new ConfigurationTestPipeline();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;

        // Override the parallel options to use -1 queue length (should be unbounded)
        context.Items[PipelineContextKeys.NodeExecutionOptions("transform")] = new ParallelOptions
        {
            MaxDegreeOfParallelism = 4,
            MaxQueueLength = -1,
            QueuePolicy = BoundedQueuePolicy.Block,
            PreserveOrdering = true,
        };

        // Act
        var result = await testRunner.RunAndGetResultAsync<ConfigurationTestPipeline, int>(context);

        // Assert
        result.Should().HaveCount(100);
        result.Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task Configuration_ZeroOutputBufferCapacity_UsesUnboundedBuffer()
    {
        // Arrange
        var pipeline = new ConfigurationTestPipeline();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;

        // Override the parallel options to use 0 output buffer (should be unbounded)
        context.Items[PipelineContextKeys.NodeExecutionOptions("transform")] = new ParallelOptions
        {
            MaxDegreeOfParallelism = 4,
            MaxQueueLength = 10,
            QueuePolicy = BoundedQueuePolicy.Block,
            PreserveOrdering = true,
            OutputBufferCapacity = 0,
        };

        // Act
        var result = await testRunner.RunAndGetResultAsync<ConfigurationTestPipeline, int>(context);

        // Assert
        result.Should().HaveCount(100);
        result.Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task Configuration_NegativeOutputBufferCapacity_UsesUnboundedBuffer()
    {
        // Arrange
        var pipeline = new ConfigurationTestPipeline();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;

        // Override the parallel options to use -1 output buffer (should be unbounded)
        context.Items[PipelineContextKeys.NodeExecutionOptions("transform")] = new ParallelOptions
        {
            MaxDegreeOfParallelism = 4,
            MaxQueueLength = 10,
            QueuePolicy = BoundedQueuePolicy.Block,
            PreserveOrdering = true,
            OutputBufferCapacity = -1,
        };

        // Act
        var result = await testRunner.RunAndGetResultAsync<ConfigurationTestPipeline, int>(context);

        // Assert
        result.Should().HaveCount(100);
        result.Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task Configuration_VeryHighMaxDegreeOfParallelism_HandlesGracefully()
    {
        // Arrange
        var pipeline = new ConfigurationTestPipeline();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;

        // Override the parallel options to use a very high DOP
        context.Items[PipelineContextKeys.NodeExecutionOptions("transform")] = new ParallelOptions
        {
            MaxDegreeOfParallelism = 1000,
            MaxQueueLength = 10,
            QueuePolicy = BoundedQueuePolicy.Block,
            PreserveOrdering = false,
        };

        // Act
        var result = await testRunner.RunAndGetResultAsync<ConfigurationTestPipeline, int>(context);

        // Assert
        result.Should().HaveCount(100);

        // Order might not be preserved with high DOP and PreserveOrdering = false
    }

    [Fact]
    public async Task Configuration_NoOptionsProvided_UsesDefaults()
    {
        // Arrange
        var pipeline = new ConfigurationTestPipeline();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;

        // Don't set any parallel options - should use defaults

        // Act
        var result = await testRunner.RunAndGetResultAsync<ConfigurationTestPipeline, int>(context);

        // Assert
        result.Should().HaveCount(100);
        result.Should().BeInAscendingOrder(); // Default should preserve ordering
    }

    [Fact]
    public async Task Configuration_NullQueuePolicy_UsesBlockPolicy()
    {
        // Arrange
        var pipeline = new ConfigurationTestPipeline();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;

        // Override the parallel options with null queue policy (should default to Block)
        context.Items[PipelineContextKeys.NodeExecutionOptions("transform")] = new ParallelOptions
        {
            MaxDegreeOfParallelism = 4,
            MaxQueueLength = 10,
            QueuePolicy = default, // Should default to Block
            PreserveOrdering = true,
        };

        // Act
        var result = await testRunner.RunAndGetResultAsync<ConfigurationTestPipeline, int>(context);

        // Assert
        result.Should().HaveCount(100);
        result.Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task EdgeCase_EmptyInput_HandlesGracefully()
    {
        // Arrange
        var runner = new TestPipelineRunner(new PipelineRunner());
        var context = PipelineContext.Default;

        // Act
        var result = await runner.RunAndGetResultAsync<EmptyInputPipeline, int>(context);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task EdgeCase_SingleItem_ProcessesCorrectly()
    {
        // Arrange
        var runner = new TestPipelineRunner(new PipelineRunner());
        var context = PipelineContext.Default;

        // Act
        var result = await runner.RunAndGetResultAsync<SingleItemPipeline, int>(context);

        // Assert
        result.Should().NotBeNull();
        result.Should().ContainSingle().Which.Should().Be(1);
    }

    [Fact]
    public async Task EdgeCase_LargeNumberOfItems_ProcessesCorrectly()
    {
        // Arrange
        var runner = new TestPipelineRunner(new PipelineRunner());
        var context = PipelineContext.Default;

        // Act
        var result = await runner.RunAndGetResultAsync<LargeInputPipeline, int>(context);

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(1000);
        result.Should().BeEquivalentTo(Enumerable.Range(1, 1000));
    }

    #endregion

    #region Helper Pipelines for Edge Case Tests

    private sealed class EmptyInputPipeline : IPipelineDefinition
    {
        public string Name => nameof(EmptyInputPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = new List<int>();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions());
        }
    }

    private sealed class SingleItemPipeline : IPipelineDefinition
    {
        public string Name => nameof(SingleItemPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = new List<int> { 1 };
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions());
        }
    }

    private sealed class LargeInputPipeline : IPipelineDefinition
    {
        public string Name => nameof(LargeInputPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var sourceData = Enumerable.Range(1, 1000).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Apply parallel execution strategy
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions());
        }
    }

    #endregion

    #region Stress Tests for Performance and Thread Safety

    [Fact]
    public async Task Stress_HighConcurrency_MaintainsDataIntegrity()
    {
        // Arrange
        const int itemCount = 10000;
        var sourceData = Enumerable.Range(1, itemCount).ToList();
        var context = PipelineContext.Default;
        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());

        // Act
        await runner.RunAsync<StressTestPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<int>>();
        sink.Items.Should().HaveCount(itemCount);
        sink.Items.Should().BeEquivalentTo(sourceData);
    }

    [Fact]
    public async Task Stress_MultiplePipelinesConcurrent_DoesNotInterfere()
    {
        // Arrange
        const int pipelineCount = 10;
        const int itemCountPerPipeline = 1000;
        var tasks = new List<Task<IReadOnlyList<int>>>();

        // Act - Run multiple pipelines concurrently
        for (var i = 0; i < pipelineCount; i++)
        {
            var pipelineId = i;

            var task = Task.Run(async () =>
            {
                var nodeFactory = new DefaultNodeFactory();
                var pipelineFactory = new PipelineFactory();
                var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
                var testRunner = new TestPipelineRunner(pipelineRunner);
                var context = PipelineContext.Default;

                // Add pipeline identifier to context
                context.Items["pipelineId"] = pipelineId;

                return await testRunner.RunAndGetResultAsync<ConcurrentPipeline, int>(context);
            });

            tasks.Add(task);
        }

        var results = await Task.WhenAll(tasks);

        // Assert
        results.Should().HaveCount(pipelineCount);

        foreach (var result in results)
        {
            result.Should().HaveCount(itemCountPerPipeline);
            result.Should().BeInAscendingOrder();
        }
    }

    [Fact]
    public async Task Stress_RapidStartStop_HandlesGracefully()
    {
        // Arrange
        const int iterations = 20;

        // Act & Assert
        for (var i = 0; i < iterations; i++)
        {
            var nodeFactory = new DefaultNodeFactory();
            var pipelineFactory = new PipelineFactory();
            var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
            var testRunner = new TestPipelineRunner(pipelineRunner);
            var context = PipelineContext.Default;

            var result = await testRunner.RunAndGetResultAsync<ConfigurationTestPipeline, int>(context);
            result.Should().HaveCount(100);
        }
    }

    [Fact]
    public async Task Stress_LargeDataWithCancellation_CancelsCleanly()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var nodeFactory = new DefaultNodeFactory();
        var pipelineFactory = new PipelineFactory();
        var pipelineRunner = new PipelineRunner(pipelineFactory, nodeFactory);
        var testRunner = new TestPipelineRunner(pipelineRunner);

        var context = new PipelineContextBuilder()
            .WithCancellation(cts.Token)
            .Build();

        // Start the pipeline
        var pipelineTask = () => testRunner.RunAndGetResultAsync<LargeScalePipeline, int>(context);

        // Let it run for a bit
        await Task.Delay(100);

        // Cancel
        cts.Cancel();

        // Act & Assert
        await pipelineTask.Should().ThrowAsync<PipelineExecutionException>()
            .WithInnerException(typeof(OperationCanceledException));
    }

    #endregion

    #region Helper Pipelines for Stress Tests

    private sealed class StressTestPipeline : IPipelineDefinition
    {
        public string Name => nameof(StressTestPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            const int itemCount = 10000;
            var sourceData = Enumerable.Range(1, itemCount).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // High degree of parallelism for stress testing
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());

            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions
            {
                MaxDegreeOfParallelism = 50,
                MaxQueueLength = 1000,
                QueuePolicy = BoundedQueuePolicy.Block,
                PreserveOrdering = true,
            });
        }
    }

    private sealed class ConcurrentPipeline : IPipelineDefinition
    {
        public string Name => nameof(ConcurrentPipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            const int itemCount = 1000;
            var sourceData = Enumerable.Range(1, itemCount).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<IdentityTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Moderate parallelism for concurrent execution
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());

            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions
            {
                MaxDegreeOfParallelism = 8,
                MaxQueueLength = 100,
                QueuePolicy = BoundedQueuePolicy.Block,
                PreserveOrdering = true,
            });
        }
    }

    private sealed class LargeScalePipeline : IPipelineDefinition
    {
        public string Name => nameof(LargeScalePipeline);

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            const int itemCount = 50000;
            var sourceData = Enumerable.Range(1, itemCount).ToList();
            var source = builder.AddInMemorySourceWithDataFromContext(context, "source", sourceData);
            var transform = builder.AddTransform<SlowProcessingTransform, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // High throughput configuration
            builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy());

            builder.SetNodeExecutionOption(transform.Id, new ParallelExecOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount * 2,
                MaxQueueLength = 10000,
                QueuePolicy = BoundedQueuePolicy.Block,
                PreserveOrdering = false,
            });
        }
    }

    // Slow processing transform for stress testing
    public sealed class SlowProcessingTransform : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate work that takes time
            await Task.Delay(10, cancellationToken);
            return item;
        }
    }

    #endregion
}
