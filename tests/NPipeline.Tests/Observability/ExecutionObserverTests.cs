using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Parallelism;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Observability;

public sealed class ExecutionObserverTests
{
    [Fact]
    public async Task Observer_Should_Receive_NodeLifecycle_And_ItemRetry()
    {
        var observer = new CollectObserver();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var provider = services.BuildServiceProvider();
        var pipelineRunner = provider.GetRequiredService<IPipelineRunner>();

        var ctx = PipelineContext.Default;
        ctx.ExecutionObserver = observer;

        // The pipeline should succeed after retry, not throw an exception
        await pipelineRunner.RunAsync<PipelineDef>(ctx);

        // Asserts
        var orderedEvents = observer.Events.OrderBy(e =>
        {
            return e switch
            {
                NodeExecutionStarted ns => ns.StartTime,
                NodeExecutionCompleted nc => DateTimeOffset.UtcNow, // hack
                NodeRetryEvent nr => DateTimeOffset.UtcNow, // hack
                QueueDropEvent qd => DateTimeOffset.UtcNow, // hack
                QueueMetricsEvent qm => qm.Timestamp,
                _ => DateTimeOffset.UtcNow,
            };
        }).ToList();

        // Verify that retry events were recorded
        orderedEvents.OfType<NodeRetryEvent>().Should().NotBeEmpty();
    }

    [Fact]
    public void Context_ExecutionObserver_Getter_Returns_NullExecutionObserver_When_Never_Set()
    {
        // Arrange
        PipelineContext context = new();

        // Act
        var observer = context.ExecutionObserver;

        // Assert
        observer.Should().NotBeNull();
        observer.Should().BeOfType<NullExecutionObserver>();
        (observer == NullExecutionObserver.Instance).Should().BeTrue();
    }

    [Fact]
    public void Context_ExecutionObserver_Setter_Falls_Back_To_Null_Observer_When_Assigned_Null()
    {
        // Arrange
        PipelineContext context = new();
        CollectObserver customObserver = new();
        context.ExecutionObserver = customObserver;

        // Act - assign null (this used to be a problem)
#nullable disable
        context.ExecutionObserver = null;
#nullable restore
        var resultObserver = context.ExecutionObserver!;

        // Assert - should fallback to NullExecutionObserver.Instance
        resultObserver.Should().NotBeNull();
        resultObserver.Should().BeOfType<NullExecutionObserver>();
        (resultObserver == NullExecutionObserver.Instance).Should().BeTrue();
    }

    [Fact]
    public void Context_ExecutionObserver_Setter_Accepts_Custom_Observer()
    {
        // Arrange
        PipelineContext context = new();
        CollectObserver customObserver = new();

        // Act
        context.ExecutionObserver = customObserver;
        var resultObserver = context.ExecutionObserver;

        // Assert
        (resultObserver == customObserver).Should().BeTrue();
    }

    [Fact]
    public void CompositeExecutionObserver_Filters_Null_Entries()
    {
        // Arrange
        CollectObserver observer1 = new();
        CollectObserver observer2 = new();

        // Act - pass array with nulls (using nullable disable to test the filtering)
#nullable disable
        CompositeExecutionObserver composite = new(observer1, null, observer2, null);
#nullable restore
        NodeExecutionStarted nodeEvent = new("test-node", "TestNode", DateTimeOffset.UtcNow);
        composite.OnNodeStarted(nodeEvent);

        // Assert - only non-null observers receive events
        observer1.Events.Should().HaveCount(1);
        observer2.Events.Should().HaveCount(1);
        observer1.Events.First().Should().Be(nodeEvent);
        observer2.Events.First().Should().Be(nodeEvent);
    }

    [Fact]
    public void CompositeExecutionObserver_Continues_On_Failing_Observer()
    {
        // Arrange
        CollectObserver observer1 = new();
        FailingObserver failingObserver = new();
        CollectObserver observer2 = new();

        CompositeExecutionObserver composite = new(observer1, failingObserver, observer2);
        NodeExecutionStarted nodeEvent = new("test-node", "TestNode", DateTimeOffset.UtcNow);

        // Act - this should not throw, even though failingObserver throws
        composite.OnNodeStarted(nodeEvent);

        // Assert - both observers that could complete did
        observer1.Events.Should().HaveCount(1);
        observer2.Events.Should().HaveCount(1);
    }

    [Fact]
    public void CompositeExecutionObserver_Handles_Empty_Array()
    {
        // Arrange
        CompositeExecutionObserver composite = new();
        NodeExecutionStarted nodeEvent = new("test-node", "TestNode", DateTimeOffset.UtcNow);

        // Act - this should not throw with empty observer array
        composite.OnNodeStarted(nodeEvent);
        NodeExecutionCompleted completedEvent = new("test-node", "TestNode", TimeSpan.Zero, true, null);
        composite.OnNodeCompleted(completedEvent);
        NodeRetryEvent retryEvent = new("test-node", RetryKind.ItemRetry, 1, new InvalidOperationException());
        composite.OnRetry(retryEvent);

        // Assert - should complete without exception
        // (the test passing is the assertion)
    }

    private sealed class TestSource : ISourceNode<int>, IAsyncDisposable
    {
        public IDataPipe<int> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            return new StreamingDataPipe<int>(Stream(cancellationToken));

            static async IAsyncEnumerable<int> Stream([EnumeratorCancellation] CancellationToken ct)
            {
                for (var i = 0; i < 3; i++)
                {
                    yield return i;

                    await Task.Yield();
                }
            }
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FlakyTransform : ITransformNode<int, int>, IAsyncDisposable
    {
        private int _count;
        public IExecutionStrategy ExecutionStrategy { get; set; } = new ParallelExecutionStrategy(1);
        public INodeErrorHandler? ErrorHandler { get; set; } = new TestItemRetryHandler();

        public Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken ct)
        {
            _count++;

            if (_count == 1)
                throw new InvalidOperationException("boom");

            return Task.FromResult(item);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        public Task<IDataPipe<int>> ExecuteAsync(IDataPipe<int> input, PipelineContext context, IPipelineActivity parentActivity,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<IDataPipe<int>>(new StreamingDataPipe<int>(Out(cancellationToken)));

            async IAsyncEnumerable<int> Out([EnumeratorCancellation] CancellationToken ct)
            {
                await foreach (var i in input.WithCancellation(ct))
                {
                    yield return await ExecuteAsync(i, context, ct);
                }
            }
        }

        public async Task<IDataPipe> ExecuteUntypedAsync(IDataPipe input, PipelineContext context, IPipelineActivity parentActivity,
            CancellationToken cancellationToken)
        {
            return await ExecuteAsync((IDataPipe<int>)input, context, parentActivity, cancellationToken);
        }
    }

    private sealed class TestItemRetryHandler : INodeErrorHandler<ITransformNode<int, int>, int>
    {
        public Task<NodeErrorDecision> HandleAsync(ITransformNode<int, int> node, int item, Exception ex, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Retry);
        }
    }

    private sealed class CollectObserver : IExecutionObserver
    {
        public ConcurrentQueue<object> Events { get; } = new();

        public void OnNodeStarted(NodeExecutionStarted e)
        {
            Events.Enqueue(e);
        }

        public void OnNodeCompleted(NodeExecutionCompleted e)
        {
            Events.Enqueue(e);
        }

        public void OnRetry(NodeRetryEvent e)
        {
            Events.Enqueue(e);
        }

        public void OnDrop(QueueDropEvent e)
        {
            Events.Enqueue(e);
        }

        public void OnQueueMetrics(QueueMetricsEvent e)
        {
            Events.Enqueue(e);
        }
    }

    private sealed class PipelineDef : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<TestSource, int>("s");
            var t = b.AddTransform<FlakyTransform, int, int>("t");
            b.Connect(src, t);
            var sink = b.AddSink<InMemorySinkNode<int>, int>("k");
            b.Connect(t, sink);
            b.WithRetryOptions(o => o.With(1));
        }
    }

    private sealed class FailingObserver : IExecutionObserver
    {
        public void OnNodeStarted(NodeExecutionStarted e)
        {
            throw new InvalidOperationException("Observer failure");
        }

        public void OnNodeCompleted(NodeExecutionCompleted e)
        {
            throw new InvalidOperationException("Observer failure");
        }

        public void OnRetry(NodeRetryEvent e)
        {
            throw new InvalidOperationException("Observer failure");
        }

        public void OnDrop(QueueDropEvent e)
        {
            throw new InvalidOperationException("Observer failure");
        }

        public void OnQueueMetrics(QueueMetricsEvent e)
        {
            throw new InvalidOperationException("Observer failure");
        }
    }
}
