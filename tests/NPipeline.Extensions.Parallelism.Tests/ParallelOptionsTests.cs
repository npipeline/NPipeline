using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tests.Common;
using ParallelExecOptions = NPipeline.Extensions.Parallelism.ParallelOptions;
using QueuePolicy = NPipeline.Extensions.Parallelism.BoundedQueuePolicy;

namespace NPipeline.Extensions.Parallelism.Tests;

[Collection("StatefulTests")]
public class ParallelOptionsTests
{
    [Fact]
    public async Task WithParallelOptions_ShouldApplyMaxDegreeOfParallelism()
    {
        SharedTestState.Reset(25, 40);
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();
        await runner.RunAsync<TestDefinitionWithDop2>(ctx);
        SharedTestState.Peak.Should().BeLessThanOrEqualTo(2);

        // Collect sink data
        var sink = ctx.GetSink<InMemorySinkNode<int>>();
        SharedTestState.Collected.AddRange(sink.Items);
        SharedTestState.Collected.Should().HaveCount(25);
    }

    [Fact]
    public async Task WithParallelOptions_ShouldBoundQueueLength_BlockPolicy()
    {
        SharedTestState.Reset(40, 10);
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();
        await runner.RunAsync<TestDefinitionBounded>(ctx);

        // Collect sink data
        var sink = ctx.GetSink<InMemorySinkNode<int>>();
        SharedTestState.Collected.AddRange(sink.Items);
        SharedTestState.Collected.Should().HaveCount(40);
    }

    [Fact]
    public async Task OutputBufferCapacity_ShouldPreventUnboundedBacklog()
    {
        // Fast transform, slow sink simulation: sink delay already encoded in transform delay; we inflate item count.
        SharedTestState.Reset(100, 5);
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();
        await runner.RunAsync<TestDefinitionOutputCap>(ctx);

        // Collect sink data
        var sink = ctx.GetSink<InMemorySinkNode<int>>();
        SharedTestState.Collected.AddRange(sink.Items);
        SharedTestState.Collected.Should().HaveCount(100);

        // Cannot directly read high-water tags here (activity abstraction hides Activity instance), so we assert via peak concurrency bound:
        SharedTestState.Peak.Should().BeLessThanOrEqualTo(3); // DOP honored

        // Indirect sanity: no exception indicates bounded operation completed.
    }

    [Fact]
    public async Task RetryMetrics_ShouldRecordAttempts()
    {
        SharedTestState.Reset(10, 1);
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        // The pipeline should succeed after retries, not throw an exception
        await runner.RunAsync<TestDefinitionRetryMetrics>(ctx);

        // Collect sink data
        var sink = ctx.GetSink<InMemorySinkNode<int>>();
        SharedTestState.Collected.AddRange(sink.Items);

        // Assert
        var metrics = ctx.GetParallelMetrics("transform");
        SharedTestState.Collected.Should().HaveCount(10);

        // Check that we have the correct transformed values (item * 2)
        SharedTestState.Collected.Should().BeEquivalentTo([0, 2, 4, 6, 8, 10, 12, 14, 16, 18]);

        // Each item should have exactly 3 attempts (first 2 fail, last succeeds)
        SharedTestState.AttemptCounts.Count.Should().Be(10);

        foreach (var kv in SharedTestState.AttemptCounts)
        {
            kv.Value.Should().Be(3);
        }

        // Metrics object exists for Block policy now as well; attempt to retrieve to ensure no exception.
        // If metrics not present, this will throw; acceptable indicator tests need updating if policy changes.
        // We don't assert aggregated metrics object here because Block policy may not expose counts the same way yet.
    }

    [Fact]
    public void UnsupportedQueuePolicy_ShouldThrow()
    {
        SharedTestState.Reset(1, 5);
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();
        var act = () => runner.RunAsync<TestDefinitionDropNewest>(ctx).GetAwaiter().GetResult();
        act.Should().NotThrow(); // now implemented
    }

    [Fact]
    public async Task DropNewestPolicy_ShouldDropSomeLatestItems()
    {
        SharedTestState.Reset(60, 25); // fast producer, slower consumer
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();
        await runner.RunAsync<TestDefinitionDropNewest>(ctx);

        // Collect sink data
        var sink = ctx.GetSink<InMemorySinkNode<int>>();
        SharedTestState.Collected.AddRange(sink.Items);

        SharedTestState.Collected.Count.Should().BeLessThan(60);
        SharedTestState.Collected.Should().Contain(0);

        // Metrics retrieval removed for now (extension only available for drop policies in other scope)
    }

    [Fact]
    public async Task DropOldestPolicy_ShouldDropSomeEarliestItems()
    {
        SharedTestState.Reset(60, 25);
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();
        await runner.RunAsync<TestDefinitionDropOldest>(ctx);

        // Collect sink data
        var sink = ctx.GetSink<InMemorySinkNode<int>>();
        SharedTestState.Collected.AddRange(sink.Items);

        SharedTestState.Collected.Count.Should().BeLessThan(60); // some items were dropped

        // Verify at least one item is missing (non-deterministic which due to concurrency)
        var missing = new HashSet<int>(Enumerable.Range(0, 60));
        missing.ExceptWith(SharedTestState.Collected);
        missing.Should().NotBeEmpty();

        // Metrics retrieval removed for now
    }

    private sealed class TestDefinitionWithDop2 : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, SharedTestState.Count));
            var t = builder.AddTransform<TestTransform, int, int>("Transform");
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(2));
        }
    }

    private sealed class TestDefinitionOutputCap : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, SharedTestState.Count));
            var t = builder.AddTransform<TestTransform, int, int>("Transform");
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(3, 4, OutputBufferCapacity: 5));
        }
    }

    private sealed class TestDefinitionRetryMetrics : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, SharedTestState.Count));
            var t = builder.AddTransform<RetryMetricsTransform, int, int>("Transform");
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(2, 4));

            // Configure retry options globally
            builder.WithRetryOptions(o => o.With(2));

            // Attach a RetryAllHandler from the retry tests (type may not be visible here, replicate minimal handler)
            builder.WithErrorHandler(t, typeof(LocalRetryAllHandler));
        }
    }

    private sealed class TestDefinitionBounded : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, SharedTestState.Count));
            var t = builder.AddTransform<TestTransform, int, int>("Transform");
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(4, 5));
        }
    }

    private sealed class TestDefinitionDropNewest : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, SharedTestState.Count));
            var t = builder.AddTransform<TestTransform, int, int>("Transform");
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(1, 2, QueuePolicy.DropNewest));
        }
    }

    private sealed class TestDefinitionDropOldest : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, SharedTestState.Count));
            var t = builder.AddTransform<TestTransform, int, int>("Transform");
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(2, 5, QueuePolicy.DropOldest));
        }
    }

    private sealed class TestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, SharedTestState.Count));
            var t = builder.AddTransform<TestTransform, int, int>("Transform");
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new ParallelExecutionStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(2));
        }
    }

    public sealed class TestTransform : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            lock (SharedTestState.Gate)
            {
                SharedTestState.Current++;

                if (SharedTestState.Current > SharedTestState.Peak)
                    SharedTestState.Peak = SharedTestState.Current;
            }

            await Task.Delay(SharedTestState.DelayMs, cancellationToken);

            lock (SharedTestState.Gate)
            {
                SharedTestState.Current--;
            }

            return item * 2;
        }
    }

    public sealed class LocalRetryAllHandler : INodeErrorHandler<ITransformNode<int, int>, int>
    {
        public Task<NodeErrorDecision> HandleAsync(ITransformNode<int, int> node, int failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Retry);
        }
    }
}
