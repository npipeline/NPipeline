using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using ParallelExecOptions = NPipeline.Extensions.Parallelism.ParallelOptions;

namespace NPipeline.Extensions.Parallelism.Tests;

/// <summary>
///     Behavioral tests for the channel-based <see cref="BlockingParallelStrategy" /> covering
///     ordering, unordered output, backpressure, and fault propagation.
/// </summary>
public class ParallelExecutionStrategyTests
{
    [Fact]
    public async Task OrderedParallelism_PreservesInputOrder_WithVariableLatency()
    {
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        await runner.RunAsync<OrderedPipeline>(ctx);

        GetOrderedSinkItems(ctx).Should().Equal(Enumerable.Range(0, 64).Select(i => i * 2));
    }

    [Fact]
    public async Task UnorderedParallelism_EmitsAllItems()
    {
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        await runner.RunAsync<UnorderedPipeline>(ctx);

        var sink = ctx.GetSink<InMemorySinkNode<int>>();
        sink.Items.Should().HaveCount(64);
        sink.Items.Should().BeEquivalentTo(Enumerable.Range(0, 64).Select(i => i * 2));
    }

    [Fact]
    public async Task OrderedParallelism_WithBoundedWindow_ProcessesAllItemsInOrder()
    {
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        await runner.RunAsync<BoundedWindowPipeline>(ctx);

        GetOrderedSinkItems(ctx).Should().Equal(Enumerable.Range(0, 64).Select(i => i * 2));
    }

    [Fact]
    public async Task OrderedParallelism_WithOutputBuffer_ProcessesAllItemsInOrder()
    {
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        await runner.RunAsync<OutputBufferPipeline>(ctx);

        GetOrderedSinkItems(ctx).Should().Equal(Enumerable.Range(0, 64).Select(i => i * 2));
    }

    [Fact]
    public async Task Parallelism_PropagatesWorkerExceptions()
    {
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        var act = async () => await runner.RunAsync<FaultingPipeline>(ctx);

        _ = await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException));
    }

    [Fact]
    public async Task UnorderedParallelism_PropagatesWorkerExceptions()
    {
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        var act = async () => await runner.RunAsync<UnorderedFaultingPipeline>(ctx);

        _ = await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException));
    }

    [Fact]
    public async Task OrderedParallelism_PropagatesSourceExceptions()
    {
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        var act = async () => await runner.RunAsync<OrderedSourceFaultingPipeline>(ctx);

        var thrown = await act.Should().ThrowAsync<Exception>();
        thrown.Which.GetBaseException().Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Be("source-boom");
    }

    [Fact]
    public async Task UnorderedParallelism_PropagatesSourceExceptions()
    {
        var ctx = PipelineContext.Default;
        var runner = PipelineRunner.Create();

        var act = async () => await runner.RunAsync<UnorderedSourceFaultingPipeline>(ctx);

        var thrown = await act.Should().ThrowAsync<Exception>();
        thrown.Which.GetBaseException().Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Be("source-boom");
    }

    [Fact]
    public void ParallelOptions_EnableInputWaitTiming_DefaultsToFalse()
    {
        var options = new ParallelExecOptions();

        options.EnableInputWaitTiming.Should().BeFalse();
    }

    [Fact]
    public void ParallelOptionsBuilder_EnableInputWaitTiming_SetsFlag()
    {
        var options = new ParallelOptionsBuilder()
            .EnableInputWaitTiming()
            .Build();

        options.EnableInputWaitTiming.Should().BeTrue();
    }

    [Fact]
    public void ParallelOptionsBuilder_AllowUnorderedOutput_DisablesOrdering()
    {
        var options = new ParallelOptionsBuilder()
            .AllowUnorderedOutput()
            .Build();

        options.PreserveOrdering.Should().BeFalse();
    }

    private static IReadOnlyList<int> GetOrderedSinkItems(PipelineContext ctx)
    {
        return ((OrderedSink)ctx.Items[OrderedSink.ContextKey]).Items;
    }

    private sealed class OrderedPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, 64));
            var t = builder.AddTransform<VariableDelayTransform, int, int>("Transform");
            var k = builder.AddSink<OrderedSink, int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new BlockingParallelStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(4));
        }
    }

    private sealed class UnorderedPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, 64));
            var t = builder.AddTransform<VariableDelayTransform, int, int>("Transform")
                .WithUnorderedParallelism(builder, 4);
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
        }
    }

    private sealed class BoundedWindowPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, 64));
            var t = builder.AddTransform<VariableDelayTransform, int, int>("Transform");
            var k = builder.AddSink<OrderedSink, int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new BlockingParallelStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(4, 8));
        }
    }

    private sealed class OutputBufferPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, 64));
            var t = builder.AddTransform<VariableDelayTransform, int, int>("Transform");
            var k = builder.AddSink<OrderedSink, int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new BlockingParallelStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(4, 8, OutputBufferCapacity: 4));
        }
    }

    private sealed class FaultingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, 64));
            var t = builder.AddTransform<FaultingTransform, int, int>("Transform");
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new BlockingParallelStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(4));
        }
    }

    private sealed class UnorderedFaultingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySourceWithDataFromContext(context, "Source", Enumerable.Range(0, 64));
            var t = builder.AddTransform<FaultingTransform, int, int>("Transform")
                .WithUnorderedParallelism(builder, 4);
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
        }
    }

    private sealed class OrderedSourceFaultingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddSource<FaultingSource, int>("Source");
            var t = builder.AddTransform<VariableDelayTransform, int, int>("Transform");
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithExecutionStrategy(t, new BlockingParallelStrategy());
            builder.SetNodeExecutionOption(t.Id, new ParallelExecOptions(4));
        }
    }

    private sealed class UnorderedSourceFaultingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddSource<FaultingSource, int>("Source");
            var t = builder.AddTransform<VariableDelayTransform, int, int>("Transform")
                .WithUnorderedParallelism(builder, 4);
            var k = builder.AddInMemorySink<int>("Sink");
            builder.Connect(s, t).Connect(t, k);
        }
    }

    public sealed class OrderedSink : SinkNode<int>
    {
        public const string ContextKey = "ParallelExecutionStrategyTests.OrderedSink";

        private readonly List<int> _items = [];

        public IReadOnlyList<int> Items => _items;

        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            context.Items[ContextKey] = this;

            await foreach (var item in input.WithCancellation(cancellationToken))
                _items.Add(item);
        }
    }

    public sealed class VariableDelayTransform : TransformNode<int, int>
    {
        public override async Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Earlier items wait longer so completion order diverges from input order.
            await Task.Delay((3 - (item % 4)) * 3, cancellationToken);
            return item * 2;
        }
    }

    public sealed class FaultingTransform : TransformNode<int, int>
    {
        public override async Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken);

            if (item == 13)
                throw new InvalidOperationException("boom");

            return item * 2;
        }
    }

    public sealed class FaultingSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new DataStream<int>(Enumerate(cancellationToken));
        }

        private static async IAsyncEnumerable<int> Enumerate([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            for (var i = 0; i < 64; i++)
            {
                await Task.Yield();

                if (i == 20)
                    throw new InvalidOperationException("source-boom");

                yield return i;
            }
        }
    }
}

