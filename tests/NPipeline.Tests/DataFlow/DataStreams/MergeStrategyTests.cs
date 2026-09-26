using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Strategies;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tests.Reliability.Behavior;

namespace NPipeline.Tests.DataFlow.DataStreams;

public sealed class MergeStrategyTests
{
    [Fact]
    public async Task Runner_WhenMultipleInputs_UsesConcatenateMergeStrategy()
    {
        // Arrange
        ServiceCollection services = new();
        _ = services.AddSingleton<ConcurrentQueue<string>>();
        _ = services.AddNPipeline(Assembly.GetExecutingAssembly());
        IServiceProvider provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.CreateDefault();

        // Act
        await runner.RunAsync<FanInTestPipeline<ConcatenateTestSink>>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<string>>();
        _ = resultStore.Should().HaveCount(4);
        _ = resultStore.Should().ContainInOrder("A1", "A2", "B1", "B2");
    }

    [Fact]
    public async Task Runner_WhenMultipleInputs_UsesInterleaveMergeStrategyByDefault()
    {
        // Arrange
        ServiceCollection services = new();
        _ = services.AddSingleton<ConcurrentQueue<string>>();
        _ = services.AddNPipeline(Assembly.GetExecutingAssembly());
        IServiceProvider provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.CreateDefault();

        // Act
        await runner.RunAsync<FanInTestPipeline<InterleaveTestSink>>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<string>>();
        _ = resultStore.Should().HaveCount(4);
        _ = resultStore.Should().Contain("A1");
        _ = resultStore.Should().Contain("A2");
        _ = resultStore.Should().Contain("B1");
        _ = resultStore.Should().Contain("B2");

        // For interleave, we cannot guarantee order, only presence
    }

    [Fact]
    public async Task FanIn_FailureSurfacesWhileOtherInputOpen()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var run = BehaviorPipeline.RunAsync(b =>
        {
            var a = b.AddSource<FailingSource, int>("a");
            var c = b.AddSource<StreamingSource<int>, int>("c");
            var k = b.AddSink<CollectingSink<int>, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(c.Id, StreamingSource<int>.Unbounded([5]))
                .AddPreconfiguredNodeInstance(k.Id, new CollectingSink<int>());
            _ = b.Connect(a, k).Connect(c, k);
        }, cancellationToken: cts.Token);
        var started = DateTime.UtcNow;
        Exception? thrown = null;
        try { await run; } catch (Exception ex) { thrown = ex; }
        (DateTime.UtcNow - started).Should().BeLessThan(TimeSpan.FromSeconds(5), "failure should surface promptly");
        thrown.Should().NotBeNull();
        thrown!.GetBaseException().Should().BeOfType<InvalidOperationException>();
    }

    /// <summary>
    ///     A consumer that leaves early must stop the producers: with a bounded merge buffer the producers may only run
    ///     a buffer's worth ahead of the consumer, instead of draining their whole upstream.
    /// </summary>
    [Fact]
    public async Task Interleave_EarlyConsumerExit_StopsTheProducers()
    {
        const int capacity = 4;
        var yielded = new YieldCounter();

        var merged = MergeStrategies.InterleaveBounded<int>(
            [CountingSource("a", yielded), CountingSource("b", yielded)], capacity, CancellationToken.None);

        var enumerator = merged.GetAsyncEnumerator();
        _ = await enumerator.MoveNextAsync();
        await enumerator.DisposeAsync();

        await Task.Delay(200);
        // Slack: the item handed to the consumer, plus one pending write and one counted-but-not-yet-written item per
        // producer.
        yielded.Count.Should().BeLessThanOrEqualTo(capacity + 5,
            "the producers must stop once the consumer leaves");

        // And they stay stopped.
        var afterSecondWait = yielded.Count;
        await Task.Delay(200);
        yielded.Count.Should().Be(afterSecondWait, "the producers must stay stopped once the consumer leaves");
    }

    /// <summary>
    ///     Disposing the merged stream early must dispose every input enumerator, so streams that hold resources are
    ///     released instead of leaking.
    /// </summary>
    [Fact]
    public async Task Interleave_EarlyConsumerExit_DisposesEveryInputEnumerator()
    {
        var first = new DisposeFlag();
        var second = new DisposeFlag();

        var merged = MergeStrategies.Interleave<int>(
            [RecordingSource("a", first), RecordingSource("b", second)], CancellationToken.None);

        var enumerator = merged.GetAsyncEnumerator();
        _ = await enumerator.MoveNextAsync();
        await enumerator.DisposeAsync();

        await Task.Delay(200);
        first.Disposed.Should().BeTrue("the first input enumerator must be disposed");
        second.Disposed.Should().BeTrue("the second input enumerator must be disposed");
    }

    /// <summary>
    ///     The global <c>merge.capacity</c> annotation must bound the merge buffer, so a fast source feeding a slow
    ///     multi-input sink cannot buffer the whole stream in memory.
    /// </summary>
    [Fact]
    public async Task GlobalMergeCapacity_AnnotationIsHonoured()
    {
        var source = new CountingYieldSource();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var slowSink = new SlowSink();

        var run = BehaviorPipeline.RunAsync(b =>
        {
            var a = b.AddSource<CountingYieldSource, int>("a");
            var c = b.AddSource<StreamingSource<int>, int>("c");
            var k = b.AddSink<SlowSink, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(a.Id, source)
                .AddPreconfiguredNodeInstance(c.Id, StreamingSource<int>.Of(Array.Empty<int>()))
                .AddPreconfiguredNodeInstance(k.Id, slowSink);
            _ = b.Connect(a, k).Connect(c, k);
            _ = b.SetGlobalAnnotation(ExecutionAnnotationKeys.GlobalMergeCapacityKey, 4);
        }, cancellationToken: cts.Token);

        // Let the fast source run ahead of the slow sink for a moment, then measure its lead.
        await Task.Delay(500);
        var lead = source.Yielded - slowSink.Consumed;
        _ = lead.Should().BeLessThanOrEqualTo(10, "a bounded merge buffer must stop the fast source running away");
        await cts.CancelAsync();
        try { await run; } catch { /* cancelled */ }
    }

    private static IDataStream CountingSource(string name, YieldCounter yielded) =>
        new DataStream<int>(ProduceForever(yielded), name);

    private static IDataStream RecordingSource(string name, DisposeFlag flag) =>
        new DataStream<int>(ProduceRecording(flag), name);

    private static async IAsyncEnumerable<int> ProduceForever(
        YieldCounter yielded, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var i = 0; ; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yielded.Increment();
            await Task.Yield();
            yield return i;
        }
    }

    private static async IAsyncEnumerable<int> ProduceRecording(
        DisposeFlag flag, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        try
        {
            var i = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                yield return i++;
                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            flag.Disposed = true;
        }
    }

    private sealed class DisposeFlag
    {
        public volatile bool Disposed;
    }

    private sealed class YieldCounter
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Increment() => Interlocked.Increment(ref _count);
    }

    private sealed class CountingYieldSource : SourceNode<int>
    {
        private int _yielded;

        public int Yielded => Volatile.Read(ref _yielded);

        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new DataStream<int>(Produce(this, cancellationToken), "counting");

        private static async IAsyncEnumerable<int> Produce(CountingYieldSource self,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            for (var i = 0; ; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = Interlocked.Increment(ref self._yielded);
                await Task.Yield();
                yield return i;
            }
        }
    }

    private sealed class FailingSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new DataStream<int>(Produce(), "failing");

        private static async IAsyncEnumerable<int> Produce()
        {
            await Task.Yield();
            yield return 1;
            throw new InvalidOperationException("source1 failed");
        }
    }

    private sealed class SlowSink : SinkNode<int>
    {
        private int _consumed;

        public int Consumed => Volatile.Read(ref _consumed);

        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                _ = Interlocked.Increment(ref _consumed);
                await Task.Delay(20, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private sealed class TestSourceNode1 : SourceNode<string>
    {
        public override IDataStream<string> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            string[] items = ["A1", "A2"];
            DataStream<string> pipe = new(items.ToAsyncEnumerable(), "TestStream1");
            return pipe;
        }
    }

    private sealed class TestSourceNode2 : SourceNode<string>
    {
        public override IDataStream<string> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            string[] items = ["B1", "B2"];
            DataStream<string> pipe = new(items.ToAsyncEnumerable(), "TestStream2");
            return pipe;
        }
    }

    [MergeStrategy(MergeType.Concatenate)]
    private sealed class ConcatenateTestSink(ConcurrentQueue<string> store) : SinkNode<string>
    {
        public override async Task ConsumeAsync(IDataStream<string> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    private sealed class InterleaveTestSink(ConcurrentQueue<string> store) : SinkNode<string>
    {
        public override async Task ConsumeAsync(IDataStream<string> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    private sealed class FanInTestPipeline<TSink> : IPipelineDefinition where TSink : ISinkNode<string>
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source1 = builder.AddSource<TestSourceNode1, string>("source1");
            var source2 = builder.AddSource<TestSourceNode2, string>("source2");
            var sink = builder.AddSink<TSink, string>("sink");
            _ = builder.Connect(source1, sink);
            _ = builder.Connect(source2, sink);
        }
    }
}
