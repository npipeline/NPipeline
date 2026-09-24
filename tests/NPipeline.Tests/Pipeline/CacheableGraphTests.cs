using AwesomeAssertions;
using NPipeline.Attributes;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Pipeline;

/// <summary>
///     Building a graph costs a <c>Define</c> call, two frozen dictionaries, the child graphs and the full validation
///     rule set, and it used to happen on every run. <see cref="CacheableGraphAttribute" /> lets a definition opt into
///     paying that once.
///     <para>
///         The risk the attribute carries is the one finding 1 already found in the plan cache: anything the cached
///         graph holds is shared with every later run, and a later run must never be handed an object an earlier run
///         disposed. These tests pin both halves — that the build really is skipped, and that nothing per-run leaks
///         across with it.
///     </para>
/// </summary>
public sealed class CacheableGraphTests
{
    [Fact]
    public void MarkedDefinition_IsDefinedOnceAcrossBuilds()
    {
        using var recorder = new DefineRecorder();
        var factory = new PipelineFactory();

        _ = factory.Create<CacheablePipeline>(NewContext());
        _ = factory.Create<CacheablePipeline>(NewContext());
        _ = factory.Create<CacheablePipeline>(NewContext());

        recorder.CacheableDefineCalls.Should().Be(1);
    }

    [Fact]
    public void MarkedDefinition_ReturnsTheSameGraphInstance()
    {
        using var _ = new DefineRecorder();
        var factory = new PipelineFactory();

        var first = factory.Create<CacheablePipeline>(NewContext());
        var second = factory.Create<CacheablePipeline>(NewContext());

        second.Graph.Should().BeSameAs(first.Graph);
    }

    [Fact]
    public void UnmarkedDefinition_IsRebuiltEveryTime()
    {
        using var recorder = new DefineRecorder();
        var factory = new PipelineFactory();

        var first = factory.Create<PlainPipeline>(NewContext());
        var second = factory.Create<PlainPipeline>(NewContext());

        recorder.PlainDefineCalls.Should().Be(2);
        second.Graph.Should().NotBeSameAs(first.Graph);
    }

    [Fact]
    public void EachFactory_KeepsItsOwnCache()
    {
        using var _ = new DefineRecorder();

        var first = new PipelineFactory().Create<CacheablePipeline>(NewContext());
        var second = new PipelineFactory().Create<CacheablePipeline>(NewContext());

        second.Graph.Should().NotBeSameAs(first.Graph, "the cache is scoped to the factory, not to the process");
    }

    [Fact]
    public void MarkedDefinition_IsNotCachedAcrossDifferentLineageModules()
    {
        using var _ = new DefineRecorder();
        var factory = new PipelineFactory();

        var withNullLineage = NewContext();
        var withOtherLineage = NewContext();
        withOtherLineage.Lineage.Module = new NullLineage();

        var first = factory.Create<CacheablePipeline>(withNullLineage);
        var second = factory.Create<CacheablePipeline>(withOtherLineage);

        second.Graph.Should().NotBeSameAs(
            first.Graph,
            "lineage adapters are built from the run's module, so a graph must not cross between modules");
    }

    [Fact]
    public void MarkedDefinition_IsNotCached_WhenItRegistersNodeInstances()
    {
        using var recorder = new DefineRecorder();
        var factory = new PipelineFactory();

        var first = factory.Create<CacheableWithInstancePipeline>(NewContext());
        var second = factory.Create<CacheableWithInstancePipeline>(NewContext());

        recorder.InstanceDefineCalls.Should().Be(2, "instances registered at definition time are disposed with their run");
        second.Graph.Should().NotBeSameAs(first.Graph);
    }

    [Fact]
    public void MarkedDefinition_IsNotCached_WhenTheContextSuppliesNodeInstances()
    {
        using var recorder = new DefineRecorder();
        var factory = new PipelineFactory();

        var context = NewContext();
        context.NodeEnvironment.PreconfiguredNodeInstances["sink"] = new CollectingSink();

        _ = factory.Create<CacheablePipeline>(context);
        _ = factory.Create<CacheablePipeline>(NewContext());

        recorder.CacheableDefineCalls.Should().Be(2, "a context-supplied instance makes the graph specific to that run");
    }

    [Fact]
    public async Task MarkedDefinition_UsesFreshNodeInstancesOnEveryRun()
    {
        using var recorder = new DefineRecorder();
        var runner = PipelineRunner.Create();

        await runner.RunAsync<CacheablePipeline>(NewContext());
        await runner.RunAsync<CacheablePipeline>(NewContext());

        recorder.CacheableDefineCalls.Should().Be(1, "the graph is built once");
        recorder.SourceInstances.Should().Be(2, "but each run still instantiates its own nodes");
        recorder.SinkInstances.Should().Be(2);
        recorder.UseAfterDisposeCount.Should().Be(0, "a reused graph must never hand a run a disposed node");
    }

    [Fact]
    public async Task MarkedDefinition_ProducesTheSameOutputOnEveryRun()
    {
        using var recorder = new DefineRecorder();
        var runner = PipelineRunner.Create();

        await runner.RunAsync<CacheablePipeline>(NewContext());
        var firstRun = recorder.TakeConsumed();

        await runner.RunAsync<CacheablePipeline>(NewContext());
        var secondRun = recorder.TakeConsumed();

        firstRun.Should().Equal(0, 2, 4);
        secondRun.Should().Equal([0, 2, 4], "reusing the graph must not change observable behaviour");
    }

    private static PipelineContext NewContext() => new(new PipelineContextConfiguration(new Dictionary<string, object> { ["count"] = 3 }));

    /// <summary>
    ///     Ambient recorder shared by the definitions and nodes, which the framework instantiates itself.
    /// </summary>
    private sealed class DefineRecorder : IDisposable
    {
        private static readonly object Gate = new();
        private readonly List<int> _consumed = [];

        public DefineRecorder()
        {
            lock (Gate)
            {
                Current = this;
            }
        }

        public static DefineRecorder? Current { get; private set; }

        public int CacheableDefineCalls { get; private set; }

        public int PlainDefineCalls { get; private set; }

        public int InstanceDefineCalls { get; private set; }

        public int SourceInstances { get; private set; }

        public int SinkInstances { get; private set; }

        public int UseAfterDisposeCount { get; private set; }

        public void Dispose()
        {
            lock (Gate)
            {
                Current = null;
            }
        }

        public void RecordCacheableDefine()
        {
            lock (Gate)
            {
                CacheableDefineCalls++;
            }
        }

        public void RecordPlainDefine()
        {
            lock (Gate)
            {
                PlainDefineCalls++;
            }
        }

        public void RecordInstanceDefine()
        {
            lock (Gate)
            {
                InstanceDefineCalls++;
            }
        }

        public void RecordSourceConstructed()
        {
            lock (Gate)
            {
                SourceInstances++;
            }
        }

        public void RecordSinkConstructed()
        {
            lock (Gate)
            {
                SinkInstances++;
            }
        }

        public void RecordUseAfterDispose()
        {
            lock (Gate)
            {
                UseAfterDisposeCount++;
            }
        }

        public void RecordConsumed(int value)
        {
            lock (Gate)
            {
                _consumed.Add(value);
            }
        }

        public List<int> TakeConsumed()
        {
            lock (Gate)
            {
                var copy = new List<int>(_consumed);
                _consumed.Clear();
                return copy;
            }
        }
    }

    [CacheableGraph]
    private sealed class CacheablePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            DefineRecorder.Current?.RecordCacheableDefine();

            var source = builder.AddSource<CountingSource, int>("source");
            var doubler = builder.AddTransform<Doubler, int, int>("double");
            var sink = builder.AddSink<CollectingSink, int>("sink");
            builder.Connect(source, doubler).Connect(doubler, sink);
        }
    }

    private sealed class PlainPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            DefineRecorder.Current?.RecordPlainDefine();

            var source = builder.AddSource<CountingSource, int>("source");
            var sink = builder.AddSink<CollectingSink, int>("sink");
            builder.Connect(source, sink);
        }
    }

    /// <summary>
    ///     Marked cacheable but registers a node instance, which the factory must refuse to cache.
    /// </summary>
    [CacheableGraph]
    private sealed class CacheableWithInstancePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            DefineRecorder.Current?.RecordInstanceDefine();

            var source = builder.AddSource<CountingSource, int>("source");
            var sink = builder.AddSink<CollectingSink, int>("sink");
            _ = builder.AddPreconfiguredNodeInstance(sink.Id, new CollectingSink());
            builder.Connect(source, sink);
        }
    }

    private sealed class CountingSource : SourceNode<int>, IDisposable
    {
        private bool _disposed;

        public CountingSource()
        {
            DefineRecorder.Current?.RecordSourceConstructed();
        }

        public void Dispose()
        {
            _disposed = true;
        }

        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            if (_disposed)
                DefineRecorder.Current?.RecordUseAfterDispose();

            var count = context.Parameters.TryGetValue("count", out var value)
                ? Convert.ToInt32(value)
                : 0;

            return new DataStream<int>(Generate(count), "source");
        }

        private static async IAsyncEnumerable<int> Generate(int count)
        {
            await Task.Yield();

            for (var i = 0; i < count; i++)
            {
                yield return i;
            }
        }
    }

    private sealed class Doubler : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken) => new(input * 2);
    }

    private sealed class CollectingSink : SinkNode<int>, IDisposable
    {
        private bool _disposed;

        public CollectingSink()
        {
            DefineRecorder.Current?.RecordSinkConstructed();
        }

        public void Dispose()
        {
            _disposed = true;
        }

        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            if (_disposed)
                DefineRecorder.Current?.RecordUseAfterDispose();

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                DefineRecorder.Current?.RecordConsumed(item);
            }
        }
    }
}
