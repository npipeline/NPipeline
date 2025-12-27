using System.Reflection;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Validation.BuilderRules;

public sealed class BuiltInRulesTests
{
    [Fact]
    public void MissingSinkRule_Fires()
    {
        var b = new PipelineBuilder();
        var src = b.AddSource<IntSource, int>("s");
        var ok = b.TryBuild(out _, out var result);
        ok.Should().BeFalse();
        result.Issues.Should().Contain(i => i.Category == "Structure" && i.Message.Contains("no sink"));
    }

    [Fact]
    public void SelfLoopRule_Fires()
    {
        var b = new PipelineBuilder();
        var src = b.AddSource<IntSource, int>("s");
        var connectionState = typeof(PipelineBuilder).GetProperty("ConnectionState", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var state = connectionState.GetValue(b)!;
        var edgesProperty = state.GetType().GetProperty("Edges");
        var edges = (List<Edge>)edgesProperty!.GetValue(state)!;
        edges.Add(new Edge(src.Id, src.Id));
        var ok = b.TryBuild(out _, out var result);
        ok.Should().BeFalse();
        result.Issues.Should().Contain(i => i.Message.Contains("Self-loop"));
    }

    [Fact]
    public void DuplicateEdgeRule_Fires()
    {
        var b = new PipelineBuilder();
        var s = b.AddSource<IntSource, int>("s");
        var t = b.AddTransform<IntToString, int, string>("t");
        b.Connect(s, t);
        b.Connect(s, t); // duplicate

        // Add a sink to avoid triggering MissingSinkRule so we isolate duplicate edge error.
        var sink = b.AddSink<StringSink, string>("sink");
        b.Connect(t, sink);
        var ok = b.TryBuild(out _, out var result);
        ok.Should().BeFalse();
        result.Issues.Should().Contain(i => i.Message.Contains("Duplicate edge"));
    }

    [Fact]
    public void MultiInboundNonJoinRule_NoLongerFires()
    {
        // MultiInboundNonJoinRule was removed from extended validation because all nodes support
        // multiple inputs via the default Interleave merge strategy. This test now verifies that
        // multi-inbound scenarios are accepted.
        var b = new PipelineBuilder();
        var s1 = b.AddSource<IntSource, int>("s1");
        var s2 = b.AddSource<IntSource, int>("s2");
        var t = b.AddTransform<IntToString, int, string>("t");
        b.Connect(s1, t);
        b.Connect(s2, t); // second inbound -> now valid (uses default Interleave strategy)

        var sink = b.AddSink<StringSink, string>("sink");
        b.Connect(t, sink);
        var ok = b.TryBuild(out _, out var result);
        ok.Should().BeTrue();
        result.Issues.Should().BeEmpty();
    }

    [Fact]
    public void TypeCompatibilityRule_Fires()
    {
        var b = new PipelineBuilder();
        var s = b.AddSource<IntSource, int>("s");
        var str = b.AddTransform<IntToString, int, string>("tx");
        var bad = b.AddSink<BadSink, int>("bad"); // expects int
        b.Connect(s, bad); // valid edge
        b.Connect(s, str); // normal

        // Inject invalid edge: str (string) -> bad (int)
        var connectionState = typeof(PipelineBuilder).GetProperty("ConnectionState", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var state = connectionState.GetValue(b)!;
        var edgesProperty = state.GetType().GetProperty("Edges");
        var edges = (List<Edge>)edgesProperty!.GetValue(state)!;
        edges.Add(new Edge(str.Id, bad.Id));
        var ok = b.TryBuild(out _, out var result);
        ok.Should().BeFalse();
        result.Issues.Should().Contain(i => i.Category == "Types" && i.Message.Contains("Type mismatch"));
    }

    private sealed class IntSource : ISourceNode<int>
    {
        public IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            IDataPipe<int> pipe = new StreamingDataPipe<int>(Stream());

            return pipe;

            static async IAsyncEnumerable<int> Stream()
            {
                yield return 1;

                await Task.CompletedTask;
            }
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class IntToString : ITransformNode<int, string>
    {
        public IExecutionStrategy ExecutionStrategy { get; set; } = new SequentialExecutionStrategy();
        public INodeErrorHandler? ErrorHandler { get; set; }

        public Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item.ToString());
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class StringSink : ISinkNode<string>
    {
        public Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }


        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class BadSink : ISinkNode<int>
    {
        public Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }


        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
