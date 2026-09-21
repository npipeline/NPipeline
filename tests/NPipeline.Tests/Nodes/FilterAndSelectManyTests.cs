using AwesomeAssertions;
using NPipeline.Attributes.Lineage;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes;

/// <summary>
///     Filtering was the one common operation the core could not express: <c>ITransformNode</c> returns exactly one
///     output per input, so dropping an item required hand-writing a stream transform — the most advanced node
///     abstraction for the most ordinary job. <c>AddFilter</c> and <c>AddSelectMany</c> close that gap.
/// </summary>
public sealed class FilterAndSelectManyTests
{
    [Fact]
    public async Task AddFilter_KeepsOnlyTheItemsThatSatisfyThePredicate()
    {
        var collected = await RunAsync(builder => builder.AddFilter((int n) => n % 2 == 0, "evens"));

        collected.Should().Equal([2, 4, 6]);
    }

    [Fact]
    public async Task AddFilter_WithAnAsyncPredicate_KeepsOnlyTheMatchingItems()
    {
        var collected = await RunAsync(builder => builder.AddFilter(
            async (int n, CancellationToken ct) =>
            {
                await Task.Yield();
                return n > 3;
            },
            "big"));

        collected.Should().Equal([4, 5, 6]);
    }

    [Fact]
    public async Task AddFilter_DroppingEverything_ProducesAnEmptyStream()
    {
        var collected = await RunAsync(builder => builder.AddFilter((int _) => false, "none"));

        collected.Should().BeEmpty();
    }

    [Fact]
    public async Task AddFilter_DropsItemsAsItReads_WithoutBufferingTheStream()
    {
        var trace = new List<string>();
        var consumed = new List<int>();

        var runner = PipelineRunner.Create();
        await using var context = PipelineContext.CreateDefault();

        await runner.RunAsync(
            new InlinePipeline<int>(
                builder => builder.AddFilter((int n) => n % 2 == 0, "evens"),
                trace,
                consumed),
            context,
            CancellationToken.None);

        consumed.Should().Equal([2, 4, 6]);

        // A buffering filter would emit every "produced" before the first "consumed". Interleaving proves it streams.
        var firstConsumed = trace.IndexOf("consumed 2");
        var lastProduced = trace.IndexOf("produced 6");
        firstConsumed.Should().BeGreaterThan(-1);
        firstConsumed.Should().BeLessThan(lastProduced, "the sink sees an item before the source is exhausted");
    }

    [Fact]
    public async Task AddSelectMany_ExpandsEachItem()
    {
        var collected = await RunAsync(builder => builder.AddSelectMany((int n) => Enumerable.Repeat(n, n <= 2 ? n : 0), "repeat"));

        collected.Should().Equal([1, 2, 2]);
    }

    [Fact]
    public async Task AddSelectMany_WithAnAsyncSelector_ExpandsEachItem()
    {
        var collected = await RunAsync(builder => builder.AddSelectMany(
            (int n, CancellationToken ct) => Expand(n, ct),
            "expand"));

        collected.Should().Equal([1, 10, 2, 20, 3, 30, 4, 40, 5, 50, 6, 60]);

        static async IAsyncEnumerable<int> Expand(int n, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();
            yield return n;
            yield return n * 10;
        }
    }

    [Fact]
    public void TheNodesDeclareTheirCardinality_SoLineageDoesNotReportAMismatch()
    {
        typeof(FilterNode<int>).GetCustomAttributes(typeof(TransformCardinalityAttribute), false)
            .OfType<TransformCardinalityAttribute>().Single().Cardinality
            .Should().Be(TransformCardinality.OneToZeroOrOne);

        typeof(SelectManyNode<int, int>).GetCustomAttributes(typeof(TransformCardinalityAttribute), false)
            .OfType<TransformCardinalityAttribute>().Single().Cardinality
            .Should().Be(TransformCardinality.OneToMany);
    }

    [Fact]
    public void TheNodesRunUnderTheStreamPassthroughStrategy_NotAPerItemOne()
    {
        new FilterNode<int>(_ => true).DefaultExecutionStrategy
            .Should().BeSameAs(StreamPassthroughExecutionStrategy.Instance);

        new SelectManyNode<int, int>(n => [n]).DefaultExecutionStrategy
            .Should().BeSameAs(StreamPassthroughExecutionStrategy.Instance);
    }

    [Fact]
    public async Task AFilterConfiguredWithAPerItemStrategy_IsRejectedWithAClearError()
    {
        var runner = PipelineRunner.Create();
        await using var context = PipelineContext.CreateDefault();

        var act = async () => await runner.RunAsync(
            new PerItemStrategyPipeline(),
            context,
            CancellationToken.None);

        (await act.Should().ThrowAsync<Exception>()).And.ToString().Should().Contain("NP0421");
    }

    private static async Task<List<int>> RunAsync(Func<PipelineBuilder, TransformNodeHandle<int, int>> addNode)
    {
        var consumed = new List<int>();
        var runner = PipelineRunner.Create();
        await using var context = PipelineContext.CreateDefault();

        await runner.RunAsync(new InlinePipeline<int>(addNode, null, consumed), context, CancellationToken.None);
        return consumed;
    }

    private sealed class InlinePipeline<T>(
        Func<PipelineBuilder, TransformNodeHandle<int, int>> addNode,
        List<string>? trace,
        List<int> consumed) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource(Produce, "numbers");
            var node = addNode(builder);

            var sink = builder.AddSink(
                (int item) =>
                {
                    trace?.Add($"consumed {item}");
                    consumed.Add(item);
                },
                "collect");

            _ = builder.Connect(source, node);
            _ = builder.Connect(node, sink);
        }

        private IEnumerable<int> Produce()
        {
            for (var i = 1; i <= 6; i++)
            {
                trace?.Add($"produced {i}");
                yield return i;
            }
        }
    }

    private sealed class PerItemStrategyPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource(() => new[] { 1, 2, 3 }, "numbers");
            var filter = builder.AddFilter((int n) => n > 1, "evens");
            var sink = builder.AddSink((int _) => { }, "collect");

            // A filter cannot run one item at a time: dropping an item is a property of the stream.
            _ = builder.WithExecutionStrategy(filter, new SequentialExecutionStrategy());

            _ = builder.Connect(source, filter);
            _ = builder.Connect(filter, sink);
        }
    }
}
