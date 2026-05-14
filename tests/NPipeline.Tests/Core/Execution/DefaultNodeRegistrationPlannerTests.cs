using AwesomeAssertions;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline.Internals;

namespace NPipeline.Tests.Core.Execution;

public sealed class DefaultNodeRegistrationPlannerTests
{
    [Fact]
    public async Task BuildCustomMergeDelegate_ForTypedNode_ReturnsExecutableDelegate()
    {
        var planner = new DefaultNodeRegistrationPlanner();
        var mergeDelegate = planner.BuildCustomMergeDelegate(typeof(TypedCustomMergeNode));

        var pipes = new IDataStream[]
        {
            new DataStream<string>(new[] { "A1", "A2" }.ToAsyncEnumerable(), "left"),
            new DataStream<string>(new[] { "B1" }.ToAsyncEnumerable(), "right"),
        };

        var merged = await mergeDelegate(new TypedCustomMergeNode(), pipes, CancellationToken.None);
        var items = new List<object?>();

        await foreach (var item in merged.ToAsyncEnumerable())
        {
            items.Add(item);
        }

        items.Should().ContainInOrder("A1", "A2", "B1");
    }

    [Fact]
    public async Task BuildCustomMergeDelegate_ForUntypedNode_ReturnsExecutableDelegate()
    {
        var planner = new DefaultNodeRegistrationPlanner();
        var mergeDelegate = planner.BuildCustomMergeDelegate(typeof(UntypedCustomMergeNode));

        var pipes = new IDataStream[]
        {
            new DataStream<int>(new[] { 1 }.ToAsyncEnumerable(), "one"),
            new DataStream<int>(new[] { 2 }.ToAsyncEnumerable(), "two"),
        };

        var merged = await mergeDelegate(new UntypedCustomMergeNode(), pipes, CancellationToken.None);
        var items = new List<object?>();

        await foreach (var item in merged.ToAsyncEnumerable())
        {
            items.Add(item);
        }

        items.Should().ContainInOrder(2, 1);
    }

    [Fact]
    public void BuildCustomMergeDelegate_ForSameType_UsesCache()
    {
        var planner = new DefaultNodeRegistrationPlanner();

        var first = planner.BuildCustomMergeDelegate(typeof(TypedCustomMergeNode));
        var second = planner.BuildCustomMergeDelegate(typeof(TypedCustomMergeNode));

        ReferenceEquals(first, second).Should().BeTrue();
    }

    [Fact]
    public void PrepareNode_ForJoinNode_PrecompilesKeySelectors()
    {
        var planner = new DefaultNodeRegistrationPlanner();
        planner.PrepareNode(NodeKind.Join, typeof(PlannerJoinNode));

        var found = JoinKeySelectorRegistry.TryGetSelectors(typeof(PlannerJoinNode), out var selector1, out var selector2);

        found.Should().BeTrue();
        selector1.Should().NotBeNull();
        selector2.Should().NotBeNull();
    }

    private sealed class TypedCustomMergeNode : ICustomMergeNode<string>
    {
        public async Task<IDataStream<string>> MergeAsync(IEnumerable<IDataStream> pipes, CancellationToken cancellationToken)
        {
            var merged = new List<string>();

            foreach (var pipe in pipes.OfType<IDataStream<string>>())
            {
                await foreach (var item in pipe.WithCancellation(cancellationToken))
                {
                    merged.Add(item);
                }
            }

            return new DataStream<string>(merged.ToAsyncEnumerable(), "typed-merge");
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class UntypedCustomMergeNode : ICustomMergeNodeUntyped
    {
        public async Task<IDataStream> MergeAsyncUntyped(IEnumerable<IDataStream> pipes, CancellationToken cancellationToken)
        {
            var merged = new List<object?>();

            foreach (var pipe in pipes.Reverse())
            {
                await foreach (var item in pipe.ToAsyncEnumerable(cancellationToken).WithCancellation(cancellationToken))
                {
                    merged.Add(item);
                }
            }

            return new DataStream<object?>(merged.ToAsyncEnumerable(), "untyped-merge");
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    [KeySelector(typeof(PlannerLeft), nameof(PlannerLeft.Id))]
    [KeySelector(typeof(PlannerRight), nameof(PlannerRight.Id))]
    private sealed class PlannerJoinNode : KeyedJoinNode<int, PlannerLeft, PlannerRight, int>
    {
        public override int CreateOutput(PlannerLeft item1, PlannerRight item2)
        {
            return item1.Id + item2.Id;
        }
    }

    private sealed record PlannerLeft(int Id);

    private sealed record PlannerRight(int Id);
}