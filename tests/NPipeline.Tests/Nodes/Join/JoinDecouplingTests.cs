using AwesomeAssertions;
using NPipeline.Attributes.Nodes;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Join;

public sealed class JoinDecouplingTests
{
    [Fact]
    public void KeyedJoinNode_IsNotATransformationNode()
    {
        var interfaces = typeof(DummyJoinNode).GetInterfaces();

        var implementsTransformation =
            interfaces.Any(i => i == typeof(ITransformNode) ||
                                (i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ITransformNode<,>)));

        implementsTransformation.Should().BeFalse("join nodes must be decoupled from transform nodes");
    }

    [Fact]
    public void PipelineBuilder_WithExecutionStrategy_OnJoinHandle_Throws()
    {
        var builder = new PipelineBuilder().WithoutExtendedValidation();

        var handle = builder.AddJoin<DummyJoinNode, Left, Right, int>("dummy-join");

        Action act = () => builder.WithExecutionStrategy(handle, new SequentialExecutionStrategy());

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*NP0401*");
    }

    [KeySelector(typeof(Left), nameof(Left.Id))]
    [KeySelector(typeof(Right), nameof(Right.Id))]
    private sealed class DummyJoinNode : KeyedJoinNode<int, Left, Right, int>
    {
        public override int CreateOutput(Left item1, Right item2)
        {
            return item1.Id + item2.Id;
        }

        public override int CreateOutputFromLeft(Left item1)
        {
            return item1.Id;
        }

        public override int CreateOutputFromRight(Right item2)
        {
            return item2.Id;
        }
    }

    private sealed record Left(int Id);

    private sealed record Right(int Id);
}
