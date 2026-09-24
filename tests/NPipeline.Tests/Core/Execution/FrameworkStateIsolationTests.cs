using AwesomeAssertions;
using NPipeline.Execution;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Execution;

/// <summary>
///     <c>Items</c> and <c>Properties</c> are documented as the user's, but the framework used to keep its own
///     retry state, state services and per-node completion flags in them under interpolated magic strings.
///     A user key could collide with one and corrupt execution. Framework state now lives on typed members.
/// </summary>
public sealed class FrameworkStateIsolationTests
{
    [Fact]
    public async Task ARunLeavesTheUserBagsExactlyAsItFoundThem()
    {
        var runner = PipelineRunner.Create();
        await using var context = PipelineContext.CreateDefault();

        context.Items["mine"] = 1;
        context.Properties["also-mine"] = 2;

        await runner.RunAsync(new CountingPipeline(), context, CancellationToken.None);

        _ = context.Items.Should().ContainSingle().Which.Key.Should().Be("mine");
        _ = context.Properties.Should().ContainSingle().Which.Key.Should().Be("also-mine");
    }

    [Fact]
    public async Task NodeOutcomesAreRecordedAsTypedStatusesRatherThanFlagsInAContextBag()
    {
        var runner = PipelineRunner.Create();
        await using var context = PipelineContext.CreateDefault();

        await runner.RunAsync(new CountingPipeline(), context, CancellationToken.None);

        _ = context.NodeEnvironment.GetNodeStatus("numbers").Should().Be(NodeExecutionStatus.Completed);
        _ = context.NodeEnvironment.GetNodeStatus("collect").Should().Be(NodeExecutionStatus.Completed);
        _ = context.NodeEnvironment.GetNodeStatus("never-ran").Should().Be(NodeExecutionStatus.Pending);

        _ = context.NodeEnvironment.EnumerateNodeStatuses()
            .Select(kv => kv.Key)
            .Should().BeEquivalentTo("numbers", "collect");
    }

    [Fact]
    public async Task AFailedNodeIsRecordedAsFailed()
    {
        var runner = PipelineRunner.Create();
        await using var context = PipelineContext.CreateDefault();

        var act = async () => await runner.RunAsync(new ThrowingPipeline(), context, CancellationToken.None);

        _ = await act.Should().ThrowAsync<Exception>();
        _ = context.NodeEnvironment.GetNodeStatus("boom").Should().Be(NodeExecutionStatus.Failed);
        _ = context.Properties.Should().BeEmpty();
    }

    private sealed class CountingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource(() => new[] { 1, 2, 3 }, "numbers");
            var sink = builder.AddSink((int _) => { }, "collect");

            _ = builder.Connect(source, sink);
        }
    }

    private sealed class ThrowingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource(() => new[] { 1 }, "numbers");
            var sink = builder.AddSink((int _) => throw new InvalidOperationException("boom"), "boom");

            _ = builder.Connect(source, sink);
        }
    }
}
