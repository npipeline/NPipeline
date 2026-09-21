using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Lineage.Tests;

/// <summary>
///     Covers the scoping of the lineage module.
///     <para>
///         The module used to build lineage adapters used to be a process-wide mutable static that
///         <c>AddNPipelineLineage()</c> assigned, so enabling lineage in one container silently changed every other
///         container and every pipeline in the process. It is now per-builder, supplied by the runner that will
///         execute the pipeline, which keeps containers independent and keeps build-time adapters and runtime lineage
///         handling on the same instance.
///     </para>
/// </summary>
public sealed class LineageModuleIsolationTests
{
    [Fact]
    public void EnablingLineageInOneContainer_DoesNotLeakIntoAnother()
    {
        using var withLineage = BuildProvider(addLineage: true);
        using var withoutLineage = BuildProvider(addLineage: false);

        _ = withLineage.GetRequiredService<ILineage>().SupportsItemLevelLineage.Should().BeTrue();

        _ = withoutLineage.GetRequiredService<ILineage>().SupportsItemLevelLineage.Should()
            .BeFalse("a container that never asked for lineage must not inherit another container's choice");
    }

    [Fact]
    public void EnablingLineageInOneContainer_DoesNotLeakIntoAStandaloneBuilder()
    {
        using var withLineage = BuildProvider(addLineage: true);
        _ = withLineage.GetRequiredService<ILineage>().SupportsItemLevelLineage.Should().BeTrue();

        // A builder created directly, with no module supplied, tracks nothing regardless of container state.
        var standalone = new PipelineBuilder().WithoutExtendedValidation();
        _ = standalone.Lineage.SupportsItemLevelLineage.Should().BeFalse();

        _ = standalone.Lineage.Should().BeSameAs(NullLineage.Instance);
    }

    [Fact]
    public async Task RunningThroughAContainerWithoutLineage_IsUnaffectedByAContainerThatHasIt()
    {
        // Order matters: the container that enables lineage is built first, which is what used to poison the static.
        using var withLineage = BuildProvider(addLineage: true);
        using var withoutLineage = BuildProvider(addLineage: false);

        using var scope = withoutLineage.CreateScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();

        var context = new PipelineContext();
        await runner.RunAsync<CapturingPipeline>(context);

        _ = CapturingPipeline.ObservedModule.Should().BeSameAs(
            NullLineage.Instance,
            "a run in a container without lineage must not pick up another container's module");
    }

    [Fact]
    public void BuilderResolvedFromAContainer_UsesThatContainersModule()
    {
        using var provider = BuildProvider(addLineage: true);
        using var scope = provider.CreateScope();

        var builder = scope.ServiceProvider.GetRequiredService<PipelineBuilder>();

        _ = builder.Lineage.Should().BeSameAs(scope.ServiceProvider.GetRequiredService<ILineage>());
    }

    [Fact]
    public async Task RunningAPipeline_BuildsAdaptersWithTheRunnersModule()
    {
        var lineage = new LineageService();
        var runner = new PipelineRunnerBuilder().WithLineage(lineage).Build();

        var context = new PipelineContext();
        await runner.RunAsync<CapturingPipeline>(context, CancellationToken.None);

        _ = CapturingPipeline.ObservedModule.Should().BeSameAs(
            lineage,
            "the builder must receive the very module the runner executes with, so the two cannot disagree");
    }

    [Fact]
    public async Task RunningAPipelineWithoutLineage_BuildsAdaptersWithTheNullModule()
    {
        var runner = PipelineRunner.Create();

        var context = new PipelineContext();
        await runner.RunAsync<CapturingPipeline>(context, CancellationToken.None);

        _ = CapturingPipeline.ObservedModule.Should().BeSameAs(NullLineage.Instance);
    }

    private static ServiceProvider BuildProvider(bool addLineage)
    {
        var services = new ServiceCollection();
        _ = services.AddLogging();
        _ = services.AddNPipeline();

        if (addLineage)
            _ = services.AddNPipelineLineage();

        return services.BuildServiceProvider();
    }

    private sealed class CapturingPipeline : IPipelineDefinition
    {
        public static ILineage? ObservedModule { get; private set; }

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            ObservedModule = builder.Lineage;

            var source = builder.AddSource<EmptySource, int>("source");
            var sink = builder.AddSink<DiscardSink, int>("sink");
            _ = builder.Connect(source, sink);
        }
    }

    private sealed class EmptySource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataStream<int>([1, 2, 3], "source");
        }
    }

    private sealed class DiscardSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Discard.
            }
        }
    }
}
