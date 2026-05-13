using System.Collections.Immutable;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Execution;

public sealed class RuntimePipelineBinderTests
{
    private readonly IRuntimePipelineBinder _binder = RuntimePipelineBinder.Instance;

    [Fact]
    public async Task BindAsync_ItemLevelLineageOverrideTrue_EnablesLineageAndAppliesCompleteDefaults()
    {
        // Arrange
        var graph = CreateGraph();
        var context = new PipelineContext();
        context.Properties[PipelineContextKeys.ItemLevelLineageEnabledOverride] = true;

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.Graph.Lineage.ItemLevelLineageEnabled.Should().BeTrue();
        _ = result.Graph.Lineage.LineageOptions.Should().NotBeNull();
        _ = result.Graph.Lineage.LineageOptions!.SampleEvery.Should().Be(1);
        _ = result.Graph.Lineage.LineageOptions.RedactData.Should().BeFalse();
    }

    [Fact]
    public async Task BindAsync_LineageOptionsOverrideFactory_UsesRuntimeOverriddenBaseline()
    {
        // Arrange
        var graph = CreateGraph();
        var context = new PipelineContext();
        context.Properties[PipelineContextKeys.ItemLevelLineageEnabledOverride] = true;
        context.Properties[PipelineContextKeys.LineageOptionsOverride] =
            (Func<LineageOptions?, LineageOptions?>)(options =>
                options is null
                    ? new LineageOptions(SampleEvery: 7, RedactData: true)
                    : options with { SampleEvery = 7, RedactData = true });

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.Graph.Lineage.ItemLevelLineageEnabled.Should().BeTrue();
        _ = result.Graph.Lineage.LineageOptions.Should().NotBeNull();
        _ = result.Graph.Lineage.LineageOptions!.SampleEvery.Should().Be(7);
        _ = result.Graph.Lineage.LineageOptions.RedactData.Should().BeTrue();
    }

    [Fact]
    public async Task BindAsync_ErrorHandlerAndDeadLetterConfiguredByType_ResolvesAndDecoratesDeadLetterSink()
    {
        // Arrange
        var graph = CreateGraph(
            pipelineErrorHandlerType: typeof(TestPipelineErrorHandler),
            deadLetterSinkType: typeof(TestDeadLetterSink));

        var errorHandlerFactory = A.Fake<IErrorHandlerFactory>();
        var resolvedErrorHandler = A.Fake<IPipelineErrorHandler>();
        var resolvedDeadLetterSink = A.Fake<IDeadLetterSink>();
        var decoratedDeadLetterSink = A.Fake<IDeadLetterSink>();

        _ = A.CallTo(() => errorHandlerFactory.CreateErrorHandler(typeof(TestPipelineErrorHandler)))
            .Returns(resolvedErrorHandler);

        _ = A.CallTo(() => errorHandlerFactory.CreateDeadLetterSink(typeof(TestDeadLetterSink)))
            .Returns(resolvedDeadLetterSink);

        var context = new PipelineContext(new PipelineContextConfiguration(
            ErrorHandlerFactory: errorHandlerFactory));

        context.Properties[PipelineContextKeys.DeadLetterSinkDecorator] =
            (Func<IDeadLetterSink?, IDeadLetterSink?>)(_ => decoratedDeadLetterSink);

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.PipelineErrorHandler.Should().BeSameAs(resolvedErrorHandler);
        _ = result.DeadLetterSink.Should().BeSameAs(decoratedDeadLetterSink);

        _ = A.CallTo(() => errorHandlerFactory.CreateErrorHandler(typeof(TestPipelineErrorHandler)))
            .MustHaveHappenedOnceExactly();

        _ = A.CallTo(() => errorHandlerFactory.CreateDeadLetterSink(typeof(TestDeadLetterSink)))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task BindAsync_ItemLevelLineageEnabled_ResolvesAndDecoratesLineageSink()
    {
        // Arrange
        var graph = CreateGraph(
            itemLevelLineageEnabled: true,
            lineageSinkType: typeof(TestLineageSink));

        var lineageFactory = A.Fake<ILineageFactory>();
        var resolvedLineageSink = A.Fake<ILineageSink>();
        var decoratedLineageSink = A.Fake<ILineageSink>();

        _ = A.CallTo(() => lineageFactory.CreateLineageSink(typeof(TestLineageSink)))
            .Returns(resolvedLineageSink);

        var context = new PipelineContext(new PipelineContextConfiguration(
            LineageFactory: lineageFactory));

        context.Properties[PipelineContextKeys.LineageSinkDecorator] =
            (Func<ILineageSink?, ILineageSink?>)(_ => decoratedLineageSink);

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.LineageSink.Should().BeSameAs(decoratedLineageSink);
        _ = A.CallTo(() => lineageFactory.CreateLineageSink(typeof(TestLineageSink)))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task BindAsync_ItemLevelLineageDisabled_DoesNotResolveLineageSink()
    {
        // Arrange
        var graph = CreateGraph(
            itemLevelLineageEnabled: false,
            lineageSinkType: typeof(TestLineageSink));

        var lineageFactory = A.Fake<ILineageFactory>();
        var context = new PipelineContext(new PipelineContextConfiguration(
            LineageFactory: lineageFactory));

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.LineageSink.Should().BeNull();
        A.CallTo(() => lineageFactory.CreateLineageSink(A<Type>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task BindAsync_NoExplicitPipelineLineageSink_UsesProviderFallbackWhenEnabled()
    {
        // Arrange
        var graph = CreateGraph(itemLevelLineageEnabled: true);

        var lineageFactory = A.Fake<ILineageFactory>();
        var provider = A.Fake<IPipelineLineageSinkProvider>();
        var providedSink = A.Fake<IPipelineLineageSink>();

        _ = A.CallTo(() => lineageFactory.ResolvePipelineLineageSinkProvider())
            .Returns(provider);

        var context = new PipelineContext(new PipelineContextConfiguration(
            LineageFactory: lineageFactory));

        _ = A.CallTo(() => provider.Create(context))
            .Returns(providedSink);

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.PipelineLineageSink.Should().BeSameAs(providedSink);
        _ = A.CallTo(() => lineageFactory.ResolvePipelineLineageSinkProvider())
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task BindAsync_ExplicitPipelineLineageSinkType_TakesPrecedenceOverProvider()
    {
        // Arrange
        var graph = CreateGraph(
            itemLevelLineageEnabled: true,
            pipelineLineageSinkType: typeof(TestPipelineLineageSink));

        var lineageFactory = A.Fake<ILineageFactory>();
        var provider = A.Fake<IPipelineLineageSinkProvider>();
        var explicitSink = A.Fake<IPipelineLineageSink>();

        _ = A.CallTo(() => lineageFactory.CreatePipelineLineageSink(typeof(TestPipelineLineageSink)))
            .Returns(explicitSink);

        _ = A.CallTo(() => lineageFactory.ResolvePipelineLineageSinkProvider())
            .Returns(provider);

        var context = new PipelineContext(new PipelineContextConfiguration(
            LineageFactory: lineageFactory));

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.PipelineLineageSink.Should().BeSameAs(explicitSink);
        _ = A.CallTo(() => lineageFactory.CreatePipelineLineageSink(typeof(TestPipelineLineageSink)))
            .MustHaveHappenedOnceExactly();

        A.CallTo(() => lineageFactory.ResolvePipelineLineageSinkProvider())
            .MustNotHaveHappened();
    }

    private static PipelineGraph CreateGraph(
        bool itemLevelLineageEnabled = false,
        LineageOptions? lineageOptions = null,
        Type? pipelineErrorHandlerType = null,
        Type? deadLetterSinkType = null,
        Type? lineageSinkType = null,
        Type? pipelineLineageSinkType = null)
    {
        return PipelineGraphBuilder.Create()
            .WithNodes(ImmutableArray<NodeDefinition>.Empty)
            .WithEdges(ImmutableArray<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .WithPipelineErrorHandlerType(pipelineErrorHandlerType)
            .WithDeadLetterSinkType(deadLetterSinkType)
            .WithItemLevelLineageEnabled(itemLevelLineageEnabled)
            .WithLineageSinkType(lineageSinkType)
            .WithPipelineLineageSinkType(pipelineLineageSinkType)
            .WithLineageOptions(lineageOptions)
            .Build();
    }

    private sealed class TestPipelineErrorHandler : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
        }
    }

    private sealed class TestDeadLetterSink : IDeadLetterSink
    {
        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestLineageSink : ILineageSink
    {
        public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestPipelineLineageSink : IPipelineLineageSink
    {
        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}