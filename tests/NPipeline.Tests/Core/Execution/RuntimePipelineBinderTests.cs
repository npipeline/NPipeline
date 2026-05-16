using System.Collections.Immutable;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Configuration;
using NPipeline.DataFlow.Routing;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

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
    public async Task BindAsync_ResiliencePolicyAndDeadLetterConfiguredByType_ResolvesAndDecoratesDeadLetterSink()
    {
        // Arrange
        var graph = CreateGraph(
            resiliencePolicyType: typeof(TestPipelineErrorHandler),
            deadLetterSinkType: typeof(TestDeadLetterSink));

        var errorHandlerFactory = A.Fake<IErrorHandlerFactory>();
        var resolvedDeadLetterSink = A.Fake<IDeadLetterSink>();
        var decoratedDeadLetterSink = A.Fake<IDeadLetterSink>();

        _ = A.CallTo(() => errorHandlerFactory.CreateDeadLetterSink(typeof(TestDeadLetterSink)))
            .Returns(resolvedDeadLetterSink);

        var context = new PipelineContext(new PipelineContextConfiguration(
            ErrorHandlerFactory: errorHandlerFactory));

        context.Properties[PipelineContextKeys.DeadLetterSinkDecorator] =
            (Func<IDeadLetterSink?, IDeadLetterSink?>)(_ => decoratedDeadLetterSink);

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.ResiliencePolicy.Should().BeOfType<TestPipelineErrorHandler>();
        _ = result.DeadLetterSink.Should().BeSameAs(decoratedDeadLetterSink);

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

    [Fact]
    public async Task BindAsync_LineageRouteOptions_NormalizesToRuntimeRouteOptionsAndAddsContract()
    {
        // Arrange
        const string nodeId = "route";
        var routeNode = new NodeDefinition(
            Id: nodeId,
            Name: nodeId,
            NodeType: typeof(object),
            Kind: NodeKind.Route,
            InputType: typeof(int),
            OutputType: typeof(int));

        var graph = PipelineGraphBuilder.Create()
            .WithNodes([routeNode])
            .WithEdges(ImmutableArray<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .WithItemLevelLineageEnabled(true)
            .WithNodeExecutionAnnotations(new Dictionary<string, object>
            {
                [ExecutionAnnotationKeys.RouteOptionsForNode(nodeId)] = new RouteOptions<int>()
                    .When("even", value => value % 2 == 0)
                    .Otherwise("odd"),
            })
            .Build();

        var context = new PipelineContext();

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        var routeKey = ExecutionAnnotationKeys.RouteOptionsForNode(nodeId);
        var contractKey = ExecutionAnnotationKeys.RuntimeStreamContractForNode(nodeId);
        var annotations = result.Graph.ExecutionOptions.NodeExecutionAnnotations!;

        _ = annotations[routeKey].Should().BeOfType<RouteOptions<LineagePacket<int>>>();

        var normalized = (RouteOptions<LineagePacket<int>>)annotations[routeKey];
        _ = normalized.Rules.Should().HaveCount(1);
        _ = normalized.Rules[0].Predicate(new LineagePacket<int>(2, Guid.NewGuid(), ImmutableList<string>.Empty)).Should().BeTrue();
        _ = normalized.Rules[0].Predicate(new LineagePacket<int>(3, Guid.NewGuid(), ImmutableList<string>.Empty)).Should().BeFalse();

        _ = annotations[contractKey].Should().BeOfType<RuntimeNodeStreamContract>();
        var contract = (RuntimeNodeStreamContract)annotations[contractKey];
        _ = contract.ItemLevelLineageEnabled.Should().BeTrue();
        _ = contract.EffectiveInputItemType.Should().Be<LineagePacket<int>>();
        _ = contract.EffectiveOutputItemType.Should().Be<LineagePacket<int>>();
    }

    [Fact]
    public async Task BindAsync_JoinNode_RuntimeContractUsesObjectInputType()
    {
        // Arrange
        const string nodeId = "join";
        var joinNode = new NodeDefinition(
            Id: nodeId,
            Name: nodeId,
            NodeType: typeof(object),
            Kind: NodeKind.Join,
            InputType: typeof(int),
            OutputType: typeof(int));

        var graph = PipelineGraphBuilder.Create()
            .WithNodes([joinNode])
            .WithEdges(ImmutableArray<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .WithItemLevelLineageEnabled(false)
            .Build();

        // Act
        var result = await _binder.BindAsync(graph, new PipelineContext());

        // Assert
        var contractKey = ExecutionAnnotationKeys.RuntimeStreamContractForNode(nodeId);
        var annotations = result.Graph.ExecutionOptions.NodeExecutionAnnotations!;

        _ = annotations[contractKey].Should().BeOfType<RuntimeNodeStreamContract>();
        var contract = (RuntimeNodeStreamContract)annotations[contractKey];
        _ = contract.EffectiveInputItemType.Should().Be<object>();
        _ = contract.EffectiveOutputItemType.Should().Be<int>();
        _ = contract.ItemLevelLineageEnabled.Should().BeFalse();
    }

    private static PipelineGraph CreateGraph(
        bool itemLevelLineageEnabled = false,
        LineageOptions? lineageOptions = null,
        Type? resiliencePolicyType = null,
        Type? deadLetterSinkType = null,
        Type? lineageSinkType = null,
        Type? pipelineLineageSinkType = null)
    {
        return PipelineGraphBuilder.Create()
            .WithNodes(ImmutableArray<NodeDefinition>.Empty)
            .WithEdges(ImmutableArray<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .WithResiliencePolicyType(resiliencePolicyType)
            .WithDeadLetterSinkType(deadLetterSinkType)
            .WithItemLevelLineageEnabled(itemLevelLineageEnabled)
            .WithLineageSinkType(lineageSinkType)
            .WithPipelineLineageSinkType(pipelineLineageSinkType)
            .WithLineageOptions(lineageOptions)
            .Build();
    }

    private sealed class TestPipelineErrorHandler : IResiliencePolicy
    {
        public Task<ResilienceDecision> DecideNodeFailureAsync(
            NodeDefinition nodeDefinition,
            INode node,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(string nodeId, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
        {
            return context.GetRetryDelayStrategy().GetDelayAsync(attemptNumber, cancellationToken);
        }

        public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
        {
            return DefaultResiliencePolicy.Instance.GetCircuitBreaker(context, nodeId);
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