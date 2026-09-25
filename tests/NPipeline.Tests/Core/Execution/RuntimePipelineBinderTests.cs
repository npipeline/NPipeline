using System.Collections.Immutable;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Routing;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

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
            true,
            lineageSinkType: typeof(TestLineageSink));

        var lineageFactory = A.Fake<ILineageFactory>();
        var resolvedLineageSink = A.Fake<ILineageSink>();
        var decoratedLineageSink = A.Fake<ILineageSink>();

        _ = A.CallTo(() => lineageFactory.CreateLineageSink(typeof(TestLineageSink)))
            .Returns(resolvedLineageSink);

        // No collector registered, so the decorated sink is used directly.
        _ = A.CallTo(() => lineageFactory.ResolveLineageCollector()).Returns(null);

        var context = new PipelineContext(new PipelineContextConfiguration(
            LineageFactory: lineageFactory));

        context.Properties[PipelineContextKeys.LineageSinkDecorator] =
            (Func<ILineageSink?, ILineageSink?>)(_ => decoratedLineageSink);

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.LineageSink.Should().BeSameAs(decoratedLineageSink);
        _ = result.LineageCollector.Should().BeNull();

        _ = A.CallTo(() => lineageFactory.CreateLineageSink(typeof(TestLineageSink)))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task BindAsync_ItemLevelLineageEnabledWithCollector_TeesRecordsIntoCollector()
    {
        // Arrange
        var graph = CreateGraph(
            true,
            lineageSinkType: typeof(TestLineageSink));

        var lineageFactory = A.Fake<ILineageFactory>();
        var resolvedLineageSink = A.Fake<ILineageSink>();
        var collector = A.Fake<ILineageCollector>();

        _ = A.CallTo(() => lineageFactory.CreateLineageSink(typeof(TestLineageSink)))
            .Returns(resolvedLineageSink);

        _ = A.CallTo(() => lineageFactory.ResolveLineageCollector()).Returns(collector);

        var context = new PipelineContext(new PipelineContextConfiguration(
            LineageFactory: lineageFactory));

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.LineageCollector.Should().BeSameAs(collector);

        var tee = result.LineageSink.Should().BeOfType<CollectorTeeingLineageSink>().Subject;
        _ = tee.Inner.Should().BeSameAs(resolvedLineageSink);

        // The tee forwards to both the collector and the configured sink.
        var record = new LineageRecord(Guid.NewGuid(), "node", Guid.NewGuid(), LineageOutcomeReason.Emitted, false, ["node"]);
        await tee.RecordAsync(record, CancellationToken.None);

        A.CallTo(() => collector.Record(A<LineageRecord>._)).MustHaveHappenedOnceExactly();
        _ = A.CallTo(() => resolvedLineageSink.RecordAsync(record, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task BindAsync_ItemLevelLineageDisabled_DoesNotResolveCollector()
    {
        // Arrange
        var graph = CreateGraph(false);

        var lineageFactory = A.Fake<ILineageFactory>();
        var collector = A.Fake<ILineageCollector>();
        _ = A.CallTo(() => lineageFactory.ResolveLineageCollector()).Returns(collector);

        var context = new PipelineContext(new PipelineContextConfiguration(
            LineageFactory: lineageFactory));

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.LineageCollector.Should().BeNull();
        _ = result.LineageSink.Should().BeNull();
        A.CallTo(() => lineageFactory.ResolveLineageCollector()).MustNotHaveHappened();
    }

    [Fact]
    public async Task BindAsync_ItemLevelLineageDisabled_DoesNotResolveLineageSink()
    {
        // Arrange
        var graph = CreateGraph(
            false,
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
        var graph = CreateGraph(true);

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
            true,
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
            nodeId,
            nodeId,
            typeof(object),
            NodeKind.Route,
            typeof(int),
            typeof(int));

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
        _ = normalized.Rules[0].Predicate(new LineagePacket<int>(2, Guid.NewGuid(), ImmutableArray<string>.Empty)).Should().BeTrue();
        _ = normalized.Rules[0].Predicate(new LineagePacket<int>(3, Guid.NewGuid(), ImmutableArray<string>.Empty)).Should().BeFalse();

        _ = annotations[contractKey].Should().BeOfType<RuntimeNodeStreamContract>();
        var contract = (RuntimeNodeStreamContract)annotations[contractKey];
        _ = contract.ItemLevelLineageEnabled.Should().BeTrue();
        _ = contract.EffectiveInputItemType.Should().Be<LineagePacket<int>>();
        _ = contract.EffectiveOutputItemType.Should().Be<LineagePacket<int>>();
    }

    [Fact]
    public async Task BindAsync_RouteWithoutOutputType_ThrowsActionableError()
    {
        const string nodeId = "route-without-output";

        var routeNode = new NodeDefinition(
            nodeId,
            nodeId,
            typeof(object),
            NodeKind.Route,
            typeof(int));

        var graph = PipelineGraphBuilder.Create()
            .WithNodes([routeNode])
            .WithEdges(ImmutableArray<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .WithNodeExecutionAnnotations(new Dictionary<string, object>
            {
                [ExecutionAnnotationKeys.RouteOptionsForNode(nodeId)] = new RouteOptions<int>(),
            })
            .Build();

        Func<Task> act = () => _binder.BindAsync(graph, new PipelineContext());

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        _ = thrown.Which.Message.Should().Contain($"[{ErrorCodes.RouteNodeMissingOutputType}]");
        _ = thrown.Which.Message.Should().Contain(nodeId);
        _ = thrown.Which.Message.Should().Contain("PipelineBuilder.AddRoute<T>()");
    }

    [Fact]
    public async Task BindAsync_JoinNode_RuntimeContractUsesObjectInputType()
    {
        // Arrange
        const string nodeId = "join";

        var joinNode = new NodeDefinition(
            nodeId,
            nodeId,
            typeof(object),
            NodeKind.Join,
            typeof(int),
            typeof(int));

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

    [Fact]
    public async Task BindAsync_DeadLetterSinkCreatedFromType_IsRegisteredForDisposal()
    {
        // Arrange
        var graph = CreateGraph(deadLetterSinkType: typeof(DisposableDeadLetterSink));
        var context = new PipelineContext();

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert - created by the default factory, which hands ownership to the caller
        _ = result.DeadLetterSink.Should().BeOfType<DisposableDeadLetterSink>();

        // The run's context disposes it; the pipeline must register it for disposal.
        await context.DisposeAsync();
        DisposableDeadLetterSink.Disposed.Should().Be(1);
    }

    [Fact]
    public async Task BindAsync_DeadLetterSinkInstancePassedDirectly_IsNotRegisteredForDisposal()
    {
        // Arrange
        DisposableDeadLetterSink.Disposed = 0;
        var instance = new DisposableDeadLetterSink();
        var graph = CreateGraph(deadLetterSink: instance);
        var context = new PipelineContext();

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.DeadLetterSink.Should().BeSameAs(instance);
        await context.DisposeAsync();
        DisposableDeadLetterSink.Disposed.Should().Be(0, "the user owns an instance passed directly");
    }

    [Fact]
    public async Task BindAsync_DeadLetterSinkCreatedByAFactoryThatOwnsInstances_IsNotRegisteredForDisposal()
    {
        // Arrange
        DisposableDeadLetterSink.Disposed = 0;
        var graph = CreateGraph(deadLetterSinkType: typeof(DisposableDeadLetterSink));
        var factory = new ContainerOwnedErrorHandlerFactory();
        var context = new PipelineContext(new PipelineContextConfiguration(ErrorHandlerFactory: factory));

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.DeadLetterSink.Should().BeSameAs(factory.Sink);
        await context.DisposeAsync();
        DisposableDeadLetterSink.Disposed.Should().Be(0, "the container owns created instances when the factory says so");
    }

    [Fact]
    public async Task BindAsync_FactoryReportingOwnershipPerInstance_DisposesOnlyTheCallerOwnedSink()
    {
        // Arrange - a container-backed factory mixes resolved and constructed instances; only the resolved ones
        // belong to the container.
        DisposableDeadLetterSink.Disposed = 0;
        var graph = CreateGraph(deadLetterSinkType: typeof(DisposableDeadLetterSink));
        var containerSink = new DisposableDeadLetterSink();
        var factory = new MixedOwnershipErrorHandlerFactory(containerSink);
        var context = new PipelineContext(new PipelineContextConfiguration(ErrorHandlerFactory: factory));

        // Act
        var containerResult = await _binder.BindAsync(graph, context);
        await context.DisposeAsync();
        DisposableDeadLetterSink.Disposed.Should().Be(0, "the container tracks the instance the factory resolved");

        // A second run through the same factory constructs a fresh instance, which the caller owns.
        DisposableDeadLetterSink.Disposed = 0;
        factory.ResolveFromContainer = false;
        var secondContext = new PipelineContext(new PipelineContextConfiguration(ErrorHandlerFactory: factory));
        var constructedResult = await _binder.BindAsync(graph, secondContext);

        // Assert
        _ = containerResult.DeadLetterSink.Should().BeSameAs(containerSink);
        _ = constructedResult.DeadLetterSink.Should().NotBeSameAs(containerSink);
        await secondContext.DisposeAsync();
        DisposableDeadLetterSink.Disposed.Should().Be(1, "an instance the factory constructed is the caller's to release");
    }

    [Fact]
    public async Task BindAsync_LineageSinkCreatedByAFactoryThatOwnsInstances_IsNotRegisteredForDisposal()
    {
        // Arrange
        DisposableLineageSink.Disposed = 0;
        var graph = CreateGraph(itemLevelLineageEnabled: true, lineageSinkType: typeof(DisposableLineageSink));
        var factory = new ContainerOwnedLineageFactory();
        var context = new PipelineContext(new PipelineContextConfiguration(LineageFactory: factory));

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.LineageSink.Should().BeSameAs(factory.Sink);
        await context.DisposeAsync();
        DisposableLineageSink.Disposed.Should().Be(0, "the container owns created instances when the factory says so");
    }

    [Fact]
    public async Task BindAsync_LineageSinkCreatedFromType_IsRegisteredForDisposal()
    {
        // Arrange - the default lineage factory hands the instance to the caller.
        DisposableLineageSink.Disposed = 0;
        var graph = CreateGraph(itemLevelLineageEnabled: true, lineageSinkType: typeof(DisposableLineageSink));
        var context = new PipelineContext();

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.LineageSink.Should().BeOfType<DisposableLineageSink>();
        await context.DisposeAsync();
        DisposableLineageSink.Disposed.Should().Be(1);
    }

    [Fact]
    public async Task BindAsync_ResiliencePolicyCreatedFromType_IsRegisteredForDisposal()
    {
        // Arrange
        DisposableResiliencePolicy.Disposed = 0;
        var graph = CreateGraph(resiliencePolicyType: typeof(DisposableResiliencePolicy));
        var context = new PipelineContext();

        // Act
        var result = await _binder.BindAsync(graph, context);

        // Assert
        _ = result.ResiliencePolicy.Should().BeOfType<DisposableResiliencePolicy>();
        await context.DisposeAsync();
        DisposableResiliencePolicy.Disposed.Should().Be(1);
    }

    [Fact]
    public async Task RunAsync_DeadLetterSinkCreatedFromType_IsDisposedWhenTheRunOwnsTheContext()
    {
        // Arrange
        DisposableDeadLetterSink.Disposed = 0;

        // Act - the parameterless RunAsync creates and disposes its own context
        await PipelineRunner.Create().RunAsync<DeadLetterSinkPipelineDefinition>();

        // Assert
        DisposableDeadLetterSink.Disposed.Should().Be(1);
    }

    private sealed class DeadLetterSinkPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<EmptySource, int>("source");
            var sink = builder.AddSink<EmptySink, int>("sink");
            _ = builder.Connect(source, sink);
            _ = builder.AddDeadLetterSink<DisposableDeadLetterSink>();
        }
    }

    private sealed class EmptySource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new NPipeline.DataFlow.DataStreams.DataStream<int>(Empty(), "empty");

        private static async IAsyncEnumerable<int> Empty()
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private sealed class EmptySink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }

    private static PipelineGraph CreateGraph(
        bool itemLevelLineageEnabled = false,
        LineageOptions? lineageOptions = null,
        Type? resiliencePolicyType = null,
        Type? deadLetterSinkType = null,
        Type? lineageSinkType = null,
        Type? pipelineLineageSinkType = null,
        IDeadLetterSink? deadLetterSink = null) =>
        PipelineGraphBuilder.Create()
            .WithNodes(ImmutableArray<NodeDefinition>.Empty)
            .WithEdges(ImmutableArray<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .WithResiliencePolicyType(resiliencePolicyType)
            .WithDeadLetterSinkType(deadLetterSinkType)
            .WithDeadLetterSink(deadLetterSink)
            .WithItemLevelLineageEnabled(itemLevelLineageEnabled)
            .WithLineageSinkType(lineageSinkType)
            .WithPipelineLineageSinkType(pipelineLineageSinkType)
            .WithLineageOptions(lineageOptions)
            .Build();

    private sealed class DisposableDeadLetterSink : IDeadLetterSink, IAsyncDisposable
    {
        public static int Disposed;

        public ValueTask DisposeAsync()
        {
            Disposed++;
            return ValueTask.CompletedTask;
        }

        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class ContainerOwnedErrorHandlerFactory : IErrorHandlerFactory
    {
        public DisposableDeadLetterSink Sink { get; } = new();

        public bool CallerOwnsCreatedInstance(object instance) => !ReferenceEquals(instance, Sink);

        public IDeadLetterSink? CreateDeadLetterSink(Type sinkType) => Sink;
    }

    /// <summary>
    ///     Stands for a container-backed factory that mixes resolved and constructed instances: an instance it
    ///     resolved is the container's, an instance it constructed is the caller's.
    /// </summary>
    private sealed class MixedOwnershipErrorHandlerFactory(DisposableDeadLetterSink containerSink) : IErrorHandlerFactory
    {
        public bool ResolveFromContainer { get; set; } = true;

        public DisposableDeadLetterSink ConstructedSink { get; } = new();

        public bool CallerOwnsCreatedInstance(object instance) =>
            !ReferenceEquals(instance, containerSink);

        public IDeadLetterSink? CreateDeadLetterSink(Type sinkType) => ResolveFromContainer ? containerSink : ConstructedSink;
    }

    private sealed class ContainerOwnedLineageFactory : ILineageFactory
    {
        public DisposableLineageSink Sink { get; } = new();

        public bool CallerOwnsCreatedInstance(object instance) => !ReferenceEquals(instance, Sink);

        public ILineageSink? CreateLineageSink(Type sinkType) => Sink;

        public IPipelineLineageSink? CreatePipelineLineageSink(Type sinkType) => null;

        public IPipelineLineageSinkProvider? ResolvePipelineLineageSinkProvider() => null;

        public ILineageCollector? ResolveLineageCollector() => null;

        public PipelineLineageReport? CreateLineageReport(string pipelineName, Guid pipelineId, PipelineGraph graph, Guid runId) => null;
    }

    private sealed class DisposableLineageSink : ILineageSink, IAsyncDisposable
    {
        public static int Disposed;

        public ValueTask DisposeAsync()
        {
            Disposed++;
            return ValueTask.CompletedTask;
        }

        public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class DisposableResiliencePolicy : IResiliencePolicy, IAsyncDisposable
    {
        public static int Disposed;

        public ValueTask DisposeAsync()
        {
            Disposed++;
            return ValueTask.CompletedTask;
        }

        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);
    }

    private sealed class TestPipelineErrorHandler : IResiliencePolicy
    {
        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);
    }

    private sealed class TestDeadLetterSink : IDeadLetterSink
    {
        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class TestLineageSink : ILineageSink
    {
        public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class TestPipelineLineageSink : IPipelineLineageSink
    {
        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
