using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

[Collection("CompositionLineageStateful")]
public sealed class RuntimeLineageCompositeFallbackTests
{
    [Fact]
    public async Task AddComposite_WithoutServiceProvider_InheritAll_WithRuntimeLineageOverride_ShouldSucceed()
    {
        var originalLineage = PipelineBuilder.Lineage;
        PipelineBuilder.Lineage = new LineageService();

        try
        {
            var runner = new PipelineRunnerBuilder()
                .WithLineage(PipelineBuilder.Lineage)
                .Build();

            var context = new PipelineContext();
            context.Properties[PipelineContextKeys.ItemLevelLineageEnabledOverride] = true;

            await runner.RunAsync<ParentWithFallbackCompositeAndInheritAllPipeline>(context);

            GetReceivedItems(context).Should().Equal(2, 4, 6);
        }
        finally
        {
            PipelineBuilder.Lineage = originalLineage;
        }
    }

    [Fact]
    public async Task AddComposite_WithServiceProvider_InheritAll_WithRuntimeLineageOverride_ShouldSucceed()
    {
        var originalLineage = PipelineBuilder.Lineage;
        PipelineBuilder.Lineage = new LineageService();

        try
        {
            var parentRunner = new PipelineRunnerBuilder()
                .WithLineage(PipelineBuilder.Lineage)
                .Build();

            var childRunner = new PipelineRunnerBuilder()
                .WithLineage(PipelineBuilder.Lineage)
                .Build();

            var serviceProvider = new DictionaryServiceProvider()
                .Add(typeof(IPipelineRunner), childRunner)
                .Add(typeof(ChildDoublePipeline), new ChildDoublePipeline());

            var context = new PipelineContext();
            context.Properties[PipelineContextKeys.ItemLevelLineageEnabledOverride] = true;

            await parentRunner.RunAsync(new ParentWithServiceProviderCompositeAndInheritAllPipeline(serviceProvider), context);

            GetReceivedItems(context).Should().Equal(2, 4, 6);
        }
        finally
        {
            PipelineBuilder.Lineage = originalLineage;
        }
    }

    [Fact]
    public async Task AddComposite_WithoutServiceProvider_DefaultContextConfig_WithRuntimeLineageOverride_ShouldNotInheritProperties()
    {
        var originalLineage = PipelineBuilder.Lineage;
        PipelineBuilder.Lineage = new LineageService();

        try
        {
            InspectOverrideTransform.SawRuntimeLineageOverrideProperty = false;

            var runner = new PipelineRunnerBuilder()
                .WithLineage(PipelineBuilder.Lineage)
                .Build();

            var context = new PipelineContext();
            context.Properties[PipelineContextKeys.ItemLevelLineageEnabledOverride] = true;

            await runner.RunAsync<ParentWithFallbackCompositeAndDefaultContextPipeline>(context);

            InspectOverrideTransform.SawRuntimeLineageOverrideProperty.Should().BeFalse();
            GetReceivedItems(context).Should().Equal(2, 4, 6);
        }
        finally
        {
            PipelineBuilder.Lineage = originalLineage;
        }
    }

    private static IReadOnlyList<int> GetReceivedItems(PipelineContext context)
    {
        return context.Items.TryGetValue(CollectingSink.ReceivedItemsKey, out var value) && value is IReadOnlyList<int> items
            ? items
            : throw new InvalidOperationException("Expected collected output items in pipeline context.");
    }

    private sealed class IntSource : ISourceNode<int>
    {
        public IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
            => new InMemoryDataStream<int>([1, 2, 3], nameof(IntSource));

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DoubleTransform : TransformNode<int, int>
    {
        public override Task<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken)
            => Task.FromResult(input * 2);
    }

    private sealed class InspectOverrideTransform : TransformNode<int, int>
    {
        public static bool SawRuntimeLineageOverrideProperty { get; set; }

        public override Task<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            SawRuntimeLineageOverrideProperty = context.Properties.ContainsKey(PipelineContextKeys.ItemLevelLineageEnabledOverride);
            return Task.FromResult(input * 2);
        }
    }

    private sealed class CollectingSink : ISinkNode<int>
    {
        public const string ReceivedItemsKey = "RuntimeLineageCompositeFallbackTests.CollectingSink.ReceivedItems";

        public async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            var receivedItems = new List<int>();

            await foreach (var item in input.WithCancellation(cancellationToken))
                receivedItems.Add(item);

            context.Items[ReceivedItemsKey] = receivedItems;
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ChildDoublePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var input = builder.AddCompositeInput<int>("input");
            var transform = builder.AddTransform<DoubleTransform, int, int>("double");
            var output = builder.AddCompositeOutput<int>("output");

            builder.Connect(input, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class ChildInspectOverridePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var input = builder.AddCompositeInput<int>("input");
            var transform = builder.AddTransform<InspectOverrideTransform, int, int>("inspect");
            var output = builder.AddCompositeOutput<int>("output");

            builder.Connect(input, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class ParentWithFallbackCompositeAndInheritAllPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<IntSource, int>("source");
            var composite = builder.AddComposite<int, int, ChildDoublePipeline>(
                name: "composite",
                contextConfiguration: CompositeContextConfiguration.InheritAll);
            var sink = builder.AddSink<CollectingSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class ParentWithServiceProviderCompositeAndInheritAllPipeline(IServiceProvider serviceProvider)
        : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<IntSource, int>("source");
            var composite = builder.AddComposite<int, int, ChildDoublePipeline>(
                name: "composite",
                contextConfiguration: CompositeContextConfiguration.InheritAll,
                serviceProvider: serviceProvider);
            var sink = builder.AddSink<CollectingSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class ParentWithFallbackCompositeAndDefaultContextPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<IntSource, int>("source");
            var composite = builder.AddComposite<int, int, ChildInspectOverridePipeline>(
                name: "composite",
                contextConfiguration: CompositeContextConfiguration.Default);
            var sink = builder.AddSink<CollectingSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class DictionaryServiceProvider : IServiceProvider
    {
        private readonly Dictionary<Type, object> _services = [];

        public object? GetService(Type serviceType)
            => _services.TryGetValue(serviceType, out var service) ? service : null;

        public DictionaryServiceProvider Add(Type serviceType, object service)
        {
            _services[serviceType] = service;
            return this;
        }
    }
}

[CollectionDefinition("CompositionLineageStateful", DisableParallelization = true)]
public sealed class CompositionLineageStatefulCollectionDefinition;
