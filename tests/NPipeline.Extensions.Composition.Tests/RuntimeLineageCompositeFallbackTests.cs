using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

public sealed class RuntimeLineageCompositeFallbackTests
{
    [Fact]
    public async Task AddComposite_WithoutServiceProvider_InheritAll_WithRuntimeLineageOverride_ShouldSucceed()
    {
        var runner = new PipelineRunnerBuilder()
            .WithLineage(new LineageService())
            .Build();

        var context = new PipelineContext();
        context.Properties[PipelineContextKeys.ItemLevelLineageEnabledOverride] = true;

        await runner.RunAsync<ParentWithFallbackCompositeAndInheritAllPipeline>(context);

        GetReceivedItems(context).Should().Equal(2, 4, 6);
    }

    [Fact]
    public async Task AddComposite_WithServiceProvider_InheritAll_WithRuntimeLineageOverride_ShouldSucceed()
    {
        var lineage = new LineageService();

        var parentRunner = new PipelineRunnerBuilder()
            .WithLineage(lineage)
            .Build();

        var childRunner = new PipelineRunnerBuilder()
            .WithLineage(lineage)
            .Build();

        var serviceProvider = new DictionaryServiceProvider()
            .Add(typeof(IPipelineRunner), childRunner)
            .Add(typeof(ChildDoublePipeline), new ChildDoublePipeline());

        var context = new PipelineContext();
        context.Properties[PipelineContextKeys.ItemLevelLineageEnabledOverride] = true;

        await parentRunner.RunAsync(new ParentWithServiceProviderCompositeAndInheritAllPipeline(serviceProvider), context);

        GetReceivedItems(context).Should().Equal(2, 4, 6);
    }

    [Fact]
    public async Task AddComposite_WithoutServiceProvider_DefaultContextConfig_WithRuntimeLineageOverride_ShouldNotInheritProperties()
    {
        InspectOverrideTransform.SawRuntimeLineageOverrideProperty = false;

        var runner = new PipelineRunnerBuilder()
            .WithLineage(new LineageService())
            .Build();

        var context = new PipelineContext();
        context.Properties[PipelineContextKeys.ItemLevelLineageEnabledOverride] = true;

        await runner.RunAsync<ParentWithFallbackCompositeAndDefaultContextPipeline>(context);

        InspectOverrideTransform.SawRuntimeLineageOverrideProperty.Should().BeFalse();
        GetReceivedItems(context).Should().Equal(2, 4, 6);
    }

    private static IReadOnlyList<int> GetReceivedItems(PipelineContext context) =>
        context.Items.TryGetValue(CollectingSink.ReceivedItemsKey, out var value) && value is IReadOnlyList<int> items
            ? items
            : throw new InvalidOperationException("Expected collected output items in pipeline context.");

    private sealed class IntSource : ISourceNode<int>, IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }

        public IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
            => new InMemoryDataStream<int>([1, 2, 3], nameof(IntSource));
    }

    private sealed class DoubleTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken)
            => ValueTask.FromResult(input * 2);
    }

    private sealed class InspectOverrideTransform : TransformNode<int, int>
    {
        public static bool SawRuntimeLineageOverrideProperty { get; set; }

        public override ValueTask<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            SawRuntimeLineageOverrideProperty = context.Properties.ContainsKey(PipelineContextKeys.ItemLevelLineageEnabledOverride);
            return ValueTask.FromResult(input * 2);
        }
    }

    private sealed class CollectingSink : ISinkNode<int>, IAsyncDisposable
    {
        public const string ReceivedItemsKey = "RuntimeLineageCompositeFallbackTests.CollectingSink.ReceivedItems";

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }

        public async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            var receivedItems = new List<int>();

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                receivedItems.Add(item);
            }

            context.Items[ReceivedItemsKey] = receivedItems;
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
                "composite",
                CompositeContextConfiguration.InheritAll);

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
                "composite",
                CompositeContextConfiguration.InheritAll,
                serviceProvider);

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
                "composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<CollectingSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class DictionaryServiceProvider : IServiceProvider
    {
        private readonly Dictionary<Type, object> _services = [];

        public object? GetService(Type serviceType)
            => _services.TryGetValue(serviceType, out var service)
                ? service
                : null;

        public DictionaryServiceProvider Add(Type serviceType, object service)
        {
            _services[serviceType] = service;
            return this;
        }
    }
}
