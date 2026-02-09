using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

/// <summary>
///     Tests for context configuration inheritance in composite nodes.
/// </summary>
public class ContextInheritanceTests
{
    [Fact]
    public async Task CompositeNode_WithNoInheritance_ShouldNotInheritParameters()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.Parameters["TestParam"] = "ParentValue";

        // Act
        await runner.RunAsync<NoInheritancePipeline>(context);

        // Assert
        ParameterCheckTransform.FoundParameter.Should().BeFalse();
    }

    [Fact]
    public async Task CompositeNode_WithParameterInheritance_ShouldInheritParameters()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.Parameters["TestParam"] = "ParentValue";

        // Act
        await runner.RunAsync<ParameterInheritancePipeline>(context);

        // Assert
        ParameterCheckTransform.FoundParameter.Should().BeTrue();
        ParameterCheckTransform.ParameterValue.Should().Be("ParentValue");
    }

    [Fact]
    public async Task CompositeNode_WithItemInheritance_ShouldInheritItems()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.Items["TestItem"] = "ItemValue";

        // Act
        await runner.RunAsync<ItemInheritancePipeline>(context);

        // Assert
        ItemCheckTransform.FoundItem.Should().BeTrue();
        ItemCheckTransform.ItemValue.Should().Be("ItemValue");
    }

    [Fact]
    public async Task CompositeNode_WithPropertyInheritance_ShouldInheritProperties()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.Properties["TestProperty"] = "PropertyValue";

        // Act
        await runner.RunAsync<PropertyInheritancePipeline>(context);

        // Assert
        PropertyCheckTransform.FoundProperty.Should().BeTrue();
        PropertyCheckTransform.PropertyValue.Should().Be("PropertyValue");
    }

    [Fact]
    public async Task CompositeNode_WithInheritAll_ShouldInheritEverything()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.Parameters["Param"] = "P1";
        context.Items["Item"] = "I1";
        context.Properties["Property"] = "Prop1";

        // Act
        await runner.RunAsync<InheritAllPipeline>(context);

        // Assert
        AllCheckTransform.HasParameter.Should().BeTrue();
        AllCheckTransform.HasItem.Should().BeTrue();
        AllCheckTransform.HasProperty.Should().BeTrue();
    }

    [Fact]
    public async Task CompositeNode_WithConfigureContextAction_ShouldApplyConfiguration()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.Parameters["OnlyParam"] = "ParamValue";
        context.Items["OnlyItem"] = "ItemValue";

        // Act
        await runner.RunAsync<CustomConfigPipeline>(context);

        // Assert
        CustomCheckTransform.HasParameter.Should().BeTrue();
        CustomCheckTransform.HasItem.Should().BeFalse(); // Only parameters inherited
    }

    [Fact]
    public async Task CompositeNode_SubPipelineModifiesContext_ShouldNotAffectParent()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.Parameters["SharedKey"] = "OriginalValue";

        // Act
        await runner.RunAsync<IsolatedModificationPipeline>(context);

        // Assert
        context.Parameters["SharedKey"].Should().Be("OriginalValue");
        ModificationCheckSink.ValueAfterSubPipeline.Should().Be("OriginalValue");
    }

    // Test helper nodes and pipelines

    private sealed class SimpleIntSource : ISourceNode<int>
    {
        public IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataPipe<int>([1], "SimpleIntSource");
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ParameterCheckTransform : TransformNode<int, int>
    {
        public static bool FoundParameter { get; private set; }
        public static string? ParameterValue { get; private set; }

        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            FoundParameter = context.Parameters.TryGetValue("TestParam", out var value);
            ParameterValue = value?.ToString();
            return Task.FromResult(input);
        }
    }

    private sealed class ItemCheckTransform : TransformNode<int, int>
    {
        public static bool FoundItem { get; private set; }
        public static string? ItemValue { get; private set; }

        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            FoundItem = context.Items.TryGetValue("TestItem", out var value);
            ItemValue = value?.ToString();
            return Task.FromResult(input);
        }
    }

    private sealed class PropertyCheckTransform : TransformNode<int, int>
    {
        public static bool FoundProperty { get; private set; }
        public static string? PropertyValue { get; private set; }

        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            FoundProperty = context.Properties.TryGetValue("TestProperty", out var value);
            PropertyValue = value?.ToString();
            return Task.FromResult(input);
        }
    }

    private sealed class AllCheckTransform : TransformNode<int, int>
    {
        public static bool HasParameter { get; private set; }
        public static bool HasItem { get; private set; }
        public static bool HasProperty { get; private set; }

        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            HasParameter = context.Parameters.ContainsKey("Param");
            HasItem = context.Items.ContainsKey("Item");
            HasProperty = context.Properties.ContainsKey("Property");
            return Task.FromResult(input);
        }
    }

    private sealed class CustomCheckTransform : TransformNode<int, int>
    {
        public static bool HasParameter { get; private set; }
        public static bool HasItem { get; private set; }

        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            HasParameter = context.Parameters.ContainsKey("OnlyParam");
            HasItem = context.Items.ContainsKey("OnlyItem");
            return Task.FromResult(input);
        }
    }

    private sealed class ModifyingTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            // Modify the context within sub-pipeline
            if (context.Parameters.ContainsKey("SharedKey"))
                context.Parameters["SharedKey"] = "ModifiedInSubPipeline";

            return Task.FromResult(input);
        }
    }

    private sealed class ParameterCheckSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<ParameterCheckTransform, int, int>("check");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class ItemCheckSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<ItemCheckTransform, int, int>("check");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class PropertyCheckSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<PropertyCheckTransform, int, int>("check");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class AllCheckSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<AllCheckTransform, int, int>("check");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class CustomCheckSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<CustomCheckTransform, int, int>("check");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class ModifyingSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<ModifyingTransform, int, int>("modify");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class DummySink : ISinkNode<int>
    {
        public async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Consume
            }
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ModificationCheckSink : ISinkNode<int>
    {
        public static string? ValueAfterSubPipeline { get; private set; }

        public async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                ValueAfterSubPipeline = context.Parameters.TryGetValue("SharedKey", out var value)
                    ? value?.ToString()
                    : null;
            }
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class NoInheritancePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SimpleIntSource, int>("source");

            var composite = builder.AddComposite<int, int, ParameterCheckSubPipeline>(
                "composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<DummySink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class ParameterInheritancePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SimpleIntSource, int>("source");

            var composite = builder.AddComposite<int, int, ParameterCheckSubPipeline>(
                "composite",
                new CompositeContextConfiguration
                {
                    InheritParentParameters = true,
                });

            var sink = builder.AddSink<DummySink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class ItemInheritancePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SimpleIntSource, int>("source");

            var composite = builder.AddComposite<int, int, ItemCheckSubPipeline>(
                "composite",
                new CompositeContextConfiguration
                {
                    InheritParentItems = true,
                });

            var sink = builder.AddSink<DummySink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class PropertyInheritancePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SimpleIntSource, int>("source");

            var composite = builder.AddComposite<int, int, PropertyCheckSubPipeline>(
                "composite",
                new CompositeContextConfiguration
                {
                    InheritParentProperties = true,
                });

            var sink = builder.AddSink<DummySink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class InheritAllPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SimpleIntSource, int>("source");

            var composite = builder.AddComposite<int, int, AllCheckSubPipeline>(
                "composite",
                CompositeContextConfiguration.InheritAll);

            var sink = builder.AddSink<DummySink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class CustomConfigPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SimpleIntSource, int>("source");

            var composite = builder.AddComposite<int, int, CustomCheckSubPipeline>(
                config =>
                {
                    config.InheritParentParameters = true;
                    config.InheritParentItems = false;
                },
                "composite");

            var sink = builder.AddSink<DummySink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class IsolatedModificationPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SimpleIntSource, int>("source");

            var composite = builder.AddComposite<int, int, ModifyingSubPipeline>(
                "composite",
                CompositeContextConfiguration.InheritAll);

            var sink = builder.AddSink<ModificationCheckSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }
}
