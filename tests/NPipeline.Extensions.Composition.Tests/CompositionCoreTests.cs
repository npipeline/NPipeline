using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

#pragma warning disable IDE0011 // Add braces
#pragma warning disable IDE0022 // Use block body for methods
#pragma warning disable IDE0058 // Expression value is never used

namespace NPipeline.Extensions.Composition.Tests
{

    /// <summary>
    ///     Tests for the composition core recommendations:
    ///     - Composite NodeKind
    ///     - ChildDefinitionType on NodeDefinition
    ///     - Metadata dictionary on NodeDefinition
    ///     - Child graphs on PipelineGraph
    ///     - CompositeInput/CompositeOutput NodeKinds
    ///     - CompositeNaming helpers
    ///     - DI-resolved child definitions (RunAsync with pre-instantiated definition)
    ///     - AddCompositeInput/AddCompositeOutput builder extensions
    /// </summary>
    public class CompositionCoreTests
    {
        #region Rec 1 + 7: Composite, CompositeInput, CompositeOutput NodeKinds

        [Fact]
        public void AddComposite_ShouldRegisterNodeWithCompositeKind()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var composite = builder.AddComposite<int, int, SimpleSubPipeline>("composite");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);

            // Act
            var pipeline = builder.Build();

            // Assert
            var compositeDef = pipeline.Graph.Nodes.First(n => n.Name == "composite");
            compositeDef.Kind.Should().Be(NodeKind.Composite);
        }

        [Fact]
        public void AddCompositeInput_ShouldRegisterNodeWithCompositeInputKind()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var input = builder.AddCompositeInput<int>("input");
            var transform = builder.AddTransform<DoubleTransform, int, int>("double");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(input, transform);
            builder.Connect(transform, output);

            // Act
            var pipeline = builder.Build();

            // Assert
            var inputDef = pipeline.Graph.Nodes.First(n => n.Name == "input");
            inputDef.Kind.Should().Be(NodeKind.CompositeInput);
        }

        [Fact]
        public void AddCompositeOutput_ShouldRegisterNodeWithCompositeOutputKind()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var input = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<DoubleTransform, int, int>("double");
            var output = builder.AddCompositeOutput<int>("output");

            builder.Connect(input, transform);
            builder.Connect(transform, output);

            // Act
            var pipeline = builder.Build();

            // Assert
            var outputDef = pipeline.Graph.Nodes.First(n => n.Name == "output");
            outputDef.Kind.Should().Be(NodeKind.CompositeOutput);
        }

        [Fact]
        public async Task CompositeInput_CompositeOutput_ShouldWorkInSubPipeline()
        {
            // Arrange — sub-pipeline using AddCompositeInput/Output
            var runner = PipelineRunner.Create();
            var context = new PipelineContext();

            // Act
            await runner.RunAsync<ParentWithCompositeInputOutputPipeline>(context);

            // Assert
            var receivedItems = GetReceivedItems(context, CompositeInputOutputSink.ContextItemsKey);
            receivedItems.Should().Equal(2, 4, 6);
        }

        #endregion

        #region Rec 2: ChildDefinitionType on NodeDefinition

        [Fact]
        public void AddComposite_ShouldSetChildDefinitionType()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var composite = builder.AddComposite<int, int, SimpleSubPipeline>("composite");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);

            // Act
            var pipeline = builder.Build();

            // Assert
            var compositeDef = pipeline.Graph.Nodes.First(n => n.Name == "composite");
            compositeDef.ChildDefinitionType.Should().Be<SimpleSubPipeline>();
        }

        [Fact]
        public void SetNodeChildDefinitionType_ShouldUpdateNodeDefinition()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var transform = builder.AddTransform<DoubleTransform, int, int>("transform");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);

            // Act
            builder.SetNodeChildDefinitionType(transform.Id, typeof(SimpleSubPipeline));
            var pipeline = builder.Build();

            // Assert
            var transformDef = pipeline.Graph.Nodes.First(n => n.Name == "transform");
            transformDef.ChildDefinitionType.Should().Be<SimpleSubPipeline>();
        }

        [Fact]
        public void NonCompositeNodes_ShouldHaveNullChildDefinitionType()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, sink);

            // Act
            var pipeline = builder.Build();

            // Assert
            foreach (var node in pipeline.Graph.Nodes)
            {
                node.ChildDefinitionType.Should().BeNull();
            }
        }

        #endregion

        #region Rec 3: Metadata dictionary on NodeDefinition

        [Fact]
        public void SetNodeMetadata_ShouldStoreMetadataOnNode()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, sink);

            // Act
            builder.SetNodeMetadata(source.Id, "CustomKey", "CustomValue");
            builder.SetNodeMetadata(source.Id, "AnotherKey", 42);
            var pipeline = builder.Build();

            // Assert
            var sourceDef = pipeline.Graph.Nodes.First(n => n.Name == "source");
            sourceDef.Metadata.Should().NotBeNull();
            sourceDef.Metadata["CustomKey"].Should().Be("CustomValue");
            sourceDef.Metadata["AnotherKey"].Should().Be(42);
        }

        [Fact]
        public void SetNodeMetadata_ShouldOverwriteExistingKey()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, sink);

            // Act
            builder.SetNodeMetadata(source.Id, "Key", "First");
            builder.SetNodeMetadata(source.Id, "Key", "Second");
            var pipeline = builder.Build();

            // Assert
            var sourceDef = pipeline.Graph.Nodes.First(n => n.Name == "source");
            sourceDef.Metadata.Should().NotBeNull();
            sourceDef.Metadata["Key"].Should().Be("Second");
        }

        [Fact]
        public void NodesWithoutMetadata_ShouldHaveNullMetadata()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, sink);

            // Act
            var pipeline = builder.Build();

            // Assert
            foreach (var node in pipeline.Graph.Nodes)
            {
                node.Metadata.Should().BeNull();
            }
        }

        [Fact]
        public void SetNodeMetadata_WithNonExistentNode_ShouldThrow()
        {
            // Arrange
            var builder = new PipelineBuilder();

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() =>
                builder.SetNodeMetadata("nonexistent", "Key", "Value"));
        }

        #endregion

        #region Rec 4: CompositeNaming helper

        [Fact]
        public void PrefixNodeId_ShouldCreateNamespacedId()
        {
            var result = CompositeNaming.PrefixNodeId("parent", "child");
            result.Should().Be("parent::child");
        }

        [Fact]
        public void IsNamespaced_ShouldReturnTrueForNamespacedId()
        {
            CompositeNaming.IsNamespaced("parent::child").Should().BeTrue();
        }

        [Fact]
        public void IsNamespaced_ShouldReturnFalseForSimpleId()
        {
            CompositeNaming.IsNamespaced("simple-id").Should().BeFalse();
        }

        [Fact]
        public void GetParentNodeId_ShouldExtractParent()
        {
            CompositeNaming.GetParentNodeId("parent::child").Should().Be("parent");
        }

        [Fact]
        public void GetParentNodeId_ShouldExtractImmediateParentForNestedId()
        {
            CompositeNaming.GetParentNodeId("grandparent::parent::child").Should().Be("grandparent::parent");
        }

        [Fact]
        public void GetParentNodeId_ShouldReturnNullForSimpleId()
        {
            CompositeNaming.GetParentNodeId("simple-id").Should().BeNull();
        }

        [Fact]
        public void GetChildNodeId_ShouldExtractChild()
        {
            CompositeNaming.GetChildNodeId("parent::child").Should().Be("child");
        }

        [Fact]
        public void GetChildNodeId_ShouldExtractLeafChildForNestedId()
        {
            CompositeNaming.GetChildNodeId("grandparent::parent::child").Should().Be("child");
        }

        [Fact]
        public void GetChildNodeId_ShouldReturnOriginalForSimpleId()
        {
            CompositeNaming.GetChildNodeId("simple-id").Should().Be("simple-id");
        }

        #endregion

        #region Rec 5: DI-resolved child definitions / RunAsync with pre-instantiated definition

        [Fact]
        public async Task RunAsync_WithPreInstantiatedDefinition_ShouldExecuteSuccessfully()
        {
            // Arrange
            var runner = PipelineRunner.Create();
            var context = new PipelineContext();
            var definition = new TrackedPipeline();

            // Act
            await runner.RunAsync(definition, context);

            // Assert
            var receivedItems = GetReceivedItems(context, TrackedSink.ContextItemsKey);
            receivedItems.Should().HaveCount(3);
            receivedItems.Should().Equal(1, 2, 3);
        }

        [Fact]
        public async Task AddComposite_WithServiceProvider_ShouldResolveChildDefinition()
        {
            // Arrange
            var runner = PipelineRunner.Create();
            var context = new PipelineContext();
            var serviceProvider = new DictionaryServiceProvider()
                .Add(typeof(DiOnlySubPipeline), new DiOnlySubPipeline(3));
            var definition = new ParentWithDiCompositePipeline(serviceProvider, fallbackToParameterlessWhenServiceMissing: false);

            // Act
            await runner.RunAsync(definition, context);

            // Assert
            var receivedItems = GetReceivedItems(context, ParentSink.ContextItemsKey);
            receivedItems.Should().Equal(3, 6, 9);
        }

        [Fact]
        public async Task AddComposite_WithServiceProviderMissingRegistration_ShouldThrowByDefault()
        {
            // Arrange
            var runner = PipelineRunner.Create();
            var context = new PipelineContext();
            var definition = new ParentWithDiCompositePipeline(new DictionaryServiceProvider(), fallbackToParameterlessWhenServiceMissing: false);

            // Act
            Func<Task> act = () => runner.RunAsync(definition, context);

            // Assert
            await act.Should().ThrowAsync<NodeExecutionException>()
                .WithMessage("*Unable to resolve child pipeline definition*");
        }

        [Fact]
        public async Task AddComposite_WithMissingServiceAndFallbackEnabled_ShouldUseParameterlessDefinition()
        {
            // Arrange
            var runner = PipelineRunner.Create();
            var context = new PipelineContext();
            var definition = new ParentWithFallbackCompositePipeline(new DictionaryServiceProvider());

            // Act
            await runner.RunAsync(definition, context);

            // Assert
            var receivedItems = GetReceivedItems(context, ParentSink.ContextItemsKey);
            receivedItems.Should().Equal(2, 4, 6);
        }

        #endregion

        #region Rec 6: Child graphs on PipelineGraph

        [Fact]
        public void Build_WithCompositeNodes_ShouldAttachChildGraphs()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var composite = builder.AddComposite<int, int, SimpleSubPipeline>("composite");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);

            // Act
            var pipeline = builder.Build();

            // Assert
            pipeline.Graph.ChildGraphs.Should().NotBeNull();
            pipeline.Graph.ChildGraphs.Should().ContainKey(composite.Id);

            var childGraph = pipeline.Graph.ChildGraphs[composite.Id];
            childGraph.Nodes.Should().HaveCount(3); // input, double, output
        }

        [Fact]
        public void Build_WithoutCompositeNodes_ShouldHaveNullChildGraphs()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, sink);

            // Act
            var pipeline = builder.Build();

            // Assert
            pipeline.Graph.ChildGraphs.Should().BeNull();
        }

        [Fact]
        public void Build_ChildGraph_ShouldContainCorrectNodes()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var composite = builder.AddComposite<int, int, SimpleSubPipeline>("composite");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);

            // Act
            var pipeline = builder.Build();
            var childGraphs = pipeline.Graph.ChildGraphs ?? throw new InvalidOperationException("Expected child graphs to be available.");
            var childGraph = childGraphs[composite.Id];

            // Assert
            childGraph.Nodes.Should().Contain(n => n.Name == "input");
            childGraph.Nodes.Should().Contain(n => n.Name == "double");
            childGraph.Nodes.Should().Contain(n => n.Name == "output");
            childGraph.Edges.Should().HaveCount(2);
        }

        [Fact]
        public void Build_WithMultipleComposites_ShouldAttachAllChildGraphs()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var composite1 = builder.AddComposite<int, int, SimpleSubPipeline>("composite1");
            var composite2 = builder.AddComposite<int, int, SimpleSubPipeline>("composite2");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite1);
            builder.Connect(composite1, composite2);
            builder.Connect(composite2, sink);

            // Act
            var pipeline = builder.Build();

            // Assert
            pipeline.Graph.ChildGraphs.Should().HaveCount(2);
            pipeline.Graph.ChildGraphs.Should().ContainKey(composite1.Id);
            pipeline.Graph.ChildGraphs.Should().ContainKey(composite2.Id);
        }

        [Fact]
        public void TryBuild_WithCompositeNodes_ShouldAttachChildGraphs()
        {
            // Arrange
            var builder = new PipelineBuilder();
            var source = builder.AddSource<TestSource, int>("source");
            var composite = builder.AddComposite<int, int, SimpleSubPipeline>("composite");
            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);

            // Act
            var success = builder.TryBuild(out var pipeline, out _);

            // Assert
            success.Should().BeTrue();
            pipeline!.Graph.ChildGraphs.Should().NotBeNull();
            pipeline.Graph.ChildGraphs.Should().ContainKey(composite.Id);
        }

        #endregion

        #region Integration: Full execution with new NodeKinds

        [Fact]
        public async Task FullPipeline_WithCompositeNode_ShouldExecuteCorrectly()
        {
            // Arrange
            var runner = PipelineRunner.Create();
            var context = new PipelineContext();

            // Act
            await runner.RunAsync<ParentPipeline>(context);

            // Assert
            var receivedItems = GetReceivedItems(context, ParentSink.ContextItemsKey);
            receivedItems.Should().Equal(2, 4, 6);
        }

        #endregion

        #region Test Helpers

        private static IReadOnlyList<int> GetReceivedItems(PipelineContext context, string key)
        {
            return context.Items.TryGetValue(key, out var value) && value is IReadOnlyList<int> items
                ? items
                : throw new InvalidOperationException($"Expected sink output under context key '{key}'.");
        }

        private sealed class TestSource : ISourceNode<int>
        {
            public IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
                => new InMemoryDataStream<int>([1, 2, 3], "TestSource");

            public ValueTask DisposeAsync() { GC.SuppressFinalize(this); return ValueTask.CompletedTask; }
        }

        private sealed class TestSink : ISinkNode<int>
        {
            public async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
            {
                await foreach (var _ in input.WithCancellation(cancellationToken)) { }
            }

            public ValueTask DisposeAsync() { GC.SuppressFinalize(this); return ValueTask.CompletedTask; }
        }

        private sealed class DoubleTransform : TransformNode<int, int>
        {
            public override Task<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken)
                => Task.FromResult(input * 2);
        }

        private sealed class SimpleSubPipeline : IPipelineDefinition
        {
            public void Define(PipelineBuilder builder, PipelineContext context)
            {
                var input = builder.AddSource<PipelineInputSource<int>, int>("input");
                var transform = builder.AddTransform<DoubleTransform, int, int>("double");
                var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

                builder.Connect(input, transform);
                builder.Connect(transform, output);
            }
        }

        // Sub-pipeline using the new AddCompositeInput/AddCompositeOutput
        private sealed class SubPipelineWithCompositeInputOutput : IPipelineDefinition
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

        private sealed class ParentWithCompositeInputOutputPipeline : IPipelineDefinition
        {
            public void Define(PipelineBuilder builder, PipelineContext context)
            {
                var source = builder.AddSource<TestSource, int>("source");
                var composite = builder.AddComposite<int, int, SubPipelineWithCompositeInputOutput>("composite");
                var sink = builder.AddSink<CompositeInputOutputSink, int>("sink");

                builder.Connect(source, composite);
                builder.Connect(composite, sink);
            }
        }

        private sealed class CompositeInputOutputSink : ISinkNode<int>
        {
            public const string ContextItemsKey = "CompositionCoreTests.CompositeInputOutputSink.ReceivedItems";

            public async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
            {
                var receivedItems = new List<int>();
                await foreach (var item in input.WithCancellation(cancellationToken))
                    receivedItems.Add(item);

                context.Items[ContextItemsKey] = receivedItems;
            }

            public ValueTask DisposeAsync() { GC.SuppressFinalize(this); return ValueTask.CompletedTask; }
        }

        private sealed class ParentSink : ISinkNode<int>
        {
            public const string ContextItemsKey = "CompositionCoreTests.ParentSink.ReceivedItems";

            public async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
            {
                var receivedItems = new List<int>();
                await foreach (var item in input.WithCancellation(cancellationToken))
                    receivedItems.Add(item);

                context.Items[ContextItemsKey] = receivedItems;
            }

            public ValueTask DisposeAsync() { GC.SuppressFinalize(this); return ValueTask.CompletedTask; }
        }

        private sealed class ParentPipeline : IPipelineDefinition
        {
            public void Define(PipelineBuilder builder, PipelineContext context)
            {
                var source = builder.AddSource<TestSource, int>("source");
                var composite = builder.AddComposite<int, int, SimpleSubPipeline>("composite");
                var sink = builder.AddSink<ParentSink, int>("sink");

                builder.Connect(source, composite);
                builder.Connect(composite, sink);
            }
        }

        private sealed class TrackedSink : ISinkNode<int>
        {
            public const string ContextItemsKey = "CompositionCoreTests.TrackedSink.ReceivedItems";

            public async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
            {
                var receivedItems = new List<int>();
                await foreach (var item in input.WithCancellation(cancellationToken))
                    receivedItems.Add(item);

                context.Items[ContextItemsKey] = receivedItems;
            }

            public ValueTask DisposeAsync() { GC.SuppressFinalize(this); return ValueTask.CompletedTask; }
        }

        private sealed class TrackedPipeline : IPipelineDefinition
        {
            public void Define(PipelineBuilder builder, PipelineContext context)
            {
                var source = builder.AddSource<TestSource, int>("source");
                var sink = builder.AddSink<TrackedSink, int>("sink");

                builder.Connect(source, sink);
            }
        }

        private sealed class ParentWithFallbackCompositePipeline(DictionaryServiceProvider serviceProvider) : IPipelineDefinition
        {
            public void Define(PipelineBuilder builder, PipelineContext context)
            {
                var source = builder.AddSource<TestSource, int>("source");
                var composite = builder.AddComposite<int, int, SimpleSubPipeline>(
                    name: "composite",
                    serviceProvider: serviceProvider,
                    fallbackToParameterlessWhenServiceMissing: true);
                var sink = builder.AddSink<ParentSink, int>("sink");

                builder.Connect(source, composite);
                builder.Connect(composite, sink);
            }
        }

        private sealed class ParentWithDiCompositePipeline(
            IServiceProvider serviceProvider,
            bool fallbackToParameterlessWhenServiceMissing) : IPipelineDefinition
        {
            public void Define(PipelineBuilder builder, PipelineContext context)
            {
                var source = builder.AddSource<TestSource, int>("source");
                var composite = builder.AddComposite<int, int, DiOnlySubPipeline>(
                    name: "composite",
                    serviceProvider: serviceProvider,
                    fallbackToParameterlessWhenServiceMissing: fallbackToParameterlessWhenServiceMissing);
                var sink = builder.AddSink<ParentSink, int>("sink");

                builder.Connect(source, composite);
                builder.Connect(composite, sink);
            }
        }

        private sealed class DiOnlySubPipeline(int factor) : IPipelineDefinition
        {
            public void Define(PipelineBuilder builder, PipelineContext context)
            {
                var input = builder.AddCompositeInput<int>("input");
                var transform = builder.AddTransform<MultiplyByFactorTransform, int, int>("multiply");
                var output = builder.AddCompositeOutput<int>("output");

                builder.AddPreconfiguredNodeInstance(transform.Id, new MultiplyByFactorTransform(factor));

                builder.Connect(input, transform);
                builder.Connect(transform, output);
            }
        }

        private sealed class MultiplyByFactorTransform(int factor) : TransformNode<int, int>
        {
            public override Task<int> TransformAsync(int input, PipelineContext context, CancellationToken cancellationToken)
                => Task.FromResult(input * factor);
        }

        private sealed class DictionaryServiceProvider : IServiceProvider
        {
            private readonly Dictionary<Type, object> _services = [];

            public object? GetService(Type serviceType)
                => _services.TryGetValue(serviceType, out var service) ? service : null;

            public DictionaryServiceProvider Add(Type serviceType, object implementation)
            {
                _services[serviceType] = implementation;
                return this;
            }
        }

        #endregion
    }

#pragma warning restore IDE0058
#pragma warning restore IDE0022
#pragma warning restore IDE0011

}
