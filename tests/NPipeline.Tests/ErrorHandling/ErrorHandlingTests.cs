using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit.Abstractions;

namespace NPipeline.Tests.ErrorHandling;

public sealed class ErrorHandlingTests(ITestOutputHelper output)
{
    [Fact]
    public async Task Runner_WithNodeErrorHandler_ShouldSkipFailedItem()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(typeof(FailingNode).Assembly);
        services.AddSingleton<INodeErrorHandler<ITransformNode<string, string>, string>, TestErrorHandler>();

        var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredService<IPipelineRunner>();
        var diHandlerFactory = new DiHandlerFactory(provider);

        var context = new PipelineContext(
            PipelineContextConfiguration.Default with { ErrorHandlerFactory = diHandlerFactory });

        // Set source data on context
        var sourceData = new List<string> { "item1", "fail", "item2" };
        context.SetSourceData(sourceData);

        // Act
        await runner.RunAsync<FailingPipelineDefinition>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.Items.Should().BeEquivalentTo("item1", "item2");
    }

    public sealed class FailingNode : TransformNode<string, string>
    {
        public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == "fail")
                throw new InvalidOperationException("Failed on purpose");

            return Task.FromResult(item);
        }
    }

    public sealed class TestErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
    {
        public Task<NodeErrorDecision> HandleAsync(ITransformNode<string, string> node, string failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Skip);
        }
    }

    public sealed class FailingPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var failing = builder.AddTransform<FailingNode, string, string>("failing");
            var sink = builder.AddInMemorySink<string>("sink");

            failing.WithErrorHandler<string, string, TestErrorHandler>(builder);

            builder.Connect(source, failing);
            builder.Connect(failing, sink);
        }
    }
}
