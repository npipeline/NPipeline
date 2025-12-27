// ReSharper disable ClassNeverInstantiated.Local

using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Context;

public sealed class ContextPropagationTests
{
    [Fact]
    public async Task CurrentNodeId_ShouldMatchTransformNodeId_ForEachItem()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var observedIds = new List<string>();
        services.AddSingleton(observedIds);

        // replace IdCapturingTransform registration so DI injects list
        var transformDescriptor = services.Single(d => d.ServiceType == typeof(IdCapturingTransform));
        services.Remove(transformDescriptor);
        services.AddTransient<IdCapturingTransform>(_ => new IdCapturingTransform(observedIds));

        var serviceProvider = services.BuildServiceProvider();
        var diHandlerFactory = new DiHandlerFactory(serviceProvider);

        var context = new PipelineContext(
            PipelineContextConfiguration.Default with { ErrorHandlerFactory = diHandlerFactory });

        var runner = serviceProvider.GetRequiredService<IPipelineRunner>();
        await runner.RunAsync<CurrentNodePipeline>(context);

        observedIds.Should().NotBeEmpty();
        observedIds.Should().OnlyContain(id => id == "mid");
    }

    [Fact]
    public async Task PipelineHandler_FromBuilder_ShouldBeAvailableInContext()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<CapturingPipelineHandler>();
        var serviceProvider = services.BuildServiceProvider();
        var diHandlerFactory = new DiHandlerFactory(serviceProvider);

        var context = new PipelineContext(
            PipelineContextConfiguration.Default with { ErrorHandlerFactory = diHandlerFactory });

        var runner = serviceProvider.GetRequiredService<IPipelineRunner>();
        await runner.RunAsync<PipelineWithHandler>(context);
        context.PipelineErrorHandler.Should().NotBeNull();
    }

    [Fact]
    public async Task DeadLetterSink_Redirect_ShouldCaptureFailedItemWithNodeId()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        // Override failing transform & handlers / sink to be singletons for inspection
        services.AddSingleton<CapturingDeadLetterSink>();
        services.AddSingleton<RedirectingNodeHandler>();
        var serviceProvider = services.BuildServiceProvider();
        var diHandlerFactory = new DiHandlerFactory(serviceProvider);

        var context = new PipelineContext(
            PipelineContextConfiguration.Default with { ErrorHandlerFactory = diHandlerFactory });

        var runner = serviceProvider.GetRequiredService<IPipelineRunner>();
        await runner.RunAsync<RedirectPipeline>(context);
        var sink = serviceProvider.GetRequiredService<CapturingDeadLetterSink>();
        sink.Captured.Should().ContainSingle();
        var entry = sink.Captured.Single();
        entry.nodeId.Should().Be("fail");
        entry.item.Should().Be(42);
        entry.ex.Should().BeOfType<InvalidOperationException>();
    }

    private sealed class IdCapturingTransform(List<string> observedIds) : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            observedIds.Add(context.CurrentNodeId);
            return Task.FromResult(item);
        }
    }

    private sealed class CapturingPipelineHandler : IPipelineErrorHandler
    {
        public bool Called { get; private set; }

        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception error, PipelineContext context, CancellationToken cancellationToken)
        {
            Called = true;
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
        }
    }

    private sealed class RedirectingNodeHandler : INodeErrorHandler<ITransformNode<int, int>, int>
    {
        public Task<NodeErrorDecision> HandleAsync(ITransformNode<int, int> node, int failedItem, Exception error, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.DeadLetter);
        }
    }

    private sealed class FailingTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == 42)
                throw new InvalidOperationException("boom");

            return Task.FromResult(item);
        }
    }

    private sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        public List<(string nodeId, object item, Exception ex)> Captured { get; } = [];

        public Task HandleAsync(string nodeId, object failedItem, Exception error, PipelineContext context, CancellationToken cancellationToken)
        {
            Captured.Add((nodeId, failedItem, error));
            return Task.CompletedTask;
        }
    }

    private sealed class CurrentNodePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource("src", [1, 2, 3]);
            var mid = builder.AddTransform<IdCapturingTransform, int, int>("mid");
            var sink = builder.AddInMemorySink<int>("snk");
            builder.Connect(source, mid).Connect(mid, sink);
        }
    }

    private sealed class PipelineWithHandler : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource("src", [10, 20]);
            var t = builder.AddPassThroughTransform<int, int>("t");
            var sink = builder.AddInMemorySink<int>("sink");
            builder.Connect(source, t).Connect(t, sink);
            builder.AddPipelineErrorHandler<CapturingPipelineHandler>();
        }
    }

    private sealed class RedirectPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SimpleRedirectSource, int>("src");
            var fail = builder.AddTransform<FailingTransform, int, int>("fail");
            var sink = builder.AddInMemorySink<int>("snk");
            builder.Connect(source, fail).Connect(fail, sink);
            builder.WithErrorHandler(fail, typeof(RedirectingNodeHandler));
            builder.AddDeadLetterSink<CapturingDeadLetterSink>();
        }
    }

    private sealed class SimpleRedirectSource : SourceNode<int>
    {
        public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new StreamingDataPipe<int>(new[] { 1, 42, 3 }.ToAsyncEnumerable());
        }
    }
}
