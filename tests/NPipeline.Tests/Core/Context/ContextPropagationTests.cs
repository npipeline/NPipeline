// ReSharper disable ClassNeverInstantiated.Local

using NPipeline.Execution;
using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

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
    public async Task ResiliencePolicy_FromBuilder_ShouldBeAvailableInContext()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var serviceProvider = services.BuildServiceProvider();
        var context = new PipelineContext();

        var runner = serviceProvider.GetRequiredService<IPipelineRunner>();
        await runner.RunAsync<PipelineWithHandler>(context);
        context.ExecutionConfiguration.ResiliencePolicy.Should().BeOfType<CapturingPipelinePolicy>();
    }

    [Fact]
    public async Task DeadLetterSink_Redirect_ShouldCaptureFailedItemWithNodeId()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        // Override failing transform & handlers / sink to be singletons for inspection
        services.AddSingleton<CapturingDeadLetterSink>();
        var serviceProvider = services.BuildServiceProvider();
        var diHandlerFactory = new DiHandlerFactory(serviceProvider);

        var context = new PipelineContext(
            PipelineContextConfiguration.Default with { ErrorHandlerFactory = diHandlerFactory });

        var runner = serviceProvider.GetRequiredService<IPipelineRunner>();
        await runner.RunAsync<RedirectPipeline>(context);
        var sink = serviceProvider.GetRequiredService<CapturingDeadLetterSink>();
        sink.Captured.Should().ContainSingle();
        var entry = sink.Captured.Single();
        entry.Attribution.DecisionNodeId.Should().Be("fail");
        entry.Item.Should().Be(42);
        entry.Error.Should().BeOfType<InvalidOperationException>();
    }

    private sealed class IdCapturingTransform(List<string> observedIds) : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            observedIds.Add(context.NodeEnvironment.GetNodeId(this));
            return ValueTask.FromResult<int>(item);
        }
    }

    private sealed class CapturingPipelinePolicy : IResiliencePolicy
    {
        public bool Called { get; private set; }

        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
        {
            Called = true;
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
        {
            Called = true;
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            Called = true;
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

    }

    private sealed class RedirectingNodePolicy : IResiliencePolicy
    {
        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.DeadLetter);
        }

    }

    private sealed class FailingTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == 42)
                throw new InvalidOperationException("boom");

            return ValueTask.FromResult<int>(item);
        }
    }

    private sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        public List<DeadLetterEnvelope> Captured { get; } = [];

        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
        {
            Captured.Add(envelope);
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
            builder.AddResiliencePolicy<CapturingPipelinePolicy>();
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
            builder.AddResiliencePolicy(fail, new RedirectingNodePolicy());
            builder.AddDeadLetterSink<CapturingDeadLetterSink>();
        }
    }

    private sealed class SimpleRedirectSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new DataStream<int>(new[] { 1, 42, 3 }.ToAsyncEnumerable());
        }
    }
}
