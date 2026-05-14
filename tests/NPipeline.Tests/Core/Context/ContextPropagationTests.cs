// ReSharper disable ClassNeverInstantiated.Local

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
using NPipeline.Resilience;

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
        context.ResiliencePolicy.Should().BeOfType<CapturingPipelinePolicy>();
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
        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            observedIds.Add(context.CurrentNodeId);
            return Task.FromResult(item);
        }
    }

    private sealed class CapturingPipelinePolicy : IResiliencePolicy
    {
        public bool Called { get; private set; }

        public Task<ResilienceDecision> DecideNodeFailureAsync(
            NodeDefinition nodeDefinition,
            INode node,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            Called = true;
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(
            string nodeId,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            Called = true;
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
            Called = true;
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

    private sealed class RedirectingNodePolicy : IResiliencePolicy
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

        public Task<ResilienceDecision> DecidePipelineFailureAsync(
            string nodeId,
            Exception exception,
            PipelineContext context,
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
            return Task.FromResult(ResilienceDecision.DeadLetter);
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

    private sealed class FailingTransform : TransformNode<int, int>
    {
        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == 42)
                throw new InvalidOperationException("boom");

            return Task.FromResult(item);
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
            builder.SetNodeResiliencePolicy(fail, new RedirectingNodePolicy());
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
