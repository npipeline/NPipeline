// ReSharper disable ClassNeverInstantiated.Local

using System.Reflection;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Resilience.Restart;

/// <summary>
///     Memory stays bounded by the replay window when a restartable node streams a large input.
/// </summary>

// Use a collection to avoid parallel execution with other stateful tests (shared resources & GC pressure)
[Collection("StatefulTests")]
public sealed class ResilientMemoryUsageTests
{
    private static long ForceAndGetMemory(bool full = true)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // small sleep to allow finalizer thread to finish large object cleanup (kept minimal)
        Thread.Sleep(25);
        return GC.GetTotalMemory(full);
    }

    [Fact]
    public async Task LargeStream_WithASmallReplayWindow_StreamsWithBoundedMemory()
    {
        // The replay window bounds what is held for a restart; it no longer limits the input's length (R2).
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var ctx = PipelineContext.CreateDefault();

        var memoryBefore = ForceAndGetMemory();

        await runner.RunAsync<LargeStreamSmallCapPipeline>(ctx);

        var memoryIncrease = ForceAndGetMemory() - memoryBefore;

        // 10,000 items flow through a 100-item window. Allow generous overhead for the runtime and GC variability.
        memoryIncrease.Should().BeLessThan(10 * 1024 * 1024);
    }

    [Fact]
    public async Task SuccessfulExecution_WithinCap_ShouldNotLeakMemory()
    {
        // Arrange
        const int streamSize = 50;

        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var ctx = PipelineContext.CreateDefault();

        var initialMemory = ForceAndGetMemory(false);

        // Act
        await runner.RunAsync<SuccessfulExecutionPipeline>(ctx);

        var finalMemory = ForceAndGetMemory(false);
        var memoryUsage = finalMemory - initialMemory;

        // Assert
        // Memory usage should be reasonable for the data processed
        // Increased threshold to account for runtime differences in .NET 8/9
        // GC behavior varies significantly between versions
        var maxExpectedMemory = streamSize * 200 * 1024;
        memoryUsage.Should().BeLessThan(maxExpectedMemory);
    }

    // Helper classes for the tests
    private sealed class MemoryTestResiliencePolicy : IResiliencePolicy
    {
        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);
    }

    private sealed class LargeStreamSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new DataStream<int>(Stream(cancellationToken));

            static async IAsyncEnumerable<int> Stream([EnumeratorCancellation] CancellationToken ct)
            {
                await Task.Yield();

                for (var i = 0; i < 10000; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return i;
                }
            }
        }
    }

    private sealed class SmallStreamSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new DataStream<int>(Stream(cancellationToken));

            static async IAsyncEnumerable<int> Stream([EnumeratorCancellation] CancellationToken ct)
            {
                await Task.Yield();

                for (var i = 0; i < 50; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return i;
                }
            }
        }
    }

    private sealed class MemoryIntensiveTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate some memory-intensive processing
            var data = new byte[1024]; // 1KB per item
            return ValueTask.FromResult(item);
        }
    }

    private sealed class LargeStreamSmallCapPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddSource<LargeStreamSource, int>("largeSrc");
            var t = builder.AddTransform<MemoryIntensiveTransform, int, int>("memTx");
            var k = builder.AddInMemorySink<int>("testSink");
            builder.Connect(s, t).Connect(t, k);

            builder.WithResilience(o =>
                o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 1, MaxReplayWindow = 100, Backoff = RetryBackoff.None } });

            builder.AddResiliencePolicy<MemoryTestResiliencePolicy>();
        }
    }

    private sealed class SuccessfulExecutionPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddSource<SmallStreamSource, int>("smallSrc");
            var t = builder.AddTransform<MemoryIntensiveTransform, int, int>("memTx");
            var k = builder.AddInMemorySink<int>("testSink");
            builder.Connect(s, t).Connect(t, k);

            builder.WithResilience(o =>
                o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 1, MaxReplayWindow = 200, Backoff = RetryBackoff.None } });

            builder.AddResiliencePolicy<MemoryTestResiliencePolicy>();
        }
    }
}
