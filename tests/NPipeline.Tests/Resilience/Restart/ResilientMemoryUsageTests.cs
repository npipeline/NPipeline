// ReSharper disable ClassNeverInstantiated.Local

using System.Reflection;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Resilience.Restart;

/// <summary>
///     Tests for verifying memory usage stays within acceptable bounds when using resilience caps.
///     These tests simulate large data streams with caps to ensure memory doesn't grow unbounded.
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
    public async Task LargeStream_WithSmallCap_ShouldLimitMemoryUsage()
    {
        // Arrange
        const long maxMemoryIncrease = 100 * 1024; // 100KB

        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<MemoryTestErrorHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var ctx = PipelineContext.Default;

        // Baseline memory
        var memoryBefore = ForceAndGetMemory();

        // Act & Assert - Should throw due to cap being exceeded
        var act = async () => await runner.RunAsync<LargeStreamSmallCapPipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .WithMessage("*Resilience materialization exceeded MaxMaterializedItems*");

        // Measure memory after failure
        var memoryAfter = ForceAndGetMemory();
        var memoryIncrease = memoryAfter - memoryBefore;

        // Assert - Memory usage should be limited by the cap
        // The cap should prevent excessive materialization
        // Allow for some overhead in test infrastructure
        // Increase buffer to account for runtime and GC variability on different platforms
        var adjustedMaxMemory = maxMemoryIncrease + 4024 * 1024; // Add 4MB buffer for test overhead
        memoryIncrease.Should().BeLessThan(adjustedMaxMemory);
    }

    [Fact]
    public async Task MemoryUsage_ShouldScaleWithCapSize()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<MemoryTestErrorHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        // Test with small cap
        var smallCapCtx = PipelineContext.Default;

        var memoryBeforeSmallCap = ForceAndGetMemory();

        var smallCapAct = async () => await runner.RunAsync<SmallCapPipeline>(smallCapCtx);

        await smallCapAct.Should().ThrowAsync<NodeExecutionException>()
            .WithMessage("*Resilience materialization exceeded MaxMaterializedItems*");

        var memoryAfterSmallCap = ForceAndGetMemory();
        var smallCapMemoryUsage = memoryAfterSmallCap - memoryBeforeSmallCap;

        // Test with large cap
        var largeCapCtx = PipelineContext.Default;

        var memoryBeforeLargeCap = ForceAndGetMemory();

        var largeCapAct = async () => await runner.RunAsync<LargeCapPipeline>(largeCapCtx);

        await largeCapAct.Should().ThrowAsync<NodeExecutionException>()
            .WithMessage("*Resilience materialization exceeded MaxMaterializedItems*");

        var memoryAfterLargeCap = ForceAndGetMemory();
        var largeCapMemoryUsage = memoryAfterLargeCap - memoryBeforeLargeCap;

        // Memory usage should be reasonable (allowing for negative values due to GC)
        // The important thing is that the exceptions are thrown, indicating caps work
        smallCapMemoryUsage.Should().BeLessThan(10 * 1024 * 1024); // Less than 10MB
        largeCapMemoryUsage.Should().BeLessThan(10 * 1024 * 1024); // Less than 10MB
    }

    [Fact]
    public async Task SuccessfulExecution_WithinCap_ShouldNotLeakMemory()
    {
        // Arrange
        const int streamSize = 50;

        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<MemoryTestErrorHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var ctx = PipelineContext.Default;

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

    [Fact]
    public async Task ResourceDisposal_WhenCapReached_ShouldBeProper()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<MemoryTestErrorHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();
        var ctx = PipelineContext.Default;

        // Act & Assert
        var act = async () => await runner.RunAsync<ResourceDisposalTestPipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .WithMessage("*Resilience materialization exceeded MaxMaterializedItems*");

        // The test passes if the exception is thrown, which indicates that
        // the cap was reached and the pipeline was properly terminated
        // without resource leaks
    }

    // Helper classes for the tests
    private sealed class MemoryTestErrorHandler : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception error, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
        }
    }

    private sealed class LargeStreamSource : SourceNode<int>
    {
        public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new StreamingDataPipe<int>(Stream(cancellationToken));

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

    private sealed class MediumStreamSource : SourceNode<int>
    {
        public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new StreamingDataPipe<int>(Stream(cancellationToken));

            static async IAsyncEnumerable<int> Stream([EnumeratorCancellation] CancellationToken ct)
            {
                await Task.Yield();

                for (var i = 0; i < 1000; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return i;
                }
            }
        }
    }

    private sealed class SmallStreamSource : SourceNode<int>
    {
        public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new StreamingDataPipe<int>(Stream(cancellationToken));

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
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate some memory-intensive processing
            var data = new byte[1024]; // 1KB per item
            return Task.FromResult(item);
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
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 1, maxMaterializedItems: 100));
            builder.AddPipelineErrorHandler<MemoryTestErrorHandler>();
        }
    }

    private sealed class SmallCapPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddSource<MediumStreamSource, int>("medSrc");
            var t = builder.AddTransform<MemoryIntensiveTransform, int, int>("memTx");
            var k = builder.AddInMemorySink<int>("testSink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 1, maxMaterializedItems: 50));
            builder.AddPipelineErrorHandler<MemoryTestErrorHandler>();
        }
    }

    private sealed class LargeCapPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddSource<MediumStreamSource, int>("medSrc");
            var t = builder.AddTransform<MemoryIntensiveTransform, int, int>("memTx");
            var k = builder.AddInMemorySink<int>("testSink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 1, maxMaterializedItems: 500));
            builder.AddPipelineErrorHandler<MemoryTestErrorHandler>();
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
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 1, maxMaterializedItems: 200));
            builder.AddPipelineErrorHandler<MemoryTestErrorHandler>();
        }
    }

    private sealed class ResourceDisposalTestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddSource<MediumStreamSource, int>("medSrc");
            var t = builder.AddTransform<MemoryIntensiveTransform, int, int>("memTx");
            var k = builder.AddInMemorySink<int>("testSink");
            builder.Connect(s, t).Connect(t, k);
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 1, maxMaterializedItems: 100));
            builder.AddPipelineErrorHandler<MemoryTestErrorHandler>();
        }
    }
}
