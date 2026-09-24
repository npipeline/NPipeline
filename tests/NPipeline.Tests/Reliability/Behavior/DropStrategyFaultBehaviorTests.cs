using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.Extensions.Parallelism;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using ParallelOptions = NPipeline.Extensions.Parallelism.ParallelOptions;

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     The drop-oldest and drop-newest strategies used to let their feeder and workers fail independently. A failed
///     worker went unnoticed until the input drained, so over a never-ending input the run hung; and a failed input
///     completed the stream as if it had drained, silently truncating the result.
/// </summary>
public sealed class DropStrategyFaultBehaviorTests
{
    public static TheoryData<BoundedQueuePolicy> DropPolicies => [BoundedQueuePolicy.DropOldest, BoundedQueuePolicy.DropNewest];

    [Theory]
    [MemberData(nameof(DropPolicies))]
    public async Task AFailedWorker_FailsTheRun_EvenWhenTheInputNeverEnds(BoundedQueuePolicy policy)
    {
        var act = () => BehaviorPipeline.RunAsync(b => Wire(b, StreamingSource<int>.Unbounded([1, 2, 3]), new FlakyTransform(100), policy));

        var thrown = await act.Should().ThrowAsync<Exception>().WaitAsync(TimeSpan.FromSeconds(10));
        Flatten(thrown.Which).Should().Contain(e => e is TimeoutException);
    }

    [Theory]
    [MemberData(nameof(DropPolicies))]
    public async Task AFailedInput_FailsTheRun_InsteadOfEndingTheStream(BoundedQueuePolicy policy)
    {
        var sink = new CollectingSink<int>();
        var source = new StreamingSource<int>(ct => FailAfter([1, 2, 3], new IOException("the source's connection dropped"), ct));

        var act = () => BehaviorPipeline.RunAsync(b => Wire(b, source, new FlakyTransform(0), policy, sink));

        var thrown = await act.Should().ThrowAsync<Exception>();
        Flatten(thrown.Which).Should().Contain(e => e is IOException && e.Message == "the source's connection dropped");
    }

    private static void Wire(PipelineBuilder builder, StreamingSource<int> source, FlakyTransform transform, BoundedQueuePolicy policy,
        CollectingSink<int>? sink = null)
    {
        var s = builder.AddSource<StreamingSource<int>, int>("source");
        var t = builder.AddTransform<FlakyTransform, int, int>("transform");
        var k = builder.AddSink<CollectingSink<int>, int>("sink");

        _ = builder.AddPreconfiguredNodeInstance(s.Id, source)
            .AddPreconfiguredNodeInstance(t.Id, transform)
            .AddPreconfiguredNodeInstance(k.Id, sink ?? new CollectingSink<int>())
            .Connect(s, t)
            .Connect(t, k)
            .WithResilience(o => o with { ItemRetry = ItemRetryOptions.None })
            .WithExecutionStrategy(t, new ParallelExecutionStrategy(2));

        _ = builder.WithParallelOptions(t, new ParallelOptions(2, 1_000, policy));
    }

    private static IEnumerable<Exception> Flatten(Exception exception)
    {
        var pending = new Stack<Exception>([exception]);

        while (pending.Count > 0)
        {
            var current = pending.Pop();
            yield return current;

            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    pending.Push(inner);
                }
            }
            else if (current.InnerException is { } inner)
                pending.Push(inner);
        }
    }

    private static async IAsyncEnumerable<int> FailAfter(IEnumerable<int> items, Exception failure, [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var item in items)
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }

        await Task.Yield();
        throw failure;
    }
}
