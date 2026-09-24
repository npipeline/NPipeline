using System.Diagnostics;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace Sample_RetryDelay;

/// <summary>
///     What a backoff costs: computing a delay is effectively free, and waiting is the whole bill.
/// </summary>
public static class PerformanceComparison
{
    public static async Task RunAllExamples()
    {
        DelayComputationCost();
        await RunTimeByBackoff();
    }

    /// <summary>
    ///     <see cref="RetryBackoff.DelayFor" /> is arithmetic on a struct plus, with jitter, one random draw.
    /// </summary>
    public static void DelayComputationCost()
    {
        Output.Heading("Cost of computing a delay (1,000,000 calls each)");

        const int calls = 1_000_000;

        (string Name, RetryBackoff Backoff)[] curves =
        [
            ("Constant", RetryBackoff.Constant(TimeSpan.FromMilliseconds(100))),
            ("Linear, Equal jitter", RetryBackoff.Linear(TimeSpan.FromMilliseconds(100))),
            ("Exponential, Full jitter", RetryBackoff.Exponential(TimeSpan.FromMilliseconds(100), maxDelay: TimeSpan.FromSeconds(30))),
            ("Custom", RetryBackoff.Custom(retry => TimeSpan.FromMilliseconds(100 * retry * retry))),
        ];

        foreach (var (name, backoff) in curves)
        {
            var total = TimeSpan.Zero;
            var stopwatch = Stopwatch.StartNew();

            for (var i = 0; i < calls; i++)
            {
                total += backoff.DelayFor(i % 8 + 1);
            }

            stopwatch.Stop();

            // 'total' is printed so the loop cannot be optimized away.
            var nanosPerCall = stopwatch.Elapsed.TotalNanoseconds / calls;
            Console.WriteLine($"  {name,-26} {nanosPerCall,6:F1} ns/call   (sum of delays {total.TotalHours:F0}h)");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Runs the same pipeline, where every item fails twice before succeeding, under different backoffs.
    /// </summary>
    public static async Task RunTimeByBackoff()
    {
        Output.Heading("Pipeline run time by backoff (20 items, each fails twice)");

        (string Name, RetryBackoff Backoff)[] curves =
        [
            ("None", RetryBackoff.None),
            ("Constant 5ms", RetryBackoff.Constant(TimeSpan.FromMilliseconds(5))),
            ("Exponential 5ms, no jitter", RetryBackoff.Exponential(TimeSpan.FromMilliseconds(5), jitter: RetryJitter.None)),
            ("Exponential 5ms, Full jitter", RetryBackoff.Exponential(TimeSpan.FromMilliseconds(5))),
        ];

        foreach (var (name, backoff) in curves)
        {
            var elapsed = await InlinePipeline.RunAsync(builder =>
            {
                var source = builder.AddSource(() => Enumerable.Range(1, 20), "numbers");
                var work = builder.AddFlaky(new FlakyTransform(2), "work");
                var sink = builder.AddSink<int>(_ => { }, "discard");
                builder.Connect(source, work).Connect(work, sink);

                builder.WithResilience(o => o with { ItemRetry = o.ItemRetry with { MaxRetries = 2, Backoff = backoff } });
            });

            Console.WriteLine($"  {name,-30} {elapsed.TotalMilliseconds,6:F0}ms");
        }

        // Items are processed one at a time here, so every wait adds to the run time: 20 x (5 + 5)ms = 200ms for
        // the constant curve and 20 x (5 + 10)ms = 300ms for the exponential one, plus timer granularity. Full
        // jitter averages half the exponential wait.
        Console.WriteLine("  Sequential retries add their waits to the run; the backoff itself costs nanoseconds.");
        Console.WriteLine();
    }
}
