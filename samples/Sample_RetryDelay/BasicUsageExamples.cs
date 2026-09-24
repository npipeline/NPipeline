using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace Sample_RetryDelay;

/// <summary>
///     Basic usage of <see cref="RetryBackoff" />: building backoff curves and attaching them to item retry.
/// </summary>
/// <remarks>
///     A backoff is a small value type. It holds no per-sequence state: the delay depends only on the 1-based retry
///     number, so one backoff can be shared by every item and node that uses it.
/// </remarks>
public static class BasicUsageExamples
{
    public static async Task RunAllExamples()
    {
        FactoryMethods();
        await PipelineWideItemRetry();
        await PerNodeOverride();
        Validation();
    }

    /// <summary>
    ///     The factory methods. Each prints the delays before retries 1 to 6.
    /// </summary>
    public static void FactoryMethods()
    {
        Output.Heading("Backoff factory methods (delay before retry 1..6)");

        // No wait at all. This is also what default(RetryBackoff) means.
        Output.Curve("None", RetryBackoff.None);

        // The same wait every time. Jitter defaults to none for a constant backoff.
        Output.Curve("Constant(100ms)", RetryBackoff.Constant(TimeSpan.FromMilliseconds(100)));

        // step * retry, capped. Linear defaults to Equal jitter; turn it off here so the shape is visible.
        Output.Curve("Linear(50ms, max 200ms)",
            RetryBackoff.Linear(TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(200), RetryJitter.None));

        // baseDelay * factor^(retry - 1), capped.
        Output.Curve("Exponential(10ms, x2, max 250ms)",
            RetryBackoff.Exponential(TimeSpan.FromMilliseconds(10), 2, TimeSpan.FromMilliseconds(250), RetryJitter.None));

        // Full jitter (the exponential default): anywhere between zero and the computed delay.
        Output.Curve("  ... with Full jitter",
            RetryBackoff.Exponential(TimeSpan.FromMilliseconds(10), 2, TimeSpan.FromMilliseconds(250)));

        // Equal jitter: at least half the computed delay, plus a random share of the other half.
        Output.Curve("  ... with Equal jitter",
            RetryBackoff.Exponential(TimeSpan.FromMilliseconds(10), 2, TimeSpan.FromMilliseconds(250), RetryJitter.Equal));

        // Anything else: compute the delay yourself. Custom delays are never jittered.
        Output.Curve("Custom (Fibonacci x 10ms)", RetryBackoff.Custom(retry => TimeSpan.FromMilliseconds(10 * Fibonacci(retry))));

        // A backoff is a record struct, so a variant is one 'with' away.
        var steep = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(10), jitter: RetryJitter.None) with { Factor = 3 };
        Output.Curve("Exponential(10ms) with Factor = 3", steep);

        Console.WriteLine();
    }

    /// <summary>
    ///     Sets the item-retry backoff for every transform in the pipeline.
    /// </summary>
    public static async Task PipelineWideItemRetry()
    {
        Output.Heading("Pipeline-wide item retry");

        // Each item times out twice, then succeeds. TimeoutException is transient under RetryClassifier.Default.
        var transform = new FlakyTransform(2);

        var elapsed = await InlinePipeline.RunAsync(builder =>
        {
            var source = builder.AddSource(() => new[] { 1, 2, 3 }, "numbers");
            var work = builder.AddFlaky(transform, "flaky-work");
            var sink = builder.AddSink<int>(_ => { }, "discard");
            builder.Connect(source, work).Connect(work, sink);

            // WithResilience receives the optimization profile's options and returns new ones. ItemRetry
            // (L1) retries a single item's transform without touching the rest of the stream.
            builder.WithResilience(o => o with
            {
                ItemRetry = o.ItemRetry with
                {
                    MaxRetries = 3,
                    Backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(20), maxDelay: TimeSpan.FromMilliseconds(200),
                        jitter: RetryJitter.None),
                },
            });
        });

        for (var item = 1; item <= 3; item++)
        {
            Console.WriteLine($"  item {item}: {transform.AttemptsFor(item)} attempts, waits {string.Join(", ", transform.GapsFor(item).Select(Output.Ms))}");
        }

        Console.WriteLine($"  expected waits: ~20ms then ~40ms. Run took {elapsed.TotalMilliseconds:F0}ms.");
        Console.WriteLine();
    }

    /// <summary>
    ///     Gives one node its own backoff, derived from the pipeline's options.
    /// </summary>
    public static async Task PerNodeOverride()
    {
        Output.Heading("Per-node override");

        var cache = new FlakyTransform(1);
        var slowApi = new FlakyTransform(3);

        await InlinePipeline.RunAsync(builder =>
        {
            var source = builder.AddSource(() => new[] { 1 }, "numbers");
            var cacheNode = builder.AddFlaky(cache, "cache-lookup");
            var apiNode = builder.AddFlaky(slowApi, "slow-api");
            var sink = builder.AddSink<int>(_ => { }, "discard");
            builder.Connect(source, cacheNode).Connect(cacheNode, apiNode).Connect(apiNode, sink);

            // Everything retries quickly by default...
            builder.WithResilience(o => o with
            {
                ItemRetry = o.ItemRetry with { MaxRetries = 3, Backoff = RetryBackoff.Constant(TimeSpan.FromMilliseconds(10)) },
            });

            // ...but the slow API backs off linearly and gets more attempts. The function receives the pipeline's
            // options, so anything not changed here (the classifier, OnItemFailure) is inherited.
            builder.WithResilience(apiNode, o => o with
            {
                ItemRetry = o.ItemRetry with
                {
                    MaxRetries = 5,
                    Backoff = RetryBackoff.Linear(TimeSpan.FromMilliseconds(30), jitter: RetryJitter.None),
                },
            });
        });

        Console.WriteLine($"  cache-lookup waits: {string.Join(", ", cache.GapsFor(1).Select(Output.Ms))}  (constant 10ms)");
        Console.WriteLine($"  slow-api waits:     {string.Join(", ", slowApi.GapsFor(1).Select(Output.Ms))}  (linear 30, 60, 90ms)");
        Console.WriteLine();
    }

    /// <summary>
    ///     The factory methods validate their arguments, so a bad curve fails where it is written.
    /// </summary>
    public static void Validation()
    {
        Output.Heading("Validation");

        try
        {
            _ = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(100), 0.5);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            Console.WriteLine($"  Exponential(factor: 0.5)       -> {ex.Message}");
        }

        try
        {
            _ = RetryBackoff.Constant(TimeSpan.FromMilliseconds(-1));
        }
        catch (ArgumentOutOfRangeException ex)
        {
            Console.WriteLine($"  Constant(-1ms)                 -> {ex.Message}");
        }

        // A value changed with 'with' skips the factory's checks. Building the pipeline validates its options, or
        // call Validate() yourself.
        var tampered = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(100)) with { Factor = 0 };

        try
        {
            tampered.Validate();
        }
        catch (ArgumentOutOfRangeException ex)
        {
            Console.WriteLine($"  with {{ Factor = 0 }}, Validate() -> {ex.Message}");
        }

        Console.WriteLine();
    }

    private static int Fibonacci(int n)
    {
        var (a, b) = (1, 1);

        for (var i = 1; i < n; i++)
        {
            (a, b) = (b, a + b);
        }

        return a;
    }
}
