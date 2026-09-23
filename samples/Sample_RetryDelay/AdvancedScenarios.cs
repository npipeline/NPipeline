using System.Diagnostics;
using System.Net;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace Sample_RetryDelay;

/// <summary>
///     What decides whether a retry happens at all, what happens when it does not, and how jitter spreads delays.
/// </summary>
public static class AdvancedScenarios
{
    public static async Task RunAllExamples()
    {
        ClassifierRules();
        await ClassifierInAPipeline();
        await NodeRetryBackoff();
        await SkipAndDeadLetter();
        JitterSpread();
    }

    /// <summary>
    ///     A backoff only says how long to wait. The classifier says whether to retry at all: only transient failures
    ///     are retried, and a permanent one goes straight to <see cref="ItemFailureAction" />.
    /// </summary>
    public static void ClassifierRules()
    {
        Output.Heading("Classifier rules");

        // Rules are checked in the order added, before the built-in ones.
        var classifier = RetryClassifier.Default
            .Transient<InvalidOperationException>(e => e.Message.Contains("busy", StringComparison.Ordinal))
            .Permanent<HttpRequestException>(e => e.StatusCode == HttpStatusCode.ServiceUnavailable);

        Exception[] failures =
        [
            new TimeoutException("timed out"),
            new HttpRequestException("503", null, HttpStatusCode.ServiceUnavailable),
            new HttpRequestException("502", null, HttpStatusCode.BadGateway),
            new InvalidOperationException("server busy"),
            new InvalidOperationException("bad state"),
            new FormatException("bad format"),
        ];

        Console.WriteLine($"  {"exception",-44} {"Default",-10} {"custom",-10} All");

        foreach (var failure in failures)
        {
            var label = failure is HttpRequestException http
                ? $"HttpRequestException ({(int)http.StatusCode!})"
                : $"{failure.GetType().Name} (\"{failure.Message}\")";

            Console.WriteLine(
                $"  {label,-44} {Verdict(RetryClassifier.Default, failure),-10} {Verdict(classifier, failure),-10} {Verdict(RetryClassifier.All, failure)}");
        }

        Console.WriteLine();

        static string Verdict(RetryClassifier c, Exception e)
        {
            return c.IsTransient(e, CancellationToken.None) ? "transient" : "permanent";
        }
    }

    /// <summary>
    ///     The same classifier attached to item retry: the transient item is retried, the permanent one is not.
    /// </summary>
    public static async Task ClassifierInAPipeline()
    {
        Output.Heading("Classifier in a pipeline");

        // Item 1 fails with a "busy" error our rule makes transient; item 2 with one that stays permanent.
        var transform = new FlakyTransform((item, attempt) => attempt <= 2
            ? new InvalidOperationException(item == 1 ? "server busy" : "bad state")
            : null);

        await InlinePipeline.RunAsync(builder =>
        {
            var source = builder.AddSource(() => new[] { 1, 2 }, "numbers");
            var work = builder.AddFlaky(transform, "work");
            var sink = builder.AddSink<int>(_ => { }, "discard");
            builder.Connect(source, work).Connect(work, sink);

            builder.WithResilience(o => o with
            {
                ItemRetry = o.ItemRetry with
                {
                    MaxRetries = 3,
                    Backoff = RetryBackoff.Constant(TimeSpan.FromMilliseconds(10)),
                    Classifier = RetryClassifier.Default.Transient<InvalidOperationException>(e =>
                        e.Message.Contains("busy", StringComparison.Ordinal)),
                },

                // A permanent failure is not retried; Skip drops the item instead of failing the pipeline.
                OnItemFailure = ItemFailureAction.Skip,
            });
        });

        Console.WriteLine($"  item 1 (\"server busy\", transient): {transform.AttemptsFor(1)} attempts, then succeeded");
        Console.WriteLine($"  item 2 (\"bad state\", permanent):   {transform.AttemptsFor(2)} attempt, then skipped");
        Console.WriteLine();
    }

    /// <summary>
    ///     Node retry (L3) runs a whole failed node again, with its own backoff. It is the only layer that covers
    ///     sources and sinks.
    /// </summary>
    public static async Task NodeRetryBackoff()
    {
        Output.Heading("Node retry backoff");

        var source = new FailsToOpenOnceSource([1, 2, 3]);
        var received = 0;

        await InlinePipeline.RunAsync(builder =>
        {
            var s = builder.AddSource(source, "flaky-source");
            var sink = builder.AddSink<int>(_ => received++, "count");
            builder.Connect(s, sink);

            // The default node-retry backoff starts at 1s. A sample does not need to wait that long.
            builder.WithResilience(o => o with
            {
                NodeRetry = new NodeRetryOptions
                {
                    MaxRetries = 2,
                    Backoff = RetryBackoff.Constant(TimeSpan.FromMilliseconds(50)),
                },
            });
        });

        var wait = Stopwatch.GetElapsedTime(source.OpenTimestamps[0], source.OpenTimestamps[1]);
        Console.WriteLine($"  source opened {source.Opens} times, waited {Output.Ms(wait)} between them, sink received {received} items");
        Console.WriteLine();
    }

    /// <summary>
    ///     What happens to an item whose failure is not retried, either because it is permanent or because its
    ///     retries ran out.
    /// </summary>
    public static async Task SkipAndDeadLetter()
    {
        Output.Heading("OnItemFailure: Skip and DeadLetter");

        foreach (var action in new[] { ItemFailureAction.Skip, ItemFailureAction.DeadLetter })
        {
            // Item 3 always times out, so it exhausts its retries; the others pass.
            var transform = new FlakyTransform((item, attempt) => item == 3 ? new TimeoutException($"item 3 timed out (attempt {attempt})") : null);
            var deadLetters = new CollectingDeadLetterSink();
            var received = new List<int>();

            await InlinePipeline.RunAsync(builder =>
            {
                var source = builder.AddSource(() => new[] { 1, 2, 3, 4 }, "numbers");
                var work = builder.AddFlaky(transform, "work");
                var sink = builder.AddSink<int>(received.Add, "collect");
                builder.Connect(source, work).Connect(work, sink);

                builder.WithResilience(o => o with
                {
                    ItemRetry = o.ItemRetry with { MaxRetries = 2, Backoff = RetryBackoff.Constant(TimeSpan.FromMilliseconds(5)) },
                    OnItemFailure = action,
                });

                // DeadLetter needs somewhere to send items; without a sink the run fails before any node starts.
                if (action == ItemFailureAction.DeadLetter)
                    builder.AddDeadLetterSink(deadLetters);
            });

            Console.WriteLine(
                $"  {action}: item 3 tried {transform.AttemptsFor(3)} times; sink received [{string.Join(", ", received)}], dead letters: {deadLetters.Envelopes.Count}");

            foreach (var envelope in deadLetters.Envelopes)
            {
                Console.WriteLine(
                    $"    item {envelope.Item} from '{envelope.Attribution.OriginNodeId}': {envelope.Error.GetBaseException().Message}");
            }
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Jitter keeps many failing callers from retrying in lockstep. Sampling <see cref="RetryBackoff.DelayFor" />
    ///     shows the range each kind draws from.
    /// </summary>
    public static void JitterSpread()
    {
        Output.Heading("Jitter spread: min / mean / max of 10,000 draws per retry (exponential 100ms, x2)");

        foreach (var jitter in new[] { RetryJitter.None, RetryJitter.Full, RetryJitter.Equal })
        {
            var backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(100), jitter: jitter);
            Console.WriteLine($"  {jitter}:");

            for (var retry = 1; retry <= 3; retry++)
            {
                var draws = Enumerable.Range(0, 10_000).Select(_ => backoff.DelayFor(retry).TotalMilliseconds).ToArray();
                Console.WriteLine($"    retry {retry}: {Output.Ms(draws.Min())} / {Output.Ms(draws.Average())} / {Output.Ms(draws.Max())}");
            }
        }

        Console.WriteLine();
    }
}
