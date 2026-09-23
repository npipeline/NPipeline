using System.Collections.Concurrent;
using System.Diagnostics;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_RetryDelay;

/// <summary>
///     Runs a pipeline whose shape is supplied inline, so each demo can keep references to the node instances it
///     hands the builder and inspect them afterwards.
/// </summary>
internal sealed class InlinePipeline(Action<PipelineBuilder> define) : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        define(builder);
    }

    /// <summary>
    ///     Builds and runs the pipeline, returning how long the run took.
    /// </summary>
    public static async Task<TimeSpan> RunAsync(Action<PipelineBuilder> define)
    {
        var runner = PipelineRunner.Create();
        await using var context = new PipelineContext();

        var stopwatch = Stopwatch.StartNew();
        await runner.RunAsync(new InlinePipeline(define), context);
        return stopwatch.Elapsed;
    }
}

/// <summary>
///     A transform that fails items on demand, and records when every attempt happened so a demo can print the real
///     waits between retries.
/// </summary>
/// <param name="failure">
///     Given an item and its 1-based attempt number, returns the exception to throw, or null to pass the item through.
/// </param>
internal sealed class FlakyTransform(Func<int, int, Exception?> failure) : TransformNode<int, int>
{
    private readonly ConcurrentDictionary<int, List<long>> _attempts = new();

    /// <summary>
    ///     Fails each item <paramref name="failuresPerItem" /> times with a <see cref="TimeoutException" />, which the
    ///     default classifier treats as transient.
    /// </summary>
    public FlakyTransform(int failuresPerItem)
        : this((item, attempt) => attempt <= failuresPerItem ? new TimeoutException($"timeout on item {item}, attempt {attempt}") : null)
    {
    }

    public int AttemptsFor(int item)
    {
        return _attempts.TryGetValue(item, out var times) ? times.Count : 0;
    }

    /// <summary>
    ///     The waits between consecutive attempts of <paramref name="item" />, in milliseconds.
    /// </summary>
    public IReadOnlyList<double> GapsFor(int item)
    {
        if (!_attempts.TryGetValue(item, out var times))
            return [];

        lock (times)
        {
            return [.. times.Zip(times.Skip(1), (a, b) => Stopwatch.GetElapsedTime(a, b).TotalMilliseconds)];
        }
    }

    public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
    {
        var times = _attempts.GetOrAdd(item, _ => []);
        int attempt;

        lock (times)
        {
            times.Add(Stopwatch.GetTimestamp());
            attempt = times.Count;
        }

        if (failure(item, attempt) is { } exception)
            throw exception;

        return ValueTask.FromResult(item);
    }
}

/// <summary>
///     A source whose first <see cref="OpenStream" /> fails with a transient error, so node retry (L3) runs it again.
/// </summary>
internal sealed class FailsToOpenOnceSource(IReadOnlyList<int> items) : SourceNode<int>
{
    private readonly List<long> _opens = [];

    public int Opens => _opens.Count;

    public IReadOnlyList<long> OpenTimestamps => _opens;

    public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        _opens.Add(Stopwatch.GetTimestamp());

        if (_opens.Count == 1)
            throw new TimeoutException("the upstream service did not answer in time");

        return new DataStream<int>(items.ToAsyncEnumerable(), "fails-to-open-once");
    }
}

/// <summary>
///     A dead-letter sink that keeps what it receives in memory, standing in for a queue or table.
/// </summary>
internal sealed class CollectingDeadLetterSink : IDeadLetterSink
{
    private readonly ConcurrentQueue<DeadLetterEnvelope> _envelopes = new();

    public IReadOnlyList<DeadLetterEnvelope> Envelopes => [.. _envelopes];

    public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
    {
        _envelopes.Enqueue(envelope);
        return Task.CompletedTask;
    }
}

internal static class BuilderExtensions
{
    /// <summary>
    ///     Adds a transform node instance the demo keeps a reference to.
    /// </summary>
    public static TransformNodeHandle<int, int> AddFlaky(this PipelineBuilder builder, FlakyTransform transform, string name)
    {
        var handle = builder.AddTransform<FlakyTransform, int, int>(name);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, transform);
        return handle;
    }
}

internal static class Output
{
    public static void Heading(string title)
    {
        Console.WriteLine(title);
        Console.WriteLine(new string('-', title.Length));
    }

    public static string Ms(double milliseconds)
    {
        return $"{milliseconds,7:F1}ms";
    }

    public static string Ms(TimeSpan delay)
    {
        return Ms(delay.TotalMilliseconds);
    }

    /// <summary>
    ///     Prints the first <paramref name="retries" /> delays of a backoff curve. Retry numbers are 1-based.
    /// </summary>
    public static void Curve(string label, NPipeline.Reliability.RetryBackoff backoff, int retries = 6)
    {
        var delays = Enumerable.Range(1, retries).Select(n => Ms(backoff.DelayFor(n)));
        Console.WriteLine($"  {label,-34} {string.Join(" ", delays)}");
    }
}
