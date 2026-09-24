using NPipeline.Execution;
using NPipeline.Pipeline;
using NResilience;

namespace Sample_EdgeResilience;

/// <summary>
///     Resilience at the edges: a transform that calls an HTTP API retries through NResilience inside the node, and
///     the pipeline's own item retry is turned off for that node.
/// </summary>
/// <remarks>
///     The sample runs the same pipeline twice. The first run keeps the Default profile's item retry on the lookup
///     node as well, and the failing call is retried by both layers. The second run turns item retry off for that
///     node, and each call is retried by exactly one layer.
/// </remarks>
internal static class Program
{
    public static async Task Main()
    {
        Console.WriteLine("NPipeline Sample: Resilience at the Edges");
        Console.WriteLine("=========================================");
        Console.WriteLine();

        var api = new FakeInventoryApi();

        // NResilience's HTTP preset retries 408, 429, 5xx, and network failures, honors Retry-After, and never
        // retries POST or PATCH without an idempotency key. The delays are shortened so the sample finishes quickly.
        var policy = Resilience.Http with
        {
            Name = "inventory",
            Attempts = 4,
            Backoff = Backoff.Default with
            {
                TransientBase = TimeSpan.FromMilliseconds(20),
                MaximumDelay = TimeSpan.FromMilliseconds(200),
            },
            Adaptive = false,
        };

        policy = policy.WithListener(e =>
        {
            if (e.Kind == CallEventKind.Retrying)
                Console.WriteLine($"    [http] {e.Verdict} failure; making attempt {e.AttemptNumber}");
        });

        // Create one client and reuse it. The handler holds per-host state, such as a circuit breaker for each host.
        // This sample turns the per-host breaker off so that both runs send the same calls. Otherwise, the breaker
        // opens during the first run and refuses the second run's calls to the host.
        using var inventory = HttpResilience.CreateClient(policy, new HttpResilienceOptions { BreakerPerHost = false }, api);
        inventory.BaseAddress = new Uri("https://inventory.example.com/");

        await RunAsync("Run 1: HTTP retries, and pipeline item retry on the same node (not recommended)", inventory, api, true);
        await RunAsync("Run 2: HTTP retries only, pipeline item retry off for the node (recommended)", inventory, api, false);
    }

    private static async Task RunAsync(string title, HttpClient inventory, FakeInventoryApi api, bool pipelineRetriesToo)
    {
        Console.WriteLine(title);
        Console.WriteLine(new string('-', title.Length));

        api.Reset();

        var deadLetters = new DeadLetterCollector();

        await using var context = new PipelineContext();
        await PipelineRunner.Create().RunAsync(new StockPipeline(inventory, pipelineRetriesToo, deadLetters), context);

        Console.WriteLine();
        Console.WriteLine("  Requests the inventory service received:");

        foreach (var sku in StockPipeline.Skus)
        {
            Console.WriteLine($"    {sku}: {api.RequestsFor(sku)}");
        }

        Console.WriteLine();
        Console.WriteLine("  Dead-lettered:");

        foreach (var envelope in deadLetters.Envelopes)
        {
            Console.WriteLine($"    {envelope.Item}: {envelope.Error.GetBaseException().Message}");
        }

        Console.WriteLine();
    }
}
