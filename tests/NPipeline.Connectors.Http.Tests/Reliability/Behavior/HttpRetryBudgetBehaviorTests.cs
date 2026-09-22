using System.Net;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Retry;
using NPipeline.Connectors.Http.Tests.Helpers;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Http.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the Http retry-delay budget (<c>MaxTotalRetryDelay</c>), H1 in
///     <c>plans/resilience-improvements.md</c>. The sink uses PUT so the tests stay valid once non-idempotent writes
///     without a key stop being retried (D-6).
/// </summary>
public sealed class HttpRetryBudgetBehaviorTests
{
    private static readonly TimeSpan BaseDelay = TimeSpan.FromMilliseconds(20);

    [Fact]
    public async Task ManyRequests_DoNotExhaustASharedDelayBudget()
    {
        const int requests = 10;
        var handler = new MockHttpMessageHandler();

        // Each request fails once, then succeeds.
        for (var i = 0; i < requests; i++)
        {
            _ = handler.Respond(HttpStatusCode.ServiceUnavailable).Respond(HttpStatusCode.OK);
        }

        // Enough budget for five of the ten first retries if the budget is shared, and ample for any single request.
        var strategy = new RecordingRetryStrategy(BudgetedStrategy(maxRetries: 3, budget: BaseDelay * 5));

        await RunSinkAsync(handler, strategy, requests);

        strategy.Delays.Should().HaveCount(requests)
            .And.OnlyContain(d => d == BaseDelay, "each request's first retry gets the full backoff, however many requests came before it");
    }

    [Fact]
    public async Task SpentBudget_StopsRetryingInsteadOfRetryingWithoutDelay()
    {
        var handler = new MockHttpMessageHandler();

        for (var i = 0; i < 10; i++)
        {
            _ = handler.Respond(HttpStatusCode.ServiceUnavailable);
        }

        // The budget covers one and a half backoffs, so it runs out partway through the retries.
        var strategy = new RecordingRetryStrategy(BudgetedStrategy(maxRetries: 5, budget: BaseDelay * 1.5));

        var act = () => RunSinkAsync(handler, strategy, items: 1);

        _ = await act.Should().ThrowAsync<HttpRequestException>();

        // A 20ms wait, then the 10ms left in the budget, then stop. Before the fix the spent budget produced zero-delay
        // retries until MaxRetries ran out, hammering a server that was already failing.
        handler.Requests.Should().HaveCount(3, "once the budget is spent the request stops retrying");
    }

    private static ExponentialBackoffHttpRetryStrategy BudgetedStrategy(int maxRetries, TimeSpan budget)
    {
        return new ExponentialBackoffHttpRetryStrategy
        {
            MaxRetries = maxRetries,
            BaseDelayMs = (int)BaseDelay.TotalMilliseconds,
            JitterFactor = 0,
            MaxTotalRetryDelay = budget,
        };
    }

    private static async Task RunSinkAsync(MockHttpMessageHandler handler, IHttpRetryStrategy strategy, int items)
    {
        using var httpClient = new HttpClient(handler);

        var configuration = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com/items"),
            Method = SinkHttpMethod.Put,
            RetryStrategy = strategy,
        };

        var sink = new HttpSinkNode<int>(configuration, httpClient);
        await using var input = new InMemoryDataStream<int>([.. Enumerable.Range(0, items)]);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
    }

    private sealed class RecordingRetryStrategy(IHttpRetryStrategy inner) : IHttpRetryStrategy
    {
        private readonly List<TimeSpan> _delays = [];

        public IReadOnlyList<TimeSpan> Delays => _delays;

        public bool ShouldRetry(HttpResponseMessage? response, Exception? exception, int attempt)
        {
            return inner.ShouldRetry(response, exception, attempt);
        }

        public TimeSpan? MaxTotalRetryDelay => inner.MaxTotalRetryDelay;

        public TimeSpan GetDelay(HttpResponseMessage? response, int attempt)
        {
            var delay = inner.GetDelay(response, attempt);
            _delays.Add(delay);
            return delay;
        }
    }
}
