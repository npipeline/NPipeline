using System.Net;
using System.Net.Http.Headers;
using System.Text;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Metrics;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Reliability;
using NPipeline.Connectors.Http.Tests.Helpers;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Http.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the HTTP connector's NResilience policy (H1-H4 and D-6 in <c>plans/resilience-improvements.md</c>).
///     They assert what reaches the server, not what is configured.
/// </summary>
public sealed class HttpResilienceBehaviorTests
{
    private static readonly Uri Endpoint = new("https://api.example.com/items");

    // The shipped preset with near-zero backoff, so the tests barely wait between attempts. Not Backoff.None, whose zero
    // maximum delay would also clamp a server's Retry-After to nothing.
    private static readonly Resilience Fast = HttpConnectorResilience.Default with
    {
        Backoff = HttpConnectorResilience.Default.Backoff with
        {
            TransientBase = TimeSpan.FromMilliseconds(1),
            ThrottledBase = TimeSpan.FromMilliseconds(1),
        },
    };

    [Fact]
    public void DefaultPreset_PreservesTheAttemptCountsAndDelaysOfTheRetryStrategyItReplaces()
    {
        var preset = HttpConnectorResilience.Default;

        // ExponentialBackoffHttpRetryStrategy.Default: MaxRetries = 3 (four attempts), 200 ms base, 30 s cap, 30 s timeout.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromMilliseconds(200));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.AttemptTimeout.Should().Be(TimeSpan.FromSeconds(30));
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse("the preset reproduces fixed attempt counts and timeouts");
        preset.Validate();
    }

    [Fact]
    public void ConservativePreset_PreservesTheStrategyItReplaces()
    {
        var preset = HttpConnectorResilience.Conservative;

        preset.Attempts.Should().Be(3);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(1));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(60));
        preset.Validate();
    }

    [Theory]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.RequestTimeout)]
    [InlineData(HttpStatusCode.TooManyRequests)]
    public async Task Sink_RetriesATransientStatusFourTimesThenThrows(HttpStatusCode status)
    {
        var handler = RespondWith(status, 10);

        var act = () => RunSinkAsync(handler, new HttpSinkConfiguration { Uri = Endpoint, Method = SinkHttpMethod.Put, Resilience = Fast });

        var thrown = await act.Should().ThrowAsync<HttpRequestException>();
        thrown.Which.StatusCode.Should().Be(status);
        handler.Requests.Should().HaveCount(4);
    }

    [Fact]
    public async Task Sink_DoesNotRetryAPermanentStatus()
    {
        var handler = RespondWith(HttpStatusCode.BadRequest, 10);

        var act = () => RunSinkAsync(handler, new HttpSinkConfiguration { Uri = Endpoint, Method = SinkHttpMethod.Put, Resilience = Fast });

        _ = await act.Should().ThrowAsync<HttpRequestException>();
        handler.Requests.Should().ContainSingle();
    }

    [Fact]
    public async Task Sink_RecoversWhenARetrySucceeds()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.ServiceUnavailable)
            .Respond(HttpStatusCode.OK);

        var metrics = new RecordingMetrics();

        await RunSinkAsync(handler, new HttpSinkConfiguration { Uri = Endpoint, Method = SinkHttpMethod.Put, Resilience = Fast }, metrics);

        handler.Requests.Should().HaveCount(2);
        metrics.Retries.Should().Be(1);
        metrics.Requests.Should().Be(2, "each attempt is recorded as a request");
        metrics.Errors.Should().Be(0);
    }

    [Theory]
    [InlineData(SinkHttpMethod.Post)]
    [InlineData(SinkHttpMethod.Patch)]
    public async Task Sink_SendsANonIdempotentWriteWithoutAnIdempotencyKeyOnce(SinkHttpMethod method)
    {
        // D-6: retrying a POST or PATCH the server already applied would duplicate it.
        var handler = RespondWith(HttpStatusCode.ServiceUnavailable, 10);

        var act = () => RunSinkAsync(handler, new HttpSinkConfiguration { Uri = Endpoint, Method = method, Resilience = Fast });

        _ = await act.Should().ThrowAsync<HttpRequestException>();
        handler.Requests.Should().ContainSingle();
    }

    [Fact]
    public async Task Sink_RetriesAPostThatCarriesAnIdempotencyKey_SendingTheSameKeyEachTime()
    {
        var handler = RespondWith(HttpStatusCode.ServiceUnavailable, 2).Respond(HttpStatusCode.Created);

        var configuration = new HttpSinkConfiguration
        {
            Uri = Endpoint,
            Method = SinkHttpMethod.Post,
            Resilience = Fast,
            IdempotencyKeyFactory = item => $"key-{item}",
        };

        await RunSinkAsync(handler, configuration);

        handler.Requests.Should().HaveCount(3);

        handler.Requests.Should().OnlyContain(r => r.Headers.GetValues("Idempotency-Key").Single() == "key-0");
        handler.RequestBodies.Should().OnlyContain(b => b == "0", "every attempt carries the whole body");
    }

    [Fact]
    public async Task Sink_CapturesAnErrorResponseOnlyAfterRetriesAreSpent()
    {
        // H3: CaptureErrorResponses used to capture the first 503 and drop the batch without retrying.
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.ServiceUnavailable)
            .Respond(HttpStatusCode.OK);

        var configuration = new HttpSinkConfiguration
        {
            Uri = Endpoint,
            Method = SinkHttpMethod.Put,
            Resilience = Fast,
            CaptureErrorResponses = true,
        };

        var metrics = new RecordingMetrics();
        await RunSinkAsync(handler, configuration, metrics);

        handler.Requests.Should().HaveCount(2);
        metrics.Written.Should().Be(1, "the retry delivered the batch");
    }

    [Fact]
    public async Task Sink_CapturesAPersistentErrorResponseWithoutThrowing()
    {
        var handler = RespondWith(HttpStatusCode.ServiceUnavailable, 10);

        var configuration = new HttpSinkConfiguration
        {
            Uri = Endpoint,
            Method = SinkHttpMethod.Put,
            Resilience = Fast,
            CaptureErrorResponses = true,
        };

        await RunSinkAsync(handler, configuration);

        handler.Requests.Should().HaveCount(4);
    }

    [Fact]
    public async Task Sink_ManyRequestsEachGetTheirOwnRetries()
    {
        // H1: the old delay budget was shared by every request, so later requests stopped backing off.
        const int items = 10;
        var handler = new MockHttpMessageHandler();

        for (var i = 0; i < items; i++)
        {
            _ = handler.Respond(HttpStatusCode.ServiceUnavailable).Respond(HttpStatusCode.OK);
        }

        var metrics = new RecordingMetrics();
        await RunSinkAsync(handler, new HttpSinkConfiguration { Uri = Endpoint, Method = SinkHttpMethod.Put, Resilience = Fast }, metrics, items);

        handler.Requests.Should().HaveCount(items * 2);
        metrics.Written.Should().Be(items);
    }

    [Fact]
    public async Task Sink_RetriesAnAttemptThatTimesOut()
    {
        var handler = new SlowThenFastHandler(1, TimeSpan.FromSeconds(5));

        var configuration = new HttpSinkConfiguration
        {
            Uri = Endpoint,
            Method = SinkHttpMethod.Put,
            Resilience = Fast with { AttemptTimeout = TimeSpan.FromMilliseconds(100) },
        };

        await RunSinkAsync(handler, configuration);

        handler.Attempts.Should().Be(2);
    }

    [Fact]
    public async Task Sink_RetriesAClientTimeout()
    {
        // HttpClient.Timeout surfaces as a TaskCanceledException that the pipeline did not cause.
        var handler = new SlowThenFastHandler(1, TimeSpan.FromSeconds(5));
        using var client = new HttpClient(handler) { Timeout = TimeSpan.FromMilliseconds(100) };

        var sink = new HttpSinkNode<int>(new HttpSinkConfiguration { Uri = Endpoint, Method = SinkHttpMethod.Put, Resilience = Fast }, client);
        await using var input = new InMemoryDataStream<int>([1]);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        handler.Attempts.Should().Be(2);
    }

    [Fact]
    public async Task Sink_PipelineCancellationStopsWithoutRetrying()
    {
        using var cts = new CancellationTokenSource();
        var handler = new SlowThenFastHandler(10, TimeSpan.FromSeconds(30), cts.Cancel);

        var act = () => RunSinkAsync(handler, new HttpSinkConfiguration { Uri = Endpoint, Method = SinkHttpMethod.Put, Resilience = Fast },
            cancellationToken: cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        handler.Attempts.Should().Be(1);
    }

    [Fact]
    public async Task Sink_HonorsRetryAfterOnA429()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(_ =>
            {
                var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
                response.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromMilliseconds(300));
                return response;
            })
            .Respond(HttpStatusCode.OK);

        var started = DateTimeOffset.UtcNow;
        await RunSinkAsync(handler, new HttpSinkConfiguration { Uri = Endpoint, Method = SinkHttpMethod.Put, Resilience = Fast });

        handler.Requests.Should().HaveCount(2);
        (DateTimeOffset.UtcNow - started).Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(250), "the server asked for 300 ms");
    }

    [Fact]
    public async Task Source_RetriesATransientStatusFourTimesThenThrows()
    {
        var handler = RespondWith(HttpStatusCode.BadGateway, 10);
        var metrics = new RecordingMetrics();

        var act = () => DrainSourceAsync(handler, new HttpSourceConfiguration { BaseUri = Endpoint, Resilience = Fast }, metrics);

        var thrown = await act.Should().ThrowAsync<HttpRequestException>();
        thrown.Which.StatusCode.Should().Be(HttpStatusCode.BadGateway);
        handler.Requests.Should().HaveCount(4);
        metrics.Retries.Should().Be(3);
        metrics.Errors.Should().Be(1, "the terminal failure is recorded once");
    }

    [Fact]
    public async Task Source_DoesNotRetryANotFound()
    {
        var handler = RespondWith(HttpStatusCode.NotFound, 10);

        var act = () => DrainSourceAsync(handler, new HttpSourceConfiguration { BaseUri = Endpoint, Resilience = Fast });

        _ = await act.Should().ThrowAsync<HttpRequestException>();
        handler.Requests.Should().ContainSingle();
    }

    [Fact]
    public async Task Source_RecoversAndYieldsThePage()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.ServiceUnavailable)
            .Respond(HttpStatusCode.OK, "[1,2,3]");

        var items = await DrainSourceAsync(handler, new HttpSourceConfiguration { BaseUri = Endpoint, Resilience = Fast });

        items.Should().Equal(1, 2, 3);
        handler.Requests.Should().HaveCount(2);
    }

    [Fact]
    public async Task Source_RetriesAPostQuery()
    {
        // A source's POST carries a query, not a write, so it is retried like a GET.
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.ServiceUnavailable)
            .Respond(HttpStatusCode.OK, "[1]");

        var configuration = new HttpSourceConfiguration
        {
            BaseUri = Endpoint,
            RequestMethod = HttpMethod.Post,
            RequestBodyFactory = _ => new StringContent("{\"query\":\"all\"}", Encoding.UTF8, "application/json"),
            Resilience = Fast,
        };

        var items = await DrainSourceAsync(handler, configuration);

        items.Should().Equal(1);
        handler.Requests.Should().HaveCount(2);
        handler.RequestBodies.Should().OnlyContain(b => b == "{\"query\":\"all\"}");
    }

    [Fact]
    public async Task Source_WithNoneSendsOnce()
    {
        var handler = RespondWith(HttpStatusCode.ServiceUnavailable, 10);

        var act = () => DrainSourceAsync(handler, new HttpSourceConfiguration { BaseUri = Endpoint, Resilience = Resilience.None });

        _ = await act.Should().ThrowAsync<HttpRequestException>();
        handler.Requests.Should().ContainSingle();
    }

    [Fact]
    public void Classifier_TreatsAClientTimeoutAsTransientAndOtherCancellationAsPermanent()
    {
        var classifier = HttpConnectorResilience.Classifier;

        classifier.ClassifyException(new TaskCanceledException("timeout", new TimeoutException())).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new TaskCanceledException()).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new HttpRequestException("network")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException()).Kind.Should().Be(VerdictKind.Permanent);
    }

    private static MockHttpMessageHandler RespondWith(HttpStatusCode status, int times)
    {
        var handler = new MockHttpMessageHandler();

        for (var i = 0; i < times; i++)
        {
            _ = handler.Respond(status);
        }

        return handler;
    }

    private static async Task RunSinkAsync(
        HttpMessageHandler handler,
        HttpSinkConfiguration configuration,
        IHttpConnectorMetrics? metrics = null,
        int items = 1,
        CancellationToken cancellationToken = default)
    {
        using var httpClient = new HttpClient(handler, false);
        var sink = new HttpSinkNode<int>(configuration, httpClient, metrics);

        try
        {
            await using var input = new InMemoryDataStream<int>([.. Enumerable.Range(0, items)]);
            await sink.ConsumeAsync(input, new PipelineContext(), cancellationToken);
        }
        finally
        {
            await sink.DisposeAsync();
        }
    }

    private static async Task<List<int>> DrainSourceAsync(
        HttpMessageHandler handler,
        HttpSourceConfiguration configuration,
        IHttpConnectorMetrics? metrics = null)
    {
        using var httpClient = new HttpClient(handler, false);
        var source = new HttpSourceNode<int>(configuration, httpClient, metrics);
        var items = new List<int>();

        try
        {
            await foreach (var item in source.OpenStream(new PipelineContext(), CancellationToken.None))
            {
                items.Add(item);
            }
        }
        finally
        {
            await source.DisposeAsync();
        }

        return items;
    }

    private sealed class SlowThenFastHandler(int slowAttempts, TimeSpan slowDelay, Action? onAttempt = null) : HttpMessageHandler
    {
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var attempt = Interlocked.Increment(ref _attempts);
            onAttempt?.Invoke();

            if (attempt <= slowAttempts)
                await Task.Delay(slowDelay, cancellationToken);

            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{}", Encoding.UTF8, "application/json") };
        }
    }

    private sealed class RecordingMetrics : IHttpConnectorMetrics
    {
        public int Requests { get; private set; }

        public int Retries { get; private set; }

        public int Errors { get; private set; }

        public int Written { get; private set; }

        public void RecordRequest(string endpoint, string method)
        {
            Requests++;
        }

        public void RecordResponse(string endpoint, string method, int statusCode, TimeSpan latency)
        {
        }

        public void RecordRetry(string endpoint, string method, int attempt)
        {
            Retries++;
        }

        public void RecordRateLimitWait(string endpoint, TimeSpan waited)
        {
        }

        public void RecordError(string endpoint, string method, Exception ex)
        {
            Errors++;
        }

        public void RecordPageFetched(string endpoint, int itemCount)
        {
        }

        public void RecordSinkWritten(string endpoint, string method, int statusCode)
        {
            Written++;
        }
    }
}
