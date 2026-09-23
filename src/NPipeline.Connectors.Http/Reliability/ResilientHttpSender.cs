using System.Diagnostics;
using NPipeline.Connectors.Http.Metrics;
using NResilience;

namespace NPipeline.Connectors.Http.Reliability;

/// <summary>
///     Sends requests through an <see cref="HttpResilienceHandler" /> placed in front of any <see cref="HttpClient" />.
/// </summary>
/// <remarks>
///     The nodes accept a client from <see cref="IHttpClientFactory" /> or from the caller, so the resilience handler
///     cannot sit inside the client's own pipeline. Instead the client becomes the handler's transport: every attempt
///     is a separate <see cref="HttpClient.SendAsync(HttpRequestMessage, HttpCompletionOption, CancellationToken)" />,
///     so the client's base address, default headers, and handlers still apply to each one. This is the only layer
///     that retries; do not also add a retrying handler to the client.
/// </remarks>
internal sealed class ResilientHttpSender : IDisposable
{
    private static readonly ActivitySource ActivitySource = new("NPipeline.Connectors.Http");

    private readonly HttpMessageInvoker _invoker;

    public ResilientHttpSender(
        HttpClient client,
        NResilience.Resilience policy,
        bool bufferResponses,
        IHttpConnectorMetrics metrics,
        Action<CallEvent> listener)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(policy);

        var options = new HttpResilienceOptions { BufferResponses = bufferResponses };
        var transport = new ClientTransport(client, metrics);
        var handler = new HttpResilienceHandler(transport, policy.WithListener(listener), options);
        _invoker = new HttpMessageInvoker(handler, true);
    }

    /// <summary>
    ///     Sends the request, retrying as the policy allows, and returns the final response. A response that is still a
    ///     failure once retries are spent is returned rather than thrown, so the caller judges it.
    /// </summary>
    public Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        return _invoker.SendAsync(request, cancellationToken);
    }

    public void Dispose()
    {
        _invoker.Dispose();
    }

    /// <summary>
    ///     Forwards each attempt to the caller's client, which the sender does not own, and records it as one request.
    /// </summary>
    private sealed class ClientTransport(HttpClient client, IHttpConnectorMetrics metrics) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var endpoint = request.RequestUri?.ToString() ?? string.Empty;
            var method = request.Method.Method;

            using var activity = ActivitySource.StartActivity(
                $"HTTP {method} {request.RequestUri?.GetLeftPart(UriPartial.Path)}");

            _ = activity?.SetTag("http.method", method);
            _ = activity?.SetTag("http.url", endpoint);

            metrics.RecordRequest(endpoint, method);
            var started = Stopwatch.GetTimestamp();

            var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                .ConfigureAwait(false);

            metrics.RecordResponse(endpoint, method, (int)response.StatusCode, Stopwatch.GetElapsedTime(started));
            _ = activity?.SetTag("http.status_code", (int)response.StatusCode);
            return response;
        }
    }
}
