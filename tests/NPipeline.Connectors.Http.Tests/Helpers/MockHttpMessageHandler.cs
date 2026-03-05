using System.Net;
using System.Text;

namespace NPipeline.Connectors.Http.Tests.Helpers;

/// <summary>
///     A test double for <see cref="HttpMessageHandler" /> that returns pre-canned responses
///     and records the requests it receives.
/// </summary>
public sealed class MockHttpMessageHandler : HttpMessageHandler
{
    private readonly List<string?> _requestBodies = [];
    private readonly List<HttpRequestMessage> _requests = [];
    private readonly Queue<Func<HttpRequestMessage, HttpResponseMessage>> _responseFactories = new();

    public IReadOnlyList<HttpRequestMessage> Requests => _requests;

    /// <summary>
    ///     Request bodies captured before the request content is disposed.
    ///     Use this instead of reading <c>Content</c> from <see cref="Requests" />.
    /// </summary>
    public IReadOnlyList<string?> RequestBodies => _requestBodies;

    /// <summary>Enqueue a fixed response to be returned for the next request.</summary>
    public MockHttpMessageHandler Respond(HttpStatusCode status, string? body = null, string contentType = "application/json")
    {
        _responseFactories.Enqueue(_ =>
        {
            var response = new HttpResponseMessage(status);

            if (body != null)
                response.Content = new StringContent(body, Encoding.UTF8, contentType);

            return response;
        });

        return this;
    }

    /// <summary>Enqueue a response produced by a factory for more control.</summary>
    public MockHttpMessageHandler Respond(Func<HttpRequestMessage, HttpResponseMessage> factory)
    {
        _responseFactories.Enqueue(factory);
        return this;
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        // Read the body eagerly — the sink node disposes the StreamContent after SendAsync returns.
        var body = request.Content != null
            ? await request.Content.ReadAsStringAsync(cancellationToken)
            : null;

        _requestBodies.Add(body);
        _requests.Add(request);

        if (_responseFactories.Count == 0)
            return new HttpResponseMessage(HttpStatusCode.OK);

        var factory = _responseFactories.Dequeue();
        return factory(request);
    }
}
