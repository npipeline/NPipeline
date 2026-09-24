using System.Collections.Concurrent;
using System.Net;
using System.Text;

namespace Sample_EdgeResilience;

/// <summary>
///     Stands in for a remote inventory service, so the sample runs without a network. It answers
///     <c>GET /stock/{sku}</c> and counts every request it receives.
/// </summary>
/// <remarks>
///     <list type="bullet">
///         <item>
///             <description><c>SKU-1</c> and <c>SKU-4</c> answer at once.</description>
///         </item>
///         <item>
///             <description><c>SKU-2</c> answers 503 twice, then recovers.</description>
///         </item>
///         <item>
///             <description><c>SKU-3</c> is unknown and always answers 404.</description>
///         </item>
///         <item>
///             <description><c>SKU-5</c> is on a shard that is down, and always answers 503.</description>
///         </item>
///     </list>
/// </remarks>
internal sealed class FakeInventoryApi : HttpMessageHandler
{
    private readonly ConcurrentDictionary<string, int> _requests = new();

    public int RequestsFor(string sku) => _requests.GetValueOrDefault(sku);

    public void Reset()
    {
        _requests.Clear();
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var sku = request.RequestUri!.Segments[^1];
        var count = _requests.AddOrUpdate(sku, 1, (_, n) => n + 1);

        var response = sku switch
        {
            "SKU-2" when count <= 2 => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable),
            "SKU-3" => new HttpResponseMessage(HttpStatusCode.NotFound),
            "SKU-5" => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable),
            _ => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(Quantity(sku).ToString(), Encoding.UTF8, "text/plain"),
            },
        };

        return Task.FromResult(response);
    }

    private static int Quantity(string sku) => (sku[^1] - '0') * 12;
}
