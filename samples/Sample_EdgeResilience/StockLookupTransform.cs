using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_EdgeResilience;

/// <summary>
///     The stock level of one SKU.
/// </summary>
public sealed record StockLevel(string Sku, int Quantity);

/// <summary>
///     Looks up each SKU's stock level in the inventory service.
/// </summary>
/// <remarks>
///     The node makes one call per item and judges the final response. It doesn't retry: the
///     <see cref="HttpClient" /> it's given retries at the edge, where it knows HTTP's rules, such as which methods
///     are safe to repeat and what <c>Retry-After</c> means.
/// </remarks>
internal sealed class StockLookupTransform(HttpClient inventory) : TransformNode<string, StockLevel>
{
    public override async ValueTask<StockLevel> TransformAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        using var response = await inventory.GetAsync($"stock/{item}", cancellationToken).ConfigureAwait(false);

        // After NResilience gives up, the last response comes back as is. A 503 here has already been retried.
        response.EnsureSuccessStatusCode();

        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        return new StockLevel(item, int.Parse(body));
    }
}
