using System.Text.RegularExpressions;

namespace NPipeline.Connectors.Http.Pagination;

/// <summary>
///     A <see cref="IPaginationStrategy" /> that parses the RFC 5988 <c>Link</c> response header
///     for a <c>rel="next"</c> relation. Compatible with GitHub, GitLab, Stripe, and similar APIs.
/// </summary>
public sealed partial class LinkHeaderPaginationStrategy : IPaginationStrategy
{
    /// <inheritdoc />
    public Uri BuildFirstPageUri(Uri baseUri)
    {
        return baseUri;
    }

    /// <inheritdoc />
    public ValueTask<Uri?> GetNextPageUriAsync(
        Uri currentUri,
        HttpResponseMessage response,
        CancellationToken cancellationToken = default)
    {
        if (!response.Headers.TryGetValues("Link", out var linkValues))
            return ValueTask.FromResult<Uri?>(null);

        foreach (var headerValue in linkValues)
        {
            var nextUri = ParseNextLink(headerValue, currentUri);

            if (nextUri != null)
                return ValueTask.FromResult<Uri?>(nextUri);
        }

        return ValueTask.FromResult<Uri?>(null);
    }

    private static Uri? ParseNextLink(string linkHeader, Uri baseUri)
    {
        // RFC 5988: Link: <url>; rel="next", <url>; rel="prev"
        foreach (var part in linkHeader.Split(','))
        {
            var trimmed = part.Trim();
            var match = LinkRelPattern().Match(trimmed);

            if (!match.Success)
                continue;

            var url = match.Groups[1].Value;
            var rel = match.Groups[2].Value;

            if (!string.Equals(rel, "next", StringComparison.OrdinalIgnoreCase))
                continue;

            // TryCreate(Uri, string) handles both absolute and relative URLs per RFC 3986.
            // Avoids a Unix/macOS quirk where bare paths like "/foo" are parsed as
            // absolute file:// URIs by TryCreate(string, UriKind.Absolute, ...).
            if (Uri.TryCreate(baseUri, url, out var resolved) && resolved.IsAbsoluteUri)
                return resolved;
        }

        return null;
    }

    [GeneratedRegex(@"<([^>]+)>\s*;\s*rel=""([^""]+)""", RegexOptions.IgnoreCase)]
    private static partial Regex LinkRelPattern();
}
