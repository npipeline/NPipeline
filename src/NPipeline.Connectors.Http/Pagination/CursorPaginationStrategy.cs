using System.Text.Json;
using System.Web;

namespace NPipeline.Connectors.Http.Pagination;

/// <summary>Options for <see cref="CursorPaginationStrategy" />.</summary>
public sealed record CursorPaginationOptions
{
    /// <summary>
    ///     Dot-separated JSON property path to the cursor token in the response body.
    ///     For example <c>"meta.next_cursor"</c> navigates to <c>{ "meta": { "next_cursor": "..." } }</c>.
    /// </summary>
    public required string CursorJsonPath { get; init; }

    /// <summary>Query-string parameter name used to pass the cursor. Defaults to <c>cursor</c>.</summary>
    public string CursorParam { get; init; } = "cursor";

    /// <summary>Optional query-string parameter name for page size.</summary>
    public string? PageSizeParam { get; init; }

    /// <summary>Optional page size value to include when <see cref="PageSizeParam" /> is set.</summary>
    public int? PageSize { get; init; }
}

/// <summary>
///     A <see cref="IPaginationStrategy" /> that reads a cursor token from the response body and
///     threads it into the next request as a query-string parameter.
///     Stops when the cursor path is absent or the value is null/empty.
/// </summary>
public sealed class CursorPaginationStrategy : IPaginationStrategy
{
    private readonly CursorPaginationOptions _options;

    /// <summary>Creates a new instance with the specified options.</summary>
    public CursorPaginationStrategy(CursorPaginationOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public Uri BuildFirstPageUri(Uri baseUri)
    {
        if (_options.PageSizeParam != null && _options.PageSize.HasValue)
        {
            var builder = new UriBuilder(baseUri);
            var query = HttpUtility.ParseQueryString(builder.Query);
            query[_options.PageSizeParam] = _options.PageSize.Value.ToString();
            builder.Query = query.ToString();
            return builder.Uri;
        }

        return baseUri;
    }

    /// <inheritdoc />
    public async ValueTask<Uri?> GetNextPageUriAsync(
        Uri currentUri,
        HttpResponseMessage response,
        CancellationToken cancellationToken = default)
    {
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var cursor = ExtractCursor(body, _options.CursorJsonPath);

        if (string.IsNullOrEmpty(cursor))
            return null;

        var builder = new UriBuilder(currentUri);
        var queryParams = HttpUtility.ParseQueryString(builder.Query);
        queryParams[_options.CursorParam] = cursor;
        builder.Query = queryParams.ToString();
        return builder.Uri;
    }

    private static string? ExtractCursor(string json, string path)
    {
        if (string.IsNullOrWhiteSpace(json))
            return null;

        try
        {
            using var doc = JsonDocument.Parse(json);
            var current = doc.RootElement;

            foreach (var segment in path.Split('.'))
            {
                if (!current.TryGetProperty(segment, out current))
                    return null;
            }

            return current.ValueKind == JsonValueKind.String
                ? current.GetString()
                : null;
        }
        catch
        {
            return null;
        }
    }
}
