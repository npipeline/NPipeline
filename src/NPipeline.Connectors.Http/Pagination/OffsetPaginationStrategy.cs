using System.Text.Json;
using System.Web;

namespace NPipeline.Connectors.Http.Pagination;

/// <summary>Options for <see cref="OffsetPaginationStrategy" />.</summary>
public sealed record OffsetPaginationOptions
{
    /// <summary>Query-string parameter name for the page number. Defaults to <c>page</c>.</summary>
    public string PageParam { get; init; } = "page";

    /// <summary>Query-string parameter name for the page size. Defaults to <c>pageSize</c>.</summary>
    public string PageSizeParam { get; init; } = "pageSize";

    /// <summary>Number of items per page. Defaults to <c>100</c>.</summary>
    public int PageSize { get; init; } = 100;

    /// <summary>
    ///     Optional JSON path to the total-item-count field in the response body (e.g. <c>meta.total</c>).
    ///     When set, pagination stops once all items indicated by this field have been emitted.
    /// </summary>
    public string? TotalItemsJsonPath { get; init; }

    /// <summary>One-based index of the first page. Defaults to <c>1</c>.</summary>
    public int FirstPage { get; init; } = 1;
}

/// <summary>
///     A <see cref="IPaginationStrategy" /> that manages <c>page</c> and <c>pageSize</c> (or equivalent)
///     query parameters. Stops when the response body contains fewer items than the page size.
/// </summary>
public sealed class OffsetPaginationStrategy : IPaginationStrategy
{
    private readonly OffsetPaginationOptions _options;
    private int _currentPage;
    private int _totalFetched;

    /// <summary>Creates a new instance with the specified options.</summary>
    public OffsetPaginationStrategy(OffsetPaginationOptions? options = null)
    {
        _options = options ?? new OffsetPaginationOptions();
        _currentPage = _options.FirstPage;
    }

    /// <inheritdoc />
    public Uri BuildFirstPageUri(Uri baseUri)
    {
        _currentPage = _options.FirstPage;
        _totalFetched = 0;
        return AppendPagination(baseUri, _currentPage);
    }

    /// <inheritdoc />
    public async ValueTask<Uri?> GetNextPageUriAsync(
        Uri currentUri,
        HttpResponseMessage response,
        CancellationToken cancellationToken = default)
    {
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

        // Count items returned by reading the JSON array length
        var itemCount = CountResponseItems(body, null); // null = root array
        _totalFetched += itemCount;

        // Check total from JSON path if configured
        if (_options.TotalItemsJsonPath != null)
        {
            var total = ReadIntFromJson(body, _options.TotalItemsJsonPath);

            if (total.HasValue && _totalFetched >= total.Value)
                return null;
        }

        // Stop when a short page is received (fewer items than requested)
        if (itemCount < _options.PageSize)
            return null;

        _currentPage++;
        return AppendPagination(currentUri, _currentPage);
    }

    private Uri AppendPagination(Uri uri, int page)
    {
        var builder = new UriBuilder(uri);
        var query = HttpUtility.ParseQueryString(builder.Query);
        query[_options.PageParam] = page.ToString();
        query[_options.PageSizeParam] = _options.PageSize.ToString();
        builder.Query = query.ToString();
        return builder.Uri;
    }

    private static int CountResponseItems(string json, string? path)
    {
        if (string.IsNullOrWhiteSpace(json))
            return 0;

        try
        {
            using var doc = JsonDocument.Parse(json);

            var element = path == null
                ? doc.RootElement
                : NavigatePath(doc.RootElement, path);

            return element.ValueKind == JsonValueKind.Array
                ? element.GetArrayLength()
                : 0;
        }
        catch
        {
            return 0;
        }
    }

    private static int? ReadIntFromJson(string json, string path)
    {
        if (string.IsNullOrWhiteSpace(json))
            return null;

        try
        {
            using var doc = JsonDocument.Parse(json);
            var element = NavigatePath(doc.RootElement, path);

            return element.ValueKind == JsonValueKind.Number
                ? element.GetInt32()
                : null;
        }
        catch
        {
            return null;
        }
    }

    private static JsonElement NavigatePath(JsonElement root, string path)
    {
        var current = root;

        foreach (var segment in path.Split('.'))
        {
            if (!current.TryGetProperty(segment, out current))
                return default;
        }

        return current;
    }
}
