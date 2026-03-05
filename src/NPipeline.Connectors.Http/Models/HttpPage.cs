namespace NPipeline.Connectors.Http.Models;

/// <summary>Wraps a single page of items returned by a paginated HTTP source, along with metadata.</summary>
/// <typeparam name="T">The item type.</typeparam>
public sealed class HttpPage<T>
{
    /// <summary>Gets the items for this page.</summary>
    public required IReadOnlyList<T> Items { get; init; }

    /// <summary>Gets the URI used to fetch this page.</summary>
    public required Uri PageUri { get; init; }

    /// <summary>Gets the 1-based page index (resets to 1 per source run).</summary>
    public required int PageNumber { get; init; }

    /// <summary>Gets the HTTP status code that was returned.</summary>
    public required int StatusCode { get; init; }
}
