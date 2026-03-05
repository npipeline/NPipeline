namespace NPipeline.Connectors.Http.Pagination;

/// <summary>
///     A <see cref="IPaginationStrategy" /> that issues a single request and never follows up with additional pages.
/// </summary>
public sealed class NoPaginationStrategy : IPaginationStrategy
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
        return ValueTask.FromResult<Uri?>(null);
    }
}
