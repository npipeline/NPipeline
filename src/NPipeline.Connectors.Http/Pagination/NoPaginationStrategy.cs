namespace NPipeline.Connectors.Http.Pagination;

/// <summary>
///     A <see cref="IPaginationStrategy" /> that issues a single request and never follows up with additional pages.
/// </summary>
public sealed class NoPaginationStrategy : IPaginationStrategy
{
    /// <inheritdoc />
    public Uri BuildFirstPageUri(Uri baseUri) => baseUri;

    /// <inheritdoc />
    public ValueTask<Uri?> GetNextPageUriAsync(
        Uri currentUri,
        HttpResponseMessage response,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult<Uri?>(null);
}
