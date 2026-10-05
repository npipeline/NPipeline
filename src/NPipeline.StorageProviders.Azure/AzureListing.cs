using Azure;

namespace NPipeline.StorageProviders.Azure;

internal static class AzureListing
{
    /// <summary>
    ///     Enumerates <paramref name="source" /> without a prior existence check, which would cost a request per listing. A
    ///     404 from the service (a missing container or directory) ends the listing with no items, as the provider contract
    ///     requires; any other service failure is translated.
    /// </summary>
    public static async IAsyncEnumerable<T> GuardAsync<T>(
        IAsyncEnumerable<T> source,
        Func<RequestFailedException, Exception> translate,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var enumerator = source.GetAsyncEnumerator(cancellationToken);

        try
        {
            while (true)
            {
                bool hasNext;

                try
                {
                    hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                catch (RequestFailedException ex) when (ex.Status == 404)
                {
                    yield break;
                }
                catch (RequestFailedException ex)
                {
                    throw translate(ex);
                }

                if (!hasNext)
                    yield break;

                cancellationToken.ThrowIfCancellationRequested();

                yield return enumerator.Current;
            }
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }
}
