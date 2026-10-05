using System.Collections.Concurrent;
using Azure;
using Azure.Storage.Blobs;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Creates containers on first use when <see cref="AzureAccountOptions.CreateContainerIfMissing" /> is set, at most once
///     per endpoint and container for the life of the provider.
/// </summary>
internal sealed class AzureContainerInitializer(AzureAccountOptions options)
{
    private readonly ConcurrentDictionary<(AzureEndpointKey Endpoint, string Container), Task> _pending = new();

    public async Task EnsureAsync(AzureEndpointKey endpoint, BlobServiceClient client, string container, CancellationToken cancellationToken)
    {
        if (!options.CreateContainerIfMissing)
            return;

        var key = (endpoint, container);

        // The shared task is not tied to the first caller's token, so one caller's cancellation cannot fail the others.
        var task = _pending.GetOrAdd(key, static (_, state) => CreateAsync(state.Client, state.Container), (Client: client, Container: container));

        try
        {
            await task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            // Forget a failed attempt so the next write tries again.
            _ = _pending.TryRemove(new KeyValuePair<(AzureEndpointKey, string), Task>(key, task));
            throw;
        }
    }

    private static async Task CreateAsync(BlobServiceClient client, string container)
    {
        try
        {
            _ = await client.GetBlobContainerClient(container).CreateIfNotExistsAsync().ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw AzureErrors.Translate(ex, container, string.Empty);
        }
    }
}
