using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Azure;
using Azure.Storage.Blobs.Models;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Storage provider for Azure Blob Storage.
///     Handles "azure" scheme URIs and supports reading, writing, listing and metadata operations.
/// </summary>
/// <remarks>
///     Declares <see cref="StorageCapabilities.Read" />, <see cref="StorageCapabilities.Write" />,
///     <see cref="StorageCapabilities.List" />, <see cref="StorageCapabilities.Delete" /> and <see cref="StorageCapabilities.Move" />
///     (a server-side copy, then a delete, so it is not atomic). The namespace is flat, so it is not a <see cref="StorageCapabilities.Hierarchy" /> provider.
/// </remarks>
public sealed class AzureBlobStorageProvider : StorageProvider
{
    private static readonly Regex ContainerNameRegex = new(
        "^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        TimeSpan.FromMilliseconds(250));

    private static readonly IReadOnlyList<StorageScheme> SchemeList = [StorageScheme.Azure];

    private readonly AzureBlobClientFactory _clientFactory;
    private readonly AzureBlobStorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureBlobStorageProvider" /> class.
    /// </summary>
    /// <param name="clientFactory">The Azure Blob client factory.</param>
    /// <param name="options">The Azure storage provider options.</param>
    public AzureBlobStorageProvider(AzureBlobClientFactory clientFactory, AzureBlobStorageProviderOptions options)
    {
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public override string Name => "Azure Blob Storage";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => SchemeList;

    /// <inheritdoc />
    public override StorageCapabilities Capabilities => StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List
        | StorageCapabilities.Delete | StorageCapabilities.Move;

    /// <inheritdoc />
    protected override async Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (container, blob) = GetContainerAndBlob(uri, true);
        var blobServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var blobClient = blobServiceClient.GetBlobContainerClient(container).GetBlobClient(blob);

        try
        {
            // Prefer OpenReadAsync for streaming and range support
            // Call the instance method directly instead of the extension method for better testability
            return await blobClient.OpenReadAsync(null, cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw AzureErrors.Translate(ex, container, blob);
        }
    }

    /// <inheritdoc />
    protected override async Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
    {
        var (container, blob) = GetContainerAndBlob(uri, true);
        var blobServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        var contentType = !string.IsNullOrEmpty(options?.ContentType)
            ? options.ContentType
            : uri.Parameters.TryGetValue("contentType", out var ct) && !string.IsNullOrEmpty(ct)
                ? ct
                : null;

        return new PassThroughWriteStream(new AzureBlobWriteStream(
            blobServiceClient,
            container,
            blob,
            contentType,
            _options.BlockBlobUploadThresholdBytes,
            _options.UploadMaximumConcurrency,
            _options.UploadMaximumTransferSizeBytes,
            cancellationToken));
    }

    /// <inheritdoc />
    protected override async Task<bool> ExistsCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (container, blob) = GetContainerAndBlob(uri, true);
        var blobServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var blobClient = blobServiceClient.GetBlobContainerClient(container).GetBlobClient(blob);

        try
        {
            return await blobClient.ExistsAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return false;
        }
        catch (RequestFailedException ex)
        {
            throw AzureErrors.Translate(ex, container, blob);
        }
    }

    /// <inheritdoc />
    protected override async Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (container, blob) = GetContainerAndBlob(uri, true);
        var blobServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var blobClient = blobServiceClient.GetBlobContainerClient(container).GetBlobClient(blob);

        try
        {
            var properties = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var customMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var metadataKey in properties.Value.Metadata.Keys)
            {
                customMetadata[metadataKey] = properties.Value.Metadata[metadataKey];
            }

            return new StorageMetadata
            {
                Size = properties.Value.ContentLength,
                LastModified = properties.Value.LastModified,
                ContentType = properties.Value.ContentType,
                ETag = properties.Value.ETag.ToString(),
                CustomMetadata = customMetadata,
                IsDirectory = false,
            };
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
        catch (RequestFailedException ex)
        {
            throw AzureErrors.Translate(ex, container, blob);
        }
    }

    /// <inheritdoc />
    protected override async Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (container, blob) = GetContainerAndBlob(uri, true);
        var blobServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var blobClient = blobServiceClient.GetBlobContainerClient(container).GetBlobClient(blob);

        try
        {
            // DeleteIfExists is idempotent: a missing blob (or container) is not an error.
            _ = await blobClient.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // The container does not exist, so there is nothing to delete.
        }
        catch (RequestFailedException ex)
        {
            throw AzureErrors.Translate(ex, container, blob);
        }
    }

    /// <inheritdoc />
    protected override async Task MoveCoreAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken)
    {
        var (sourceContainer, sourceBlob) = GetContainerAndBlob(source, true);
        var (destinationContainer, destinationBlob) = GetContainerAndBlob(destination, true);

        if (sourceContainer == destinationContainer && sourceBlob == destinationBlob)
        {
            // Copying a blob onto itself fails; moving it onto itself changes nothing once it is known to exist.
            if (!await ExistsCoreAsync(source, cancellationToken).ConfigureAwait(false))
                throw new FileNotFoundException($"Azure blob '{sourceBlob}' in container '{sourceContainer}' not found.");

            return;
        }

        if (!source.Parameters.OrderBy(p => p.Key, StringComparer.Ordinal).SequenceEqual(destination.Parameters.OrderBy(p => p.Key, StringComparer.Ordinal))
            || source.UserName != destination.UserName)
        {
            throw new ArgumentException("Moving a blob between storage accounts is not supported; the source and destination must use the same account parameters.", nameof(destination));
        }

        var blobServiceClient = await _clientFactory.GetClientAsync(source, cancellationToken).ConfigureAwait(false);
        var sourceClient = blobServiceClient.GetBlobContainerClient(sourceContainer).GetBlobClient(sourceBlob);
        var destinationClient = blobServiceClient.GetBlobContainerClient(destinationContainer).GetBlobClient(destinationBlob);

        try
        {
            // A copy within one account is authorized by the request's own credentials, and overwrites the destination.
            var copy = await destinationClient.StartCopyFromUriAsync(sourceClient.Uri, cancellationToken: cancellationToken).ConfigureAwait(false);
            _ = await copy.WaitForCompletionAsync(cancellationToken).ConfigureAwait(false);
            _ = await sourceClient.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw AzureErrors.Translate(ex, sourceContainer, sourceBlob);
        }
    }

    private static (string container, string blob) GetContainerAndBlob(StorageUri uri, bool requireBlob = false)
    {
        var container = uri.Host ?? string.Empty;

        ValidateContainerName(container, nameof(uri));

        var blob = uri.Path.TrimStart('/');
        ValidateBlobName(blob, requireBlob, nameof(uri));
        return (container, blob);
    }

    /// <inheritdoc />
    protected override async IAsyncEnumerable<StorageItem> ListCoreAsync(
        StorageUri directory,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (container, blobPrefix) = GetContainerAndBlob(directory);
        var blobServiceClient = await _clientFactory.GetClientAsync(directory, cancellationToken).ConfigureAwait(false);
        var containerClient = blobServiceClient.GetBlobContainerClient(container);

        // Check if container exists before enumerating to avoid 404 exceptions during enumeration
        if (!await containerClient.ExistsAsync(cancellationToken).ConfigureAwait(false))
            yield break;

        if (recursive)
        {
            await foreach (var blobItem in containerClient.GetBlobsAsync(
                               BlobTraits.Metadata,
                               BlobStates.None,
                               blobPrefix,
                               cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Zero-byte "folder marker" blobs (names ending in '/') are not files.
                if (blobItem.Name.EndsWith('/'))
                    continue;

                yield return new StorageItem
                {
                    Uri = directory.WithPath("/" + blobItem.Name),
                    Size = blobItem.Properties.ContentLength,
                    LastModified = blobItem.Properties.LastModified,
                    IsDirectory = false,
                };
            }
        }
        else
        {
            await foreach (var blobItem in containerClient.GetBlobsByHierarchyAsync(
                               BlobTraits.Metadata,
                               BlobStates.None,
                               prefix: blobPrefix,
                               delimiter: "/",
                               cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (blobItem.IsPrefix)
                {
                    yield return new StorageItem
                    {
                        Uri = directory.WithPath("/" + blobItem.Prefix),
                        IsDirectory = true,
                    };

                    continue;
                }

                if (blobItem.Blob.Name.EndsWith('/'))
                    continue;

                yield return new StorageItem
                {
                    Uri = directory.WithPath("/" + blobItem.Blob.Name),
                    Size = blobItem.Blob.Properties.ContentLength,
                    LastModified = blobItem.Blob.Properties.LastModified,
                    IsDirectory = false,
                };
            }
        }
    }

    private static void ValidateContainerName(string container, string paramName)
    {
        if (string.IsNullOrWhiteSpace(container))
            throw new ArgumentException("Azure URI must specify a container name in the host component.", paramName);

        // Azure container naming rules (lowercase letters, numbers, hyphen; 3-63 chars; no leading/trailing hyphen)
        if (container.Length is < 3 or > 63 || !ContainerNameRegex.IsMatch(container))
            throw new ArgumentException($"Invalid Azure container name '{container}'.", paramName);
    }

    private static void ValidateBlobName(string blob, bool requireBlob, string paramName)
    {
        if (!requireBlob && string.IsNullOrEmpty(blob))
            return;

        if (string.IsNullOrWhiteSpace(blob))
        {
            if (requireBlob)
                throw new ArgumentException("Azure URI must specify a blob path.", paramName);

            return;
        }

        if (blob.Length > 1024 || blob.Contains('\\') || blob.Contains('?'))
            throw new ArgumentException($"Invalid Azure blob name '{blob}'.", paramName);
    }
}
