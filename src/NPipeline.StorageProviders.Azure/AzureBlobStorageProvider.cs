using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Azure;
using Azure.Storage.Blobs.Models;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Storage provider for Azure Blob Storage that implements the <see cref="IStorageProvider" /> interface.
///     Handles "azure" scheme URIs and supports reading, writing, listing, and metadata operations.
/// </summary>
/// <remarks>
///     - Async-first API design
///     - Stream-based I/O for scalability
///     - Proper error handling and exception translation
///     - Cancellation token support throughout
///     - Thread-safe implementation
///     - Consistent with existing S3StorageProvider patterns
/// </remarks>
public sealed class AzureBlobStorageProvider : IStorageProvider, IStorageProviderMetadataProvider
{
    private static readonly Regex ContainerNameRegex = new(
        "^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        TimeSpan.FromMilliseconds(250));

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

    /// <summary>
    ///     Gets the storage scheme supported by this provider.
    /// </summary>
    public StorageScheme Scheme => StorageScheme.Azure;

    /// <summary>
    ///     Determines whether this provider can handle the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI to check.</param>
    /// <returns>True if the URI scheme matches "azure"; otherwise false.</returns>
    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return Scheme.Equals(uri.Scheme);
    }

    /// <summary>
    ///     Opens a readable stream for the specified Azure blob.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the Azure blob.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a readable stream for the Azure blob.</returns>
    public async Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

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
            throw TranslateAzureException(ex, container, blob);
        }
    }

    /// <summary>
    ///     Opens a writable stream for the specified Azure blob.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the Azure blob.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a writable stream for the Azure blob.</returns>
    public async Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (container, blob) = GetContainerAndBlob(uri, true);
        var blobServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        var contentType = uri.Parameters.TryGetValue("contentType", out var ct) && !string.IsNullOrEmpty(ct)
            ? ct
            : null;

        return new AzureBlobWriteStream(
            blobServiceClient,
            container,
            blob,
            contentType,
            _options.BlockBlobUploadThresholdBytes,
            _options.UploadMaximumConcurrency,
            _options.UploadMaximumTransferSizeBytes,
            cancellationToken);
    }

    /// <summary>
    ///     Checks whether an Azure blob exists at the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI to check.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>True if the Azure blob exists; otherwise false.</returns>
    public async Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

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
            throw TranslateAzureException(ex, container, blob);
        }
    }

    /// <summary>
    ///     Deletes the Azure blob at the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI of the Azure blob to delete.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    public async Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (container, blob) = GetContainerAndBlob(uri, true);
        var blobServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var blobClient = blobServiceClient.GetBlobContainerClient(container).GetBlobClient(blob);

        try
        {
            await blobClient.DeleteIfExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw TranslateAzureException(ex, container, blob);
        }
    }

    /// <summary>
    ///     Lists Azure blobs at the specified prefix.
    /// </summary>
    /// <param name="prefix">The URI prefix to list.</param>
    /// <param name="recursive">If true, recursively lists all blobs; if false, lists only blobs in the specified prefix.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>An async enumerable of <see cref="StorageItem" /> representing Azure blobs.</returns>
    public IAsyncEnumerable<StorageItem> ListAsync(
        StorageUri prefix,
        bool recursive = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return ListAsyncCore(prefix, recursive, cancellationToken);
    }

    /// <summary>
    ///     Retrieves metadata for the Azure blob at the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the Azure blob.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing <see cref="StorageMetadata" /> if the blob exists; otherwise null.</returns>
    public async Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (container, blob) = GetContainerAndBlob(uri, true);
        var blobServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var blobClient = blobServiceClient.GetBlobContainerClient(container).GetBlobClient(blob);

        try
        {
            var properties = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var customMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Add Azure-specific metadata
            foreach (var metadataKey in properties.Value.Metadata.Keys)
            {
                customMetadata[metadataKey] = properties.Value.Metadata[metadataKey];
            }

            var metadata = new StorageMetadata
            {
                Size = properties.Value.ContentLength,
                LastModified = properties.Value.LastModified,
                ContentType = properties.Value.ContentType,
                ETag = properties.Value.ETag.ToString(),
                CustomMetadata = customMetadata,
                IsDirectory = false,
            };

            return metadata;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
        catch (RequestFailedException ex)
        {
            throw TranslateAzureException(ex, container, blob);
        }
    }

    /// <summary>
    ///     Gets metadata describing this storage provider's capabilities.
    /// </summary>
    /// <returns>A <see cref="StorageProviderMetadata" /> object containing information about the provider's supported features.</returns>
    public StorageProviderMetadata GetMetadata()
    {
        return new StorageProviderMetadata
        {
            Name = "Azure Blob Storage",
            SupportedSchemes = ["azure"],
            SupportsRead = true,
            SupportsWrite = true,
            SupportsDelete = true,
            SupportsListing = true,
            SupportsMetadata = true,
            SupportsHierarchy = false, // Azure Blob Storage is flat
            Capabilities = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                ["blockBlobUploadThresholdBytes"] = _options.BlockBlobUploadThresholdBytes,
                ["supportsServiceUrl"] = true,
                ["supportsConnectionString"] = true,
                ["supportsSasToken"] = true,
                ["supportsAccountKey"] = true,
                ["supportsDefaultCredentialChain"] = true,
            },
        };
    }

    private static (string container, string blob) GetContainerAndBlob(StorageUri uri, bool requireBlob = false)
    {
        var container = uri.Host ?? string.Empty;

        ValidateContainerName(container, nameof(uri));

        var blob = uri.Path.TrimStart('/');
        ValidateBlobName(blob, requireBlob, nameof(uri));
        return (container, blob);
    }

    private async IAsyncEnumerable<StorageItem> ListAsyncCore(
        StorageUri prefix,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (container, blobPrefix) = GetContainerAndBlob(prefix);
        var blobServiceClient = await _clientFactory.GetClientAsync(prefix, cancellationToken).ConfigureAwait(false);
        var containerClient = blobServiceClient.GetBlobContainerClient(container);

        // Check if container exists before enumerating to avoid 404 exceptions during enumeration
        if (!await containerClient.ExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            // Container doesn't exist, return empty
            yield break;
        }

        if (recursive)
        {
            await foreach (var blobItem in containerClient.GetBlobsAsync(
                               BlobTraits.Metadata,
                               BlobStates.None,
                               blobPrefix,
                               cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var blobName = blobItem.Name;
                var itemUri = StorageUri.Parse($"azure://{container}/{blobName}");

                yield return new StorageItem
                {
                    Uri = itemUri,
                    Size = blobItem.Properties.ContentLength ?? 0,
                    LastModified = blobItem.Properties.LastModified ?? DateTimeOffset.UtcNow,
                    IsDirectory = false,
                };
            }
        }
        else
        {
            await foreach (var blobItem in containerClient.GetBlobsByHierarchyAsync(
                               BlobTraits.Metadata,
                               prefix: blobPrefix,
                               delimiter: "/",
                               cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (blobItem.IsPrefix)
                {
                    // Yield virtual directories as directory items
                    var prefixPath = blobItem.Prefix.TrimEnd('/');
                    var directoryUri = StorageUri.Parse($"azure://{container}/{prefixPath}");

                    yield return new StorageItem
                    {
                        Uri = directoryUri,
                        Size = 0,
                        LastModified = DateTimeOffset.UtcNow,
                        IsDirectory = true,
                    };

                    continue;
                }

                var blobName = blobItem.Blob.Name;
                var blobUri = StorageUri.Parse($"azure://{container}/{blobName}");

                yield return new StorageItem
                {
                    Uri = blobUri,
                    Size = blobItem.Blob.Properties.ContentLength ?? 0,
                    LastModified = blobItem.Blob.Properties.LastModified ?? DateTimeOffset.UtcNow,
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

    private static Exception TranslateAzureException(RequestFailedException ex, string container, string blob)
    {
        var errorCode = ex.ErrorCode ?? string.Empty;
        var status = ex.Status;
        var message = ex.Message ?? string.Empty;
        Debug.WriteLine($"TranslateAzureException: ErrorCode='{errorCode}', Message='{message}', Status={status}");

        return errorCode switch
        {
            "AuthenticationFailed" or "AuthorizationFailed" or "AuthorizationFailure" or "TokenAuthenticationFailed"
                => new UnauthorizedAccessException(
                    $"Access denied to Azure container '{container}' and blob '{blob}'. Status={status}, Code={errorCode}. {message}", ex),
            "InvalidQueryParameterValue" or "InvalidResourceName"
                => new ArgumentException(
                    $"Invalid Azure container '{container}' or blob '{blob}'. Status={status}, Code={errorCode}. {message}", ex),
            "ContainerNotFound" or "BlobNotFound"
                => new FileNotFoundException(
                    $"Azure container '{container}' or blob '{blob}' not found. Status={status}, Code={errorCode}.", ex),
            _ when status is 401 or 403
                => new UnauthorizedAccessException(
                    $"Access denied to Azure container '{container}' and blob '{blob}'. Status={status}, Code={errorCode}. {message}", ex),
            _ when status == 400
                => new ArgumentException(
                    $"Invalid Azure container '{container}' or blob '{blob}'. Status={status}, Code={errorCode}. {message}", ex),
            _ when status == 404
                => new FileNotFoundException(
                    $"Azure container '{container}' or blob '{blob}' not found. Status={status}, Code={errorCode}.", ex),
            _
                => new IOException(
                    $"Failed to access Azure container '{container}' and blob '{blob}'. Status={status}, Code={errorCode}. {message}", ex),
        };
    }
}
