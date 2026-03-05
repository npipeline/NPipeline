using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Azure;
using Azure.Storage.Blobs.Models;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     Storage provider for Azure Data Lake Storage Gen2 that implements the <see cref="IStorageProvider" /> interface.
///     Handles "adls" scheme URIs and supports reading, writing, listing, moving, deleting, and metadata operations.
/// </summary>
/// <remarks>
///     - Async-first API design
///     - Stream-based I/O for scalability
///     - Proper error handling and exception translation
///     - Cancellation token support throughout
///     - Thread-safe implementation
///     - True hierarchical namespace support (unlike Azure Blob Storage)
///     - Native atomic rename/move operations
/// </remarks>
public sealed class AdlsGen2StorageProvider
    : IStorageProvider,
        IDeletableStorageProvider,
        IMoveableStorageProvider,
        IStorageProviderMetadataProvider
{
    private static readonly Regex FilesystemNameRegex = new(
        "^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        TimeSpan.FromMilliseconds(250));

    private readonly AdlsGen2ClientFactory _clientFactory;
    private readonly AdlsGen2StorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AdlsGen2StorageProvider" /> class.
    /// </summary>
    /// <param name="clientFactory">The ADLS Gen2 client factory.</param>
    /// <param name="options">The ADLS Gen2 storage provider options.</param>
    public AdlsGen2StorageProvider(AdlsGen2ClientFactory clientFactory, AdlsGen2StorageProviderOptions options)
    {
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Deletes a file at the specified URI.
    /// </summary>
    /// <param name="uri">The URI of the file to delete.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (filesystem, path) = GetFilesystemAndPath(uri, true);
        var dataLakeServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var pathClient = dataLakeServiceClient.GetFileSystemClient(filesystem).GetFileClient(path);

        try
        {
            await pathClient.DeleteAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // Idempotent delete - silently ignore if path doesn't exist
        }
        catch (RequestFailedException ex)
        {
            throw TranslateAdlsException(ex, filesystem, path);
        }
    }

    /// <summary>
    ///     Moves a file from one location to another using ADLS Gen2's atomic rename operation.
    /// </summary>
    /// <param name="sourceUri">The source URI.</param>
    /// <param name="destinationUri">The destination URI.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="NotSupportedException">Thrown when attempting to move across storage accounts.</exception>
    public async Task MoveAsync(StorageUri sourceUri, StorageUri destinationUri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sourceUri);
        ArgumentNullException.ThrowIfNull(destinationUri);

        var (sourceFilesystem, sourcePath) = GetFilesystemAndPath(sourceUri, true);
        var (destFilesystem, destPath) = GetFilesystemAndPath(destinationUri, true);

        var sourceServiceClient = await _clientFactory.GetClientAsync(sourceUri, cancellationToken).ConfigureAwait(false);

        // For v1, we only support moves within the same storage account
        // Check if destination uses the same account/connection
        var destServiceClient = await _clientFactory.GetClientAsync(destinationUri, cancellationToken).ConfigureAwait(false);

        if (sourceServiceClient != destServiceClient)
        {
            throw new NotSupportedException(
                "Cross-account moves are not supported in ADLS Gen2 provider v1. " +
                "Source and destination must be in the same storage account.");
        }

        var sourcePathClient = sourceServiceClient.GetFileSystemClient(sourceFilesystem).GetFileClient(sourcePath);

        // ADLS Gen2 atomic rename - destination path must be relative to filesystem root.
        // If moving to a different filesystem, we need the full destination path.
        var destinationPath = sourceFilesystem == destFilesystem
            ? destPath
            : $"{destFilesystem}/{destPath}";

        try
        {
            _ = await sourcePathClient.RenameAsync(
                destinationPath,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 400)
        {
            await MoveViaBlobCopyAsync(sourceUri, destinationUri, sourceFilesystem, sourcePath, destFilesystem, destPath, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw TranslateAdlsException(ex, sourceFilesystem, sourcePath);
        }
    }

    /// <summary>
    ///     Gets the storage scheme supported by this provider.
    /// </summary>
    public StorageScheme Scheme => StorageScheme.Adls;

    /// <summary>
    ///     Determines whether this provider can handle the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI to check.</param>
    /// <returns>True if the URI scheme matches "adls"; otherwise false.</returns>
    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return Scheme.Equals(uri.Scheme);
    }

    /// <summary>
    ///     Opens a readable stream for the specified ADLS Gen2 file.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the ADLS Gen2 file.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a readable stream for the ADLS Gen2 file.</returns>
    public async Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (filesystem, path) = GetFilesystemAndPath(uri, true);
        var dataLakeServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var fileClient = dataLakeServiceClient.GetFileSystemClient(filesystem).GetFileClient(path);

        try
        {
            return await fileClient.OpenReadAsync(null, cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw TranslateAdlsException(ex, filesystem, path);
        }
    }

    /// <summary>
    ///     Opens a writable stream for the specified ADLS Gen2 file.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the ADLS Gen2 file.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a writable stream for the ADLS Gen2 file.</returns>
    public async Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (filesystem, path) = GetFilesystemAndPath(uri, true);
        _ = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var blobServiceClient = await _clientFactory.GetBlobServiceClientAsync(uri, cancellationToken).ConfigureAwait(false);

        var contentType = uri.Parameters.TryGetValue("contentType", out var ct) && !string.IsNullOrEmpty(ct)
            ? Uri.UnescapeDataString(ct)
            : null;

        return new AdlsGen2WriteStream(
            blobServiceClient,
            filesystem,
            path,
            contentType,
            _options.UploadThresholdBytes,
            _options.UploadMaximumConcurrency,
            _options.UploadMaximumTransferSizeBytes,
            cancellationToken);
    }

    /// <summary>
    ///     Checks whether an ADLS Gen2 file exists at the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI to check.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>True if the ADLS Gen2 file exists; otherwise false.</returns>
    public async Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (filesystem, path) = GetFilesystemAndPath(uri, true);
        var dataLakeServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var fileClient = dataLakeServiceClient.GetFileSystemClient(filesystem).GetFileClient(path);

        try
        {
            return await fileClient.ExistsAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return false;
        }
        catch (RequestFailedException ex)
        {
            throw TranslateAdlsException(ex, filesystem, path);
        }
    }

    /// <summary>
    ///     Lists ADLS Gen2 paths at the specified prefix.
    /// </summary>
    /// <param name="prefix">The URI prefix to list.</param>
    /// <param name="recursive">If true, recursively lists all paths; if false, lists only paths in the specified prefix.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>An async enumerable of <see cref="StorageItem" /> representing ADLS Gen2 paths.</returns>
    public IAsyncEnumerable<StorageItem> ListAsync(
        StorageUri prefix,
        bool recursive = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return ListAsyncCore(prefix, recursive, cancellationToken);
    }

    /// <summary>
    ///     Retrieves metadata for the ADLS Gen2 file at the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the ADLS Gen2 file.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing <see cref="StorageMetadata" /> if the file exists; otherwise null.</returns>
    public async Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (filesystem, path) = GetFilesystemAndPath(uri, true);
        var dataLakeServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var pathClient = dataLakeServiceClient.GetFileSystemClient(filesystem).GetFileClient(path);

        try
        {
            var properties = await pathClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var customMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Add ADLS-specific metadata
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
                IsDirectory = properties.Value.IsDirectory,
            };

            return metadata;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
        catch (RequestFailedException ex)
        {
            throw TranslateAdlsException(ex, filesystem, path);
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
            Name = "Azure Data Lake Storage Gen2",
            SupportedSchemes = ["adls"],
            SupportsRead = true,
            SupportsWrite = true,
            SupportsListing = true,
            SupportsMetadata = true,
            SupportsHierarchy = true, // ADLS Gen2 has true hierarchical namespace
            Capabilities = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                ["supportsAtomicMove"] = true,
                ["supportsNativeDelete"] = true,
                ["supportsHierarchicalListing"] = true,
                ["uploadThresholdBytes"] = _options.UploadThresholdBytes,
                ["supportsServiceUrl"] = true,
                ["supportsConnectionString"] = true,
                ["supportsSasToken"] = true,
                ["supportsAccountKey"] = true,
                ["supportsDefaultCredentialChain"] = true,
            },
        };
    }

    private async Task MoveViaBlobCopyAsync(
        StorageUri sourceUri,
        StorageUri destinationUri,
        string sourceFilesystem,
        string sourcePath,
        string destFilesystem,
        string destPath,
        CancellationToken cancellationToken)
    {
        var sourceBlobSvc = await _clientFactory.GetBlobServiceClientAsync(sourceUri, cancellationToken).ConfigureAwait(false);
        var destBlobSvc = await _clientFactory.GetBlobServiceClientAsync(destinationUri, cancellationToken).ConfigureAwait(false);

        var sourceBlob = sourceBlobSvc.GetBlobContainerClient(sourceFilesystem).GetBlobClient(sourcePath);
        var destContainer = destBlobSvc.GetBlobContainerClient(destFilesystem);
        _ = await destContainer.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        var destBlob = destContainer.GetBlobClient(destPath);

        try
        {
            var copyOp = await destBlob.StartCopyFromUriAsync(sourceBlob.Uri, cancellationToken: cancellationToken).ConfigureAwait(false);
            await copyOp.WaitForCompletionAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw TranslateAdlsException(ex, destFilesystem, destPath);
        }

        try
        {
            await sourceBlob.DeleteIfExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw TranslateAdlsException(ex, sourceFilesystem, sourcePath);
        }
    }

    private static (string filesystem, string path) GetFilesystemAndPath(StorageUri uri, bool requirePath = false)
    {
        var filesystem = uri.Host ?? string.Empty;

        ValidateFilesystemName(filesystem, nameof(uri));

        var path = uri.Path.TrimStart('/');
        ValidatePath(path, requirePath, nameof(uri));
        return (filesystem, path);
    }

    private async IAsyncEnumerable<StorageItem> ListAsyncCore(
        StorageUri prefix,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (filesystem, pathPrefix) = GetFilesystemAndPath(prefix);

        await foreach (var item in ListViaBlobFallbackAsync(prefix, filesystem, pathPrefix, recursive, cancellationToken)
                           .ConfigureAwait(false))
        {
            yield return item;
        }
    }

    private async IAsyncEnumerable<StorageItem> ListViaBlobFallbackAsync(
        StorageUri prefix,
        string filesystem,
        string pathPrefix,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var blobServiceClient = await _clientFactory.GetBlobServiceClientAsync(prefix, cancellationToken).ConfigureAwait(false);
        var containerClient = blobServiceClient.GetBlobContainerClient(filesystem);

        if (!await containerClient.ExistsAsync(cancellationToken).ConfigureAwait(false))
            yield break;

        var blobPrefix = string.IsNullOrEmpty(pathPrefix)
            ? null
            : pathPrefix.EndsWith('/')
                ? pathPrefix
                : pathPrefix + "/";

        if (recursive)
        {
            var emittedDirectories = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var prefixDepth = string.IsNullOrEmpty(blobPrefix)
                ? 0
                : blobPrefix.TrimEnd('/').Split('/').Length;

            await foreach (var blobItem in containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, blobPrefix, cancellationToken)
                               .ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var relativeName = blobItem.Name;
                var segments = relativeName.Split('/');

                for (var i = Math.Max(1, prefixDepth + 1); i < segments.Length; i++)
                {
                    var dirPath = string.Join("/", segments, 0, i);

                    if (emittedDirectories.Add(dirPath))
                    {
                        yield return new StorageItem
                        {
                            Uri = StorageUri.Parse($"adls://{filesystem}/{dirPath}"),
                            Size = 0,
                            LastModified = default,
                            IsDirectory = true,
                        };
                    }
                }

                yield return new StorageItem
                {
                    Uri = StorageUri.Parse($"adls://{filesystem}/{blobItem.Name}"),
                    Size = blobItem.Properties?.ContentLength ?? 0,
                    LastModified = blobItem.Properties?.LastModified ?? default,
                    IsDirectory = false,
                };
            }
        }
        else
        {
            await foreach (var item in containerClient.GetBlobsByHierarchyAsync(BlobTraits.None, BlobStates.None, "/", blobPrefix, cancellationToken)
                               .ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (item.IsBlob)
                {
                    yield return new StorageItem
                    {
                        Uri = StorageUri.Parse($"adls://{filesystem}/{item.Blob.Name}"),
                        Size = item.Blob.Properties?.ContentLength ?? 0,
                        LastModified = item.Blob.Properties?.LastModified ?? default,
                        IsDirectory = false,
                    };
                }
                else if (item.IsPrefix)
                {
                    var dirName = item.Prefix.TrimEnd('/');

                    yield return new StorageItem
                    {
                        Uri = StorageUri.Parse($"adls://{filesystem}/{dirName}"),
                        Size = 0,
                        LastModified = default,
                        IsDirectory = true,
                    };
                }
            }
        }
    }

    private static void ValidateFilesystemName(string filesystem, string paramName)
    {
        if (string.IsNullOrWhiteSpace(filesystem))
            throw new ArgumentException("ADLS URI must specify a filesystem name in the host component.", paramName);

        // ADLS filesystem naming rules (lowercase letters, numbers, hyphen; 3-63 chars; no leading/trailing hyphen)
        if (filesystem.Length is < 3 or > 63 || !FilesystemNameRegex.IsMatch(filesystem))
            throw new ArgumentException($"Invalid ADLS filesystem name '{filesystem}'.", paramName);
    }

    private static void ValidatePath(string path, bool requirePath, string paramName)
    {
        if (!requirePath && string.IsNullOrEmpty(path))
            return;

        if (string.IsNullOrWhiteSpace(path))
        {
            if (requirePath)
                throw new ArgumentException("ADLS URI must specify a path.", paramName);

            return;
        }

        // ADLS path can be up to 2048 chars
        if (path.Length > 2048 || path.Contains('\\') || path.Contains('?'))
            throw new ArgumentException($"Invalid ADLS path '{path}'.", paramName);
    }

    private static Exception TranslateAdlsException(RequestFailedException ex, string filesystem, string path)
    {
        var errorCode = ex.ErrorCode ?? string.Empty;
        var status = ex.Status;
        var message = ex.Message ?? string.Empty;

        var adlsException = new AdlsStorageException(
            $"ADLS operation failed for filesystem '{filesystem}' and path '{path}'. Status={status}, Code={errorCode}.",
            filesystem,
            path,
            ex);

        return errorCode switch
        {
            "AuthenticationFailed" or "AuthorizationFailed" or "AuthorizationFailure" or "TokenAuthenticationFailed"
                => new UnauthorizedAccessException(
                    $"Access denied to ADLS filesystem '{filesystem}' and path '{path}'. Status={status}, Code={errorCode}.", ex),
            "InvalidQueryParameterValue" or "InvalidResourceName"
                => new ArgumentException(
                    $"Invalid ADLS filesystem '{filesystem}' or path '{path}'. Status={status}, Code={errorCode}.", ex),
            "FilesystemNotFound" or "PathNotFound"
                => new FileNotFoundException(
                    $"ADLS filesystem '{filesystem}' or path '{path}' not found. Status={status}, Code={errorCode}.", ex),
            "PathAlreadyExists"
                => new IOException(
                    $"Path already exists in ADLS filesystem '{filesystem}' at '{path}'. Status={status}, Code={errorCode}.", adlsException),
            _ when status is 401 or 403
                => new UnauthorizedAccessException(
                    $"Access denied to ADLS filesystem '{filesystem}' and path '{path}'. Status={status}, Code={errorCode}.", ex),
            _ when status == 400
                => new ArgumentException(
                    $"Invalid ADLS filesystem '{filesystem}' or path '{path}'. Status={status}, Code={errorCode}.", ex),
            _ when status == 404
                => new FileNotFoundException(
                    $"ADLS filesystem '{filesystem}' or path '{path}' not found. Status={status}, Code={errorCode}.", ex),
            _ when status == 409
                => new IOException(
                    $"Conflict in ADLS filesystem '{filesystem}' at path '{path}'. Status={status}, Code={errorCode}.", adlsException),
            _ when status == 429 || status >= 500
                => new IOException(
                    $"Transient ADLS failure for filesystem '{filesystem}' at path '{path}'. Status={status}, Code={errorCode}.", adlsException),
            _
                => new IOException(
                    $"Failed to access ADLS filesystem '{filesystem}' and path '{path}'. Status={status}, Code={errorCode}. {message}", adlsException),
        };
    }
}
