using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Azure;
using Azure.Storage.Blobs.Models;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     Storage provider for Azure Data Lake Storage Gen2.
///     Handles "adls" scheme URIs and supports reading, writing, listing, moving, deleting, and metadata operations.
/// </summary>
/// <remarks>
///     Declares <see cref="StorageCapabilities.Hierarchy" />. <see cref="StorageCapabilities.AtomicMove" /> is not declared
///     because <see cref="MoveCoreAsync" /> still falls back to copy-and-delete when the rename is rejected; that fallback is
///     scheduled for removal, after which the provider can declare it.
/// </remarks>
public sealed class AdlsGen2StorageProvider : StorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> SchemeList = [StorageScheme.Adls];

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

    /// <inheritdoc />
    public override string Name => "Azure Data Lake Storage Gen2";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => SchemeList;

    /// <inheritdoc />
    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete | StorageCapabilities.Move |
        StorageCapabilities.Hierarchy;

    /// <inheritdoc />
    protected override async Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
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
            throw AdlsErrors.Translate(ex, filesystem, path);
        }
    }

    /// <inheritdoc />
    protected override async Task MoveCoreAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken)
    {
        var (sourceFilesystem, sourcePath) = GetFilesystemAndPath(source, true);
        var (destFilesystem, destPath) = GetFilesystemAndPath(destination, true);

        var sourceServiceClient = await _clientFactory.GetClientAsync(source, cancellationToken).ConfigureAwait(false);

        // For v1, we only support moves within the same storage account
        // Check if destination uses the same account/connection
        var destServiceClient = await _clientFactory.GetClientAsync(destination, cancellationToken).ConfigureAwait(false);

        if (sourceServiceClient != destServiceClient)
        {
            throw new NotSupportedException(
                "Cross-account moves are not supported in ADLS Gen2 provider v1. " +
                "Source and destination must be in the same storage account.");
        }

        var sourcePathClient = sourceServiceClient.GetFileSystemClient(sourceFilesystem).GetFileClient(sourcePath);

        // ADLS Gen2 atomic rename. The destination path is relative to its filesystem, which is passed separately:
        // a null destinationFileSystem renames within the source filesystem.
        try
        {
            _ = await sourcePathClient.RenameAsync(
                destPath,
                sourceFilesystem == destFilesystem
                    ? null
                    : destFilesystem,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 400)
        {
            await MoveViaBlobCopyAsync(source, destination, sourceFilesystem, sourcePath, destFilesystem, destPath, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw AdlsErrors.Translate(ex, sourceFilesystem, sourcePath);
        }
    }

    /// <inheritdoc />
    protected override async Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (filesystem, path) = GetFilesystemAndPath(uri, true);
        var dataLakeServiceClient = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var fileClient = dataLakeServiceClient.GetFileSystemClient(filesystem).GetFileClient(path);

        try
        {
            return await fileClient.OpenReadAsync(null, cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw AdlsErrors.Translate(ex, filesystem, path);
        }
    }

    /// <inheritdoc />
    protected override async Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
    {
        var (filesystem, path) = GetFilesystemAndPath(uri, true);
        _ = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var blobServiceClient = await _clientFactory.GetBlobServiceClientAsync(uri, cancellationToken).ConfigureAwait(false);

        var contentType = !string.IsNullOrEmpty(options?.ContentType)
            ? options.ContentType
            : uri.Parameters.TryGetValue("contentType", out var ct) && !string.IsNullOrEmpty(ct)
                ? ct
                : null;

        return new PassThroughWriteStream(new AdlsGen2WriteStream(
            blobServiceClient,
            filesystem,
            path,
            contentType,
            _options.UploadThresholdBytes,
            _options.UploadMaximumConcurrency,
            _options.UploadMaximumTransferSizeBytes,
            cancellationToken));
    }

    /// <inheritdoc />
    protected override async Task<bool> ExistsCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (filesystem, path) = GetFilesystemAndPath(uri, true);
        path = path.TrimEnd('/'); // A directory URI ends with '/'; the service addresses it without it.
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
            throw AdlsErrors.Translate(ex, filesystem, path);
        }
    }

    /// <inheritdoc />
    protected override async Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (filesystem, path) = GetFilesystemAndPath(uri, true);
        path = path.TrimEnd('/'); // A directory URI ends with '/'; the service addresses it without it.
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
            throw AdlsErrors.Translate(ex, filesystem, path);
        }
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
            throw AdlsErrors.Translate(ex, destFilesystem, destPath);
        }

        try
        {
            await sourceBlob.DeleteIfExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw AdlsErrors.Translate(ex, sourceFilesystem, sourcePath);
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

    /// <inheritdoc />
    protected override async IAsyncEnumerable<StorageItem> ListCoreAsync(
        StorageUri directory,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (filesystem, pathPrefix) = GetFilesystemAndPath(directory);

        await foreach (var item in ListViaBlobFallbackAsync(directory, filesystem, pathPrefix, recursive, cancellationToken)
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
            await foreach (var blobItem in containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, blobPrefix, cancellationToken)
                               .ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (blobItem.Name.EndsWith('/'))
                    continue;

                yield return new StorageItem
                {
                    Uri = prefix.WithPath("/" + blobItem.Name),
                    Size = blobItem.Properties?.ContentLength,
                    LastModified = blobItem.Properties?.LastModified,
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
                    if (item.Blob.Name.EndsWith('/'))
                        continue;

                    yield return new StorageItem
                    {
                        Uri = prefix.WithPath("/" + item.Blob.Name),
                        Size = item.Blob.Properties?.ContentLength,
                        LastModified = item.Blob.Properties?.LastModified,
                        IsDirectory = false,
                    };
                }
                else if (item.IsPrefix)
                {
                    yield return new StorageItem
                    {
                        Uri = prefix.WithPath("/" + item.Prefix),
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
}
