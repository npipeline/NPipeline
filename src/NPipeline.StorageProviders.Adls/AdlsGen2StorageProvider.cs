using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Azure;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     Storage provider for Azure Data Lake Storage Gen2.
///     Handles "adls" scheme URIs and supports reading, writing, listing, moving, deleting, and metadata operations.
/// </summary>
/// <remarks>
///     Declares <see cref="StorageCapabilities.Hierarchy" />. <see cref="StorageCapabilities.AtomicMove" /> is not declared,
///     because whether a move is a single atomic rename depends on the account: an account with a hierarchical namespace
///     renames (and a rejected rename is an error, never a copy), while an account without one, such as the Azurite
///     emulator, moves by copy and delete. <see cref="DeleteCoreAsync" /> deletes files.
/// </remarks>
public sealed class AdlsGen2StorageProvider : StorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> SchemeList = [StorageScheme.Adls];

    private static readonly Regex FilesystemNameRegex = new(
        "^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        TimeSpan.FromMilliseconds(250));

    private readonly AdlsGen2ClientFactory _clientFactory;
    private readonly AzureContainerInitializer _containers;
    private readonly ConcurrentDictionary<AzureEndpointKey, Task<bool>> _hierarchicalNamespace = new();
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
        _containers = new AzureContainerInitializer(options);
    }

    /// <inheritdoc />
    public override string Name => "Azure Data Lake Storage Gen2";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => SchemeList;

    /// <inheritdoc />
    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete | StorageCapabilities.Move |
        StorageCapabilities.Hierarchy | StorageCapabilities.ConditionalWrite;

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

        // Both ends must be in one account. Accounts are compared by endpoint, not by client instance, which changes
        // when the client cache evicts.
        var endpoint = AzureClientBuilder.GetEndpoint(source, _options);

        if (endpoint != AzureClientBuilder.GetEndpoint(destination, _options))
        {
            throw new NotSupportedException(
                "Cross-account moves are not supported in ADLS Gen2 provider. " +
                "Source and destination must be in the same storage account.");
        }

        var blobServiceClient = await _clientFactory.GetBlobServiceClientAsync(source, cancellationToken).ConfigureAwait(false);

        if (!await HasHierarchicalNamespaceAsync(endpoint, blobServiceClient, cancellationToken).ConfigureAwait(false))
        {
            await MoveViaBlobCopyAsync(blobServiceClient, sourceFilesystem, sourcePath, destFilesystem, destPath, cancellationToken)
                .ConfigureAwait(false);

            return;
        }

        var sourceServiceClient = await _clientFactory.GetClientAsync(source, cancellationToken).ConfigureAwait(false);
        var sourcePathClient = sourceServiceClient.GetFileSystemClient(sourceFilesystem).GetFileClient(sourcePath);

        // The atomic rename of a hierarchical namespace. The destination path is relative to its filesystem, which is
        // passed separately: a null destinationFileSystem renames within the source filesystem. A rejected rename is an
        // error; falling back to copy-and-delete would break the atomic promise and could leave both objects behind.
        try
        {
            _ = await sourcePathClient.RenameAsync(
                destPath,
                sourceFilesystem == destFilesystem
                    ? null
                    : destFilesystem,
                cancellationToken: cancellationToken).ConfigureAwait(false);
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
        var blobServiceClient = await _clientFactory.GetBlobServiceClientAsync(uri, cancellationToken).ConfigureAwait(false);

        await _containers.EnsureAsync(AzureClientBuilder.GetEndpoint(uri, _options), blobServiceClient, filesystem, cancellationToken).ConfigureAwait(false);

        var contentType = !string.IsNullOrEmpty(options?.ContentType)
            ? options.ContentType
            : uri.Parameters.TryGetValue("contentType", out var ct) && !string.IsNullOrEmpty(ct)
                ? ct
                : null;

        // Uploads go through the Blob API, which works on every account and with the emulator, as block blobs.
        return new AzureBlobWriteStream(
            blobServiceClient,
            filesystem,
            path,
            contentType,
            _options.PartSizeBytes,
            _options.MaxConcurrency,
            options?.LengthHint,
            options?.IfMatch,
            options?.Overwrite ?? true,
            ex => AdlsErrors.Translate(ex, filesystem, path));
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

    private static async Task MoveViaBlobCopyAsync(
        BlobServiceClient blobServiceClient,
        string sourceFilesystem,
        string sourcePath,
        string destFilesystem,
        string destPath,
        CancellationToken cancellationToken)
    {
        var sourceBlob = blobServiceClient.GetBlobContainerClient(sourceFilesystem).GetBlobClient(sourcePath);
        var destBlob = blobServiceClient.GetBlobContainerClient(destFilesystem).GetBlobClient(destPath);

        try
        {
            var copyOp = await destBlob.StartCopyFromUriAsync(sourceBlob.Uri, cancellationToken: cancellationToken).ConfigureAwait(false);
            _ = await copyOp.WaitForCompletionAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw AdlsErrors.Translate(ex, sourceFilesystem, sourcePath);
        }

        try
        {
            _ = await sourceBlob.DeleteIfExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex)
        {
            throw AdlsErrors.Translate(ex, sourceFilesystem, sourcePath);
        }
    }

    // Whether an account has a hierarchical namespace is a property of the account, so it is asked once per endpoint. When
    // it cannot be determined (for example a token without permission for account information), assume it does: this is the
    // ADLS Gen2 provider, and a plain-blob account is the exception.
    private async Task<bool> HasHierarchicalNamespaceAsync(AzureEndpointKey endpoint, BlobServiceClient blobServiceClient, CancellationToken cancellationToken)
    {
        var detection = _hierarchicalNamespace.GetOrAdd(endpoint, static (_, client) => DetectHierarchicalNamespaceAsync(client), blobServiceClient);

        try
        {
            return await detection.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
    }

    private static async Task<bool> DetectHierarchicalNamespaceAsync(BlobServiceClient blobServiceClient)
    {
        try
        {
            var info = await blobServiceClient.GetAccountInfoAsync().ConfigureAwait(false);

            return info.Value.IsHierarchicalNamespaceEnabled;
        }
        catch (RequestFailedException)
        {
            return true;
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
        var blobServiceClient = await _clientFactory.GetBlobServiceClientAsync(directory, cancellationToken).ConfigureAwait(false);
        var endpoint = AzureClientBuilder.GetEndpoint(directory, _options);

        var items = await HasHierarchicalNamespaceAsync(endpoint, blobServiceClient, cancellationToken).ConfigureAwait(false)
            ? ListPathsAsync(directory, filesystem, pathPrefix, recursive, cancellationToken)
            : ListBlobsAsync(blobServiceClient, directory, filesystem, pathPrefix, recursive, cancellationToken);

        await foreach (var item in items.ConfigureAwait(false))
        {
            yield return item;
        }
    }

    // A hierarchical namespace has real directories, so list them with the Data Lake API: it reports directories as such,
    // with their timestamps, and includes empty ones.
    private async IAsyncEnumerable<StorageItem> ListPathsAsync(
        StorageUri directory,
        string filesystem,
        string pathPrefix,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var dataLakeServiceClient = await _clientFactory.GetClientAsync(directory, cancellationToken).ConfigureAwait(false);
        var fileSystemClient = dataLakeServiceClient.GetFileSystemClient(filesystem);

        // The service takes a directory name without a trailing slash; no name lists the root.
        var path = pathPrefix.TrimEnd('/');

        var paths = fileSystemClient.GetPathsAsync(string.IsNullOrEmpty(path) ? null : path, recursive, false, cancellationToken);

        await foreach (var item in AzureListing.GuardAsync(paths, ex => AdlsErrors.Translate(ex, filesystem, pathPrefix), cancellationToken).ConfigureAwait(false))
        {
            if (item.IsDirectory == true)
            {
                // A recursive listing yields files only.
                if (recursive)
                    continue;

                yield return new StorageItem
                {
                    Uri = directory.WithPath("/" + item.Name.TrimEnd('/') + "/"),
                    LastModified = item.LastModified,
                    IsDirectory = true,
                };

                continue;
            }

            yield return new StorageItem
            {
                Uri = directory.WithPath("/" + item.Name),
                Size = item.ContentLength,
                LastModified = item.LastModified,
                IsDirectory = false,
            };
        }
    }

    // Without a hierarchical namespace (a plain blob account, or the emulator) a filesystem is a flat container, and
    // directories are the prefixes of blob names.
    private static async IAsyncEnumerable<StorageItem> ListBlobsAsync(
        BlobServiceClient blobServiceClient,
        StorageUri prefix,
        string filesystem,
        string pathPrefix,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var containerClient = blobServiceClient.GetBlobContainerClient(filesystem);

        var blobPrefix = string.IsNullOrEmpty(pathPrefix)
            ? null
            : pathPrefix.EndsWith('/')
                ? pathPrefix
                : pathPrefix + "/";

        if (recursive)
        {
            var blobs = containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, blobPrefix, cancellationToken);

            await foreach (var blobItem in AzureListing.GuardAsync(blobs, ex => AdlsErrors.Translate(ex, filesystem, pathPrefix), cancellationToken).ConfigureAwait(false))
            {
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
            var blobs = containerClient.GetBlobsByHierarchyAsync(BlobTraits.None, BlobStates.None, "/", blobPrefix, cancellationToken);

            await foreach (var item in AzureListing.GuardAsync(blobs, ex => AdlsErrors.Translate(ex, filesystem, pathPrefix), cancellationToken).ConfigureAwait(false))
            {
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
