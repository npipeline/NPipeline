using System.Runtime.CompilerServices;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using Renci.SshNet;
using Renci.SshNet.Common;
using Renci.SshNet.Sftp;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     Storage provider for SFTP. Handles sftp:// scheme URIs and supports reading, writing, listing, deleting,
///     moving and metadata operations.
/// </summary>
/// <remarks>
///     <para>
///         Connections are pooled, and every network call uses the SSH.NET async API. Failures follow the storage provider
///         contract: a missing path is a <see cref="FileNotFoundException" />, a permission or authentication failure is an
///         <see cref="UnauthorizedAccessException" />, and any other SSH or socket failure is an <see cref="IOException" />
///         with the SSH.NET exception as its inner exception.
///     </para>
/// </remarks>
public sealed class SftpStorageProvider : StorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> SupportedSchemes = [StorageScheme.Sftp];

    private readonly SftpClientFactory _clientFactory;
    private readonly SftpStorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SftpStorageProvider" /> class.
    /// </summary>
    /// <param name="clientFactory">The SFTP client factory.</param>
    /// <param name="options">The SFTP storage provider options.</param>
    public SftpStorageProvider(
        SftpClientFactory clientFactory,
        SftpStorageProviderOptions options)
    {
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public override string Name => "SFTP";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => SupportedSchemes;

    /// <inheritdoc />
    /// <remarks>
    ///     A move is an SFTP rename, which is atomic on POSIX servers. When the destination already exists it is removed first,
    ///     because a plain rename does not overwrite, so only a move onto a new path is a single atomic step.
    /// </remarks>
    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete
        | StorageCapabilities.Move | StorageCapabilities.AtomicMove | StorageCapabilities.Hierarchy;

    /// <inheritdoc />
    protected override async Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (host, path) = ParseUri(uri);
        var lease = await AcquireAsync(uri, host, path, cancellationToken).ConfigureAwait(false);

        try
        {
            return await SftpReadStream.OpenAsync(lease, path, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // If stream creation fails, return the lease
            await lease.DisposeAsync().ConfigureAwait(false);

            if (SftpErrors.IsTranslatable(ex))
                throw SftpErrors.Translate(ex, host, path);

            throw;
        }
    }

    /// <inheritdoc />
    protected override async Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
    {
        var (host, path) = ParseUri(uri);
        var lease = await AcquireAsync(uri, host, path, cancellationToken).ConfigureAwait(false);

        try
        {
            var stream = await SftpWriteStream.OpenAsync(lease, path, true, cancellationToken).ConfigureAwait(false);

            return new PassThroughWriteStream(stream);
        }
        catch (Exception ex)
        {
            await lease.DisposeAsync().ConfigureAwait(false);

            if (SftpErrors.IsTranslatable(ex))
                throw SftpErrors.Translate(ex, host, path);

            throw;
        }
    }

    /// <inheritdoc />
    protected override Task<bool> ExistsCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (host, path) = ParseUri(uri);

        return WithClientAsync(
            uri,
            host,
            path,
            static (client, p, token) => client.ExistsAsync(p, token),
            cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (host, path) = ParseUri(uri);

        try
        {
            return await WithClientAsync<StorageMetadata?>(
                uri,
                host,
                path,
                static async (client, p, token) =>
                {
                    var attributes = await client.GetAttributesAsync(p, token).ConfigureAwait(false);

                    if (attributes is null)
                        return null;

                    var lastModified = ToTimestamp(attributes.LastWriteTimeUtc);

                    return new StorageMetadata
                    {
                        Size = attributes.IsDirectory ? 0 : attributes.Size,
                        LastModified = lastModified,
                        ContentType = null, // SFTP doesn't provide content type
                        IsDirectory = attributes.IsDirectory,
                        ETag = lastModified?.UtcTicks.ToString("x16"),
                    };
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (FileNotFoundException ex) when (ex.InnerException is SftpPathNotFoundException)
        {
            return null;
        }
    }

    /// <inheritdoc />
    protected override IAsyncEnumerable<StorageItem> ListCoreAsync(StorageUri directory, bool recursive, CancellationToken cancellationToken) =>
        ListAsyncCore(directory, recursive, cancellationToken);

    /// <inheritdoc />
    protected override async Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (host, path) = ParseUri(uri);

        try
        {
            await WithClientAsync(
                uri,
                host,
                path,
                static async (client, p, token) =>
                {
                    await client.DeleteFileAsync(p, token).ConfigureAwait(false);
                    return true;
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (FileNotFoundException ex) when (ex.InnerException is SftpPathNotFoundException)
        {
            // Deleting a missing file succeeds.
        }
    }

    /// <inheritdoc />
    protected override async Task MoveCoreAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken)
    {
        var (host, from) = ParseUri(source);
        var (_, to) = ParseUri(destination);

        if (!SameServer(source, destination))
            throw new ArgumentException("An SFTP move needs a source and destination on the same server and user.", nameof(destination));

        _ = await WithClientAsync(
            source,
            host,
            from,
            async (client, p, token) =>
            {
                // Fails with SftpPathNotFoundException (FileNotFoundException) when the source is missing.
                _ = await client.GetAttributesAsync(p, token).ConfigureAwait(false);

                if (string.Equals(p, to, StringComparison.Ordinal))
                    return true;

                await SftpWriteStream.EnsureParentDirectoryExistsAsync(client, to, token).ConfigureAwait(false);

                // A rename does not overwrite, so the destination goes first.
                try
                {
                    await client.DeleteFileAsync(to, token).ConfigureAwait(false);
                }
                catch (SftpPathNotFoundException)
                {
                    // Nothing to overwrite.
                }

                await client.RenameFileAsync(p, to, token).ConfigureAwait(false);

                return true;
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static (string host, string path) ParseUri(StorageUri uri)
    {
        var host = uri.Host;

        if (string.IsNullOrWhiteSpace(host))
            throw new ArgumentException("SFTP URI must specify a host.", nameof(uri));

        var path = uri.Path;

        // Ensure path starts with /
        if (!path.StartsWith('/'))
            path = "/" + path;

        return (host, path);
    }

    private static bool SameServer(StorageUri a, StorageUri b) =>
        string.Equals(a.Host, b.Host, StringComparison.OrdinalIgnoreCase)
        && (a.Port ?? 22) == (b.Port ?? 22)
        && string.Equals(a.UserName, b.UserName, StringComparison.Ordinal);

    /// <summary>Acquires a pooled connection, translating connection failures.</summary>
    private async Task<IPooledConnection> AcquireAsync(StorageUri uri, string host, string path, CancellationToken cancellationToken)
    {
        try
        {
            return await _clientFactory.AcquirePooledAsync(uri, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (SftpErrors.IsTranslatable(ex))
        {
            throw SftpErrors.Translate(ex, host, path);
        }
    }

    /// <summary>Runs <paramref name="operation" /> on a pooled connection that is returned afterwards, translating SSH failures.</summary>
    private async Task<T> WithClientAsync<T>(
        StorageUri uri,
        string host,
        string path,
        Func<SftpClient, string, CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        var lease = await AcquireAsync(uri, host, path, cancellationToken).ConfigureAwait(false);

        try
        {
            return await operation(lease.Client, path, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (SftpErrors.IsTranslatable(ex))
        {
            throw SftpErrors.Translate(ex, host, path);
        }
        finally
        {
            await lease.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async IAsyncEnumerable<StorageItem> ListAsyncCore(
        StorageUri directory,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (host, directoryPath) = ParseUri(directory);

        // Directory entries below are built as "<path>/<name>", so the root is held without its trailing slash.
        var rootPath = directoryPath.Length > 1 ? directoryPath.TrimEnd('/') : directoryPath;

        // One connection for the whole walk. Acquiring another per subdirectory while holding the parent's would
        // deadlock once the tree is as deep as the pool is large.
        var lease = await AcquireAsync(directory, host, rootPath, cancellationToken).ConfigureAwait(false);

        try
        {
            var pending = new Stack<string>();
            var visited = new HashSet<string>(StringComparer.Ordinal) { rootPath };
            pending.Push(rootPath);

            while (pending.TryPop(out var path))
            {
                var entries = await ListDirectoryAsync(lease.Client, host, path, cancellationToken).ConfigureAwait(false);

                foreach (var entry in entries)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (entry.Name is "." or "..")
                        continue;

                    var itemPath = path.EndsWith('/')
                        ? $"{path}{entry.Name}"
                        : $"{path}/{entry.Name}";

                    if (entry.Attributes.IsDirectory)
                    {
                        // Directory entries carry lstat attributes, so symbolic links are never followed; the visited set
                        // guards against servers that report them as directories anyway.
                        if (recursive)
                        {
                            if (visited.Add(itemPath))
                                pending.Push(itemPath);

                            continue;
                        }

                        yield return new StorageItem
                        {
                            Uri = directory.WithPath(itemPath + "/"),
                            IsDirectory = true,
                        };

                        continue;
                    }

                    yield return new StorageItem
                    {
                        Uri = directory.WithPath(itemPath),
                        Size = entry.Attributes.Size,
                        LastModified = ToTimestamp(entry.Attributes.LastWriteTimeUtc),
                        IsDirectory = false,
                    };
                }
            }
        }
        finally
        {
            await lease.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Lists one directory; a directory that does not exist (or was removed during the walk) is empty.</summary>
    private static async Task<List<ISftpFile>> ListDirectoryAsync(SftpClient client, string host, string path, CancellationToken cancellationToken)
    {
        var entries = new List<ISftpFile>();

        try
        {
            await foreach (var entry in client.ListDirectoryAsync(path, cancellationToken).ConfigureAwait(false))
            {
                entries.Add(entry);
            }
        }
        catch (SftpPathNotFoundException)
        {
            entries.Clear();
        }
        catch (Exception ex) when (SftpErrors.IsTranslatable(ex))
        {
            throw SftpErrors.Translate(ex, host, path);
        }

        return entries;
    }

    /// <summary>Converts a server timestamp, treating the Unix epoch that SSH.NET reports for a missing one as unknown.</summary>
    private static DateTimeOffset? ToTimestamp(DateTime value)
    {
        var utc = value.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(value, DateTimeKind.Utc)
            : value.ToUniversalTime();

        return utc <= DateTime.UnixEpoch ? null : new DateTimeOffset(utc);
    }
}
