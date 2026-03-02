using System.Runtime.CompilerServices;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using Renci.SshNet.Common;
using Renci.SshNet.Sftp;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     Storage provider for SFTP that implements the IStorageProvider interface.
///     Handles sftp:// scheme URIs and supports reading, writing, listing, and metadata operations.
/// </summary>
/// <remarks>
///     - Async-first API design
///     - Stream-based I/O for scalability
///     - Connection pooling for high performance
///     - Keep-alive for reduced latency
///     - Proper error handling and exception translation
///     - Cancellation token support throughout
///     - Thread-safe implementation
/// </remarks>
public sealed class SftpStorageProvider : IStorageProvider, IStorageProviderMetadataProvider
{
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
    public StorageScheme Scheme => StorageScheme.Sftp;

    /// <inheritdoc />
    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return Scheme.Equals(uri.Scheme);
    }

    /// <inheritdoc />
    public async Task<Stream> OpenReadAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (host, port, path) = ParseUri(uri);

        try
        {
            var lease = await _clientFactory.AcquirePooledAsync(uri, cancellationToken).ConfigureAwait(false);

            try
            {
                return new SftpReadStream(lease, path);
            }
            catch
            {
                // If stream creation fails, return the lease
                await lease.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch (SftpPathNotFoundException ex)
        {
            throw TranslateSftpException(ex, host, path);
        }
        catch (SshException ex)
        {
            throw TranslateSftpException(ex, host, path);
        }
    }

    /// <inheritdoc />
    public async Task<Stream> OpenWriteAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (host, port, path) = ParseUri(uri);

        try
        {
            var lease = await _clientFactory.AcquirePooledAsync(uri, cancellationToken).ConfigureAwait(false);

            try
            {
                return new SftpWriteStream(lease, path, true);
            }
            catch
            {
                // If stream creation fails, return the lease
                await lease.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch (SshException ex)
        {
            throw TranslateSftpException(ex, host, path);
        }
    }

    /// <inheritdoc />
    public async Task<bool> ExistsAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (host, port, path) = ParseUri(uri);

        try
        {
            var lease = await _clientFactory.AcquirePooledAsync(uri, cancellationToken).ConfigureAwait(false);

            try
            {
                var exists = lease.Client.Exists(path);
                return exists;
            }
            finally
            {
                await lease.DisposeAsync().ConfigureAwait(false);
            }
        }
        catch (SftpPathNotFoundException)
        {
            return false;
        }
        catch (SshException ex)
        {
            throw TranslateSftpException(ex, host, path);
        }
    }

    /// <inheritdoc />
    public IAsyncEnumerable<StorageItem> ListAsync(
        StorageUri prefix,
        bool recursive = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return ListAsyncCore(prefix, recursive, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<StorageMetadata?> GetMetadataAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (host, port, path) = ParseUri(uri);

        try
        {
            var lease = await _clientFactory.AcquirePooledAsync(uri, cancellationToken).ConfigureAwait(false);

            try
            {
                var attributes = lease.Client.GetAttributes(path);

                if (attributes is null)
                    return null;

                var metadata = new StorageMetadata
                {
                    Size = attributes.Size,
                    LastModified = NormalizeDateTime(attributes.LastWriteTimeUtc),
                    ContentType = null, // SFTP doesn't provide content type
                    CustomMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                    IsDirectory = attributes.IsDirectory,
                    ETag = attributes.LastWriteTimeUtc.Ticks.ToString("x16"),
                };

                return metadata;
            }
            finally
            {
                await lease.DisposeAsync().ConfigureAwait(false);
            }
        }
        catch (SftpPathNotFoundException)
        {
            return null;
        }
        catch (SshException ex)
        {
            throw TranslateSftpException(ex, host, path);
        }
    }

    /// <inheritdoc />
    public StorageProviderMetadata GetMetadata()
    {
        return new StorageProviderMetadata
        {
            Name = "SFTP",
            SupportedSchemes = [StorageScheme.Sftp.ToString()],
            SupportsRead = true,
            SupportsWrite = true,
            SupportsListing = true,
            SupportsMetadata = true,
            SupportsHierarchy = true,
            Capabilities = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                ["maxPoolSize"] = _options.MaxPoolSize,
                ["connectionIdleTimeout"] = _options.ConnectionIdleTimeout,
                ["keepAliveInterval"] = _options.KeepAliveInterval,
                ["connectionTimeout"] = _options.ConnectionTimeout,
            },
        };
    }

    private static (string host, int port, string path) ParseUri(StorageUri uri)
    {
        var host = uri.Host;

        if (string.IsNullOrWhiteSpace(host))
            throw new ArgumentException("SFTP URI must specify a host.", nameof(uri));

        var port = uri.Port ?? 22;
        var path = uri.Path;

        // Ensure path starts with /
        if (!path.StartsWith('/'))
            path = "/" + path;

        return (host, port, path);
    }

    private async IAsyncEnumerable<StorageItem> ListAsyncCore(
        StorageUri prefix,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (host, port, path) = ParseUri(prefix);
        IPooledConnection? lease = null;

        try
        {
            lease = await _clientFactory.AcquirePooledAsync(prefix, cancellationToken).ConfigureAwait(false);

            // Use Task.Run to move blocking ListDirectory to thread pool
            IEnumerable<ISftpFile> entries;

            try
            {
                entries = await Task.Run(
                    () => lease.Client.ListDirectory(path),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (SftpPathNotFoundException)
            {
                // Directory doesn't exist, return empty
                yield break;
            }

            foreach (var entry in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Skip . and ..
                if (entry.Name == "." || entry.Name == "..")
                    continue;

                var itemPath = path.EndsWith('/')
                    ? $"{path}{entry.Name}"
                    : $"{path}/{entry.Name}";

                var itemUri = BuildItemUri(prefix, itemPath);

                yield return new StorageItem
                {
                    Uri = itemUri,
                    Size = entry.Attributes.Size,
                    LastModified = NormalizeDateTime(entry.Attributes.LastWriteTimeUtc),
                    IsDirectory = entry.Attributes.IsDirectory,
                };

                // Recursively list subdirectories if requested
                if (recursive && entry.Attributes.IsDirectory)
                {
                    var subPrefix = BuildItemUri(prefix, itemPath);

                    await foreach (var subItem in ListAsyncCore(subPrefix, recursive, cancellationToken))
                    {
                        yield return subItem;
                    }
                }
            }
        }
        finally
        {
            if (lease is not null)
                await lease.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static StorageUri BuildItemUri(StorageUri baseUri, string path)
    {
        var portPart = baseUri.Port.HasValue
            ? $":{baseUri.Port.Value}"
            : "";

        var userInfoPart = !string.IsNullOrEmpty(baseUri.UserInfo)
            ? $"{baseUri.UserInfo}@"
            : "";

        var queryString = baseUri.Parameters.Count > 0
            ? "?" + string.Join("&", baseUri.Parameters.Select(p => $"{Uri.EscapeDataString(p.Key)}={Uri.EscapeDataString(p.Value)}"))
            : "";

        return StorageUri.Parse($"sftp://{userInfoPart}{baseUri.Host}{portPart}{path}{queryString}");
    }

    private static Exception TranslateSftpException(Exception ex, string host, string path)
    {
        return ex switch
        {
            SftpPathNotFoundException =>
                new SftpStorageException(
                    $"SFTP file not found: '{path}' on server '{host}'",
                    host,
                    path,
                    SftpErrorCode.FileNotFound,
                    ex),

            SftpPermissionDeniedException =>
                new SftpStorageException(
                    $"Access denied to SFTP path '{path}' on server '{host}'. {ex.Message}",
                    host,
                    path,
                    SftpErrorCode.PermissionDenied,
                    ex),

            SshAuthenticationException =>
                new SftpStorageException(
                    $"Authentication failed for SFTP server '{host}'. {ex.Message}",
                    host,
                    path,
                    SftpErrorCode.AuthenticationFailed,
                    ex),

            SshConnectionException =>
                new SftpStorageException(
                    $"Failed to connect to SFTP server '{host}'. {ex.Message}",
                    host,
                    path,
                    SftpErrorCode.ConnectionFailed,
                    ex),

            OperationCanceledException =>
                ex,

            _ =>
                new SftpStorageException(
                    $"SFTP operation failed on server '{host}' for path '{path}'. {ex.Message}",
                    host,
                    path,
                    SftpErrorCode.Unknown,
                    ex),
        };
    }

    private static DateTimeOffset NormalizeDateTime(DateTime? value)
    {
        var actual = value ?? DateTime.UtcNow;

        var utc = actual.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(actual, DateTimeKind.Utc)
            : actual.ToUniversalTime();

        return new DateTimeOffset(utc);
    }
}
