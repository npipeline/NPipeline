using NPipeline.StorageProviders.Models;
using Renci.SshNet;
using Renci.SshNet.Common;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     Factory for creating and configuring SFTP clients with authentication.
/// </summary>
public class SftpClientFactory : IDisposable, IAsyncDisposable
{
    private readonly SftpStorageProviderOptions _options;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SftpClientFactory" /> class.
    /// </summary>
    /// <param name="options">The SFTP storage provider options.</param>
    public SftpClientFactory(SftpStorageProviderOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        Pool = new SftpClientPool(options, CreateClientAsync);
    }

    /// <summary>
    ///     Gets the connection pool.
    /// </summary>
    internal SftpClientPool Pool { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await Pool.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        Pool.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Creates a new SFTP client configured for the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI containing connection details.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a connected <see cref="SftpClient" />.</returns>
    public async Task<SftpClient> CreateClientAsync(
        StorageUri uri,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ObjectDisposedException.ThrowIf(_disposed, this);

        var connectionInfo = BuildConnectionInfo(uri);

        var client = new SftpClient(connectionInfo)
        {
            KeepAliveInterval = _options.KeepAliveInterval,
        };

        // Configure server fingerprint validation on the client
        if (_options.ValidateServerFingerprint)
        {
            client.HostKeyReceived += (sender, e) =>
            {
                // If no expected fingerprint is configured, accept on first connection (TOFU)
                if (string.IsNullOrWhiteSpace(_options.ExpectedFingerprint))
                    return;

                // Compare with expected fingerprint using SHA256 (case-insensitive)
                if (string.Equals(e.FingerPrintSHA256, _options.ExpectedFingerprint, StringComparison.OrdinalIgnoreCase))
                    return;

                // Fingerprint mismatch - reject the connection
                e.CanTrust = false;
            };
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Connect with timeout
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_options.ConnectionTimeout);

            await Task.Run(client.Connect, cts.Token).ConfigureAwait(false);

            return client;
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    /// <summary>
    ///     Acquires a pooled connection for the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI containing connection details.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a pooled connection.</returns>
    internal Task<IPooledConnection> AcquirePooledAsync(
        StorageUri uri,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Pool.AcquireAsync(uri, cancellationToken);
    }

    /// <summary>
    ///     Extracts connection info from the URI.
    /// </summary>
    private ConnectionInfo BuildConnectionInfo(StorageUri uri)
    {
        var host = GetHost(uri);
        var port = GetPort(uri);
        var username = GetUsername(uri);
        var authMethods = BuildAuthenticationMethods(uri);

        var connectionInfo = new ConnectionInfo(host, port, username, authMethods)
        {
            Timeout = _options.ConnectionTimeout,
        };

        return connectionInfo;
    }

    /// <summary>
    ///     Gets the host from the URI or options.
    /// </summary>
    private string GetHost(StorageUri uri)
    {
        var host = uri.Host;

        if (string.IsNullOrWhiteSpace(host))
            host = _options.DefaultHost;

        if (string.IsNullOrWhiteSpace(host))
        {
            throw new ArgumentException(
                "SFTP URI must specify a host, or DefaultHost must be configured in options.",
                nameof(uri));
        }

        return host;
    }

    /// <summary>
    ///     Gets the port from the URI or options.
    /// </summary>
    private int GetPort(StorageUri uri)
    {
        if (uri.Port.HasValue && uri.Port.Value > 0)
            return uri.Port.Value;

        return _options.DefaultPort;
    }

    /// <summary>
    ///     Gets the username from the URI or options.
    /// </summary>
    private string GetUsername(StorageUri uri)
    {
        // First try UserInfo (e.g., user:pass@host)
        if (!string.IsNullOrWhiteSpace(uri.UserInfo))
        {
            var userInfo = uri.UserInfo;
            var colonIndex = userInfo.IndexOf(':');

            var username = colonIndex >= 0
                ? userInfo[..colonIndex]
                : userInfo;

            if (!string.IsNullOrWhiteSpace(username))
                return username;
        }

        // Then try username parameter
        if (uri.Parameters.TryGetValue("username", out var usernameParam) &&
            !string.IsNullOrWhiteSpace(usernameParam))
            return usernameParam;

        // Fall back to default
        if (!string.IsNullOrWhiteSpace(_options.DefaultUsername))
            return _options.DefaultUsername;

        throw new ArgumentException(
            "SFTP URI must specify a username, or DefaultUsername must be configured in options.",
            nameof(uri));
    }

    /// <summary>
    ///     Builds authentication methods based on configuration.
    /// </summary>
    private AuthenticationMethod[] BuildAuthenticationMethods(StorageUri uri)
    {
        var methods = new List<AuthenticationMethod>();

        // Check for explicit password in URI
        if (uri.Parameters.TryGetValue("password", out var passwordParam) &&
            !string.IsNullOrWhiteSpace(passwordParam))
        {
            var username = GetUsername(uri);
            methods.Add(new PasswordAuthenticationMethod(username, passwordParam));
        }

        // Check for explicit key path in URI
        else if (uri.Parameters.TryGetValue("keyPath", out var keyPathParam) &&
                 !string.IsNullOrWhiteSpace(keyPathParam))
        {
            var keyMethod = BuildKeyAuthenticationMethod(uri, keyPathParam);

            if (keyMethod is not null)
                methods.Add(keyMethod);
        }

        // Check for password in UserInfo
        else if (!string.IsNullOrWhiteSpace(uri.UserInfo))
        {
            var colonIndex = uri.UserInfo.IndexOf(':');

            if (colonIndex >= 0 && colonIndex < uri.UserInfo.Length - 1)
            {
                var password = uri.UserInfo[(colonIndex + 1)..];

                if (!string.IsNullOrWhiteSpace(password))
                {
                    var username = GetUsername(uri);
                    methods.Add(new PasswordAuthenticationMethod(username, password));
                }
            }
        }

        // Fall back to defaults
        else
        {
            // Try default password first
            if (!string.IsNullOrWhiteSpace(_options.DefaultPassword))
            {
                var username = GetUsername(uri);
                methods.Add(new PasswordAuthenticationMethod(username, _options.DefaultPassword));
            }

            // Try default key
            if (!string.IsNullOrWhiteSpace(_options.DefaultKeyPath))
            {
                var keyMethod = BuildKeyAuthenticationMethod(uri, _options.DefaultKeyPath);

                if (keyMethod is not null)
                    methods.Add(keyMethod);
            }
        }

        if (methods.Count == 0)
        {
            throw new ArgumentException(
                "SFTP authentication requires either a password or private key. " +
                "Provide credentials via URI parameters or configure defaults in options.",
                nameof(uri));
        }

        return methods.ToArray();
    }

    /// <summary>
    ///     Builds a key-based authentication method.
    /// </summary>
    private AuthenticationMethod? BuildKeyAuthenticationMethod(StorageUri uri, string keyPath)
    {
        try
        {
            var username = GetUsername(uri);
            var passphrase = GetKeyPassphrase(uri);

            if (!File.Exists(keyPath))
            {
                throw new ArgumentException(
                    $"Private key file not found: {keyPath}",
                    nameof(uri));
            }

            var keyFile = string.IsNullOrWhiteSpace(passphrase)
                ? new PrivateKeyFile(keyPath)
                : new PrivateKeyFile(keyPath, passphrase);

            return new PrivateKeyAuthenticationMethod(username, keyFile);
        }
        catch (SshException ex)
        {
            throw new ArgumentException(
                $"Failed to load private key from '{keyPath}': {ex.Message}",
                nameof(uri),
                ex);
        }
    }

    /// <summary>
    ///     Gets the key passphrase from the URI or options.
    /// </summary>
    private string? GetKeyPassphrase(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("keyPassphrase", out var passphraseParam))
            return passphraseParam;

        return _options.DefaultKeyPassphrase;
    }
}
