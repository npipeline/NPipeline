namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     Configuration options for the SFTP storage provider.
/// </summary>
public class SftpStorageProviderOptions
{
    /// <summary>
    ///     Gets or sets the default host for SFTP connections.
    /// </summary>
    public string? DefaultHost { get; set; }

    /// <summary>
    ///     Gets or sets the default port for SFTP connections.
    ///     Default is 22.
    /// </summary>
    public int DefaultPort { get; set; } = 22;

    /// <summary>
    ///     Gets or sets the default username for authentication.
    /// </summary>
    public string? DefaultUsername { get; set; }

    /// <summary>
    ///     Gets or sets the default password for password authentication.
    /// </summary>
    public string? DefaultPassword { get; set; }

    /// <summary>
    ///     Gets or sets the path to the private key file for key-based authentication.
    /// </summary>
    public string? DefaultKeyPath { get; set; }

    /// <summary>
    ///     Gets or sets the passphrase for the private key.
    /// </summary>
    public string? DefaultKeyPassphrase { get; set; }

    /// <summary>
    ///     Gets or sets the maximum number of connections in the pool.
    ///     Default is 10.
    /// </summary>
    public int MaxPoolSize { get; set; } = 10;

    /// <summary>
    ///     Gets or sets the connection idle timeout before returning to pool.
    ///     Default is 5 minutes.
    /// </summary>
    public TimeSpan ConnectionIdleTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    ///     Gets or sets the keep-alive interval for pooled connections.
    ///     Default is 30 seconds.
    /// </summary>
    public TimeSpan KeepAliveInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Gets or sets the timeout for establishing a connection (TCP connect and SSH handshake).
    ///     Default is 30 seconds. A connect that exceeds it fails with <c>SshOperationTimeoutException</c>;
    ///     the caller's cancellation token also stops a connect in progress.
    /// </summary>
    public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Gets or sets the SHA-256 fingerprints of the host keys the server may present, in the form <c>ssh-keygen -lf</c>
    ///     and <c>ssh-keyscan</c> print (<c>SHA256:ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og</c>; the <c>SHA256:</c> prefix is
    ///     optional). A connection to a server whose key is not listed fails. Required unless <see cref="AcceptAnyHostKey" />
    ///     is set.
    /// </summary>
    public IReadOnlyCollection<string> HostKeyFingerprints { get; set; } = [];

    /// <summary>
    ///     Gets or sets a value indicating whether to trust any host key. This disables protection against
    ///     man-in-the-middle attacks; use it only for local development and tests. Defaults to <see langword="false" />.
    /// </summary>
    public bool AcceptAnyHostKey { get; set; }

    /// <summary>
    ///     Gets or sets whether to validate connection health (via <see cref="Renci.SshNet.SftpClient.IsConnected" />) before
    ///     returning a connection from the pool. Unhealthy connections are discarded and replaced.
    ///     Default is true.
    /// </summary>
    public bool ValidateOnAcquire { get; set; } = true;
}
