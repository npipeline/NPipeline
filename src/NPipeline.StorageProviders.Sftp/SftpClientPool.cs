using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using NPipeline.StorageProviders.Models;
using Renci.SshNet;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     Represents a pooled SFTP connection.
/// </summary>
internal interface IPooledConnection : IDisposable, IAsyncDisposable
{
    /// <summary>
    ///     Gets the SFTP client.
    /// </summary>
    SftpClient Client { get; }

    /// <summary>
    ///     Gets the pool key for this connection.
    /// </summary>
    string PoolKey { get; }

    /// <summary>
    ///     Gets the last used timestamp.
    /// </summary>
    DateTime LastUsed { get; }

    /// <summary>
    ///     Returns the connection to the pool instead of disposing it.
    /// </summary>
    void Return();
}

/// <summary>
///     Internal wrapper for pooled connections.
/// </summary>
internal sealed class PooledConnection : IPooledConnection
{
    private readonly SftpClientPool _pool;
    private readonly Action<PooledConnection> _returnAction;
    private int _returned;

    /// <summary>
    ///     Initializes a new instance of the <see cref="PooledConnection" /> class.
    /// </summary>
    public PooledConnection(
        SftpClient client,
        string poolKey,
        SftpClientPool pool,
        Action<PooledConnection> returnAction)
    {
        Client = client ?? throw new ArgumentNullException(nameof(client));
        PoolKey = poolKey;
        _pool = pool ?? throw new ArgumentNullException(nameof(pool));
        _returnAction = returnAction;
        LastUsed = DateTime.UtcNow;
    }

    /// <summary>
    ///     Gets or sets whether this connection has been marked invalid.
    /// </summary>
    public bool IsMarkedInvalid { get; set; }

    /// <inheritdoc />
    public SftpClient Client { get; }

    /// <inheritdoc />
    public string PoolKey { get; }

    /// <inheritdoc />
    public DateTime LastUsed { get; private set; }

    /// <inheritdoc />
    public void Return()
    {
        if (Interlocked.CompareExchange(ref _returned, 1, 0) == 0)
            _returnAction(this);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Return();
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Return();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    ///     Updates the last used timestamp.
    /// </summary>
    public void Touch()
    {
        LastUsed = DateTime.UtcNow;
    }
}

/// <summary>
///     Manages a pool of SFTP connections for high-throughput scenarios.
/// </summary>
/// <remarks>
///     <strong>Thread Safety:</strong> <see cref="SftpClient" /> is NOT thread-safe for concurrent operations.
///     Each active SFTP operation therefore requires its own dedicated connection from the pool. The pool
///     enforces this invariant: a connection held by a caller cannot be used by any other caller until it
///     is explicitly returned.
/// </remarks>
internal sealed class SftpClientPool : IDisposable, IAsyncDisposable
{
    private readonly ConcurrentDictionary<PooledConnection, bool> _allConnections = new();

    // Separate queue per pool key so connections are never handed to the wrong server.
    private readonly ConcurrentDictionary<string, ConcurrentQueue<PooledConnection>> _available = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _cleanupCts;
    private readonly Task _cleanupTask;
    private readonly Func<StorageUri, CancellationToken, Task<SftpClient>> _clientFactory;
    private readonly SftpStorageProviderOptions _options;
    private readonly SemaphoreSlim _semaphore;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SftpClientPool" /> class.
    /// </summary>
    public SftpClientPool(
        SftpStorageProviderOptions options,
        Func<StorageUri, CancellationToken, Task<SftpClient>> clientFactory)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _semaphore = new SemaphoreSlim(options.MaxPoolSize, options.MaxPoolSize);
        _cleanupCts = new CancellationTokenSource();
        _cleanupTask = CleanupLoopAsync(_cleanupCts.Token);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        _cleanupCts.Cancel();

        try
        {
            await _cleanupTask.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch
        {
            // Ignore cleanup task wait errors
        }

        _cleanupCts.Dispose();

        // Dispose all connections tracked across all pool keys.
        foreach (var connection in _allConnections.Keys)
        {
            DisposeConnection(connection);
        }

        _allConnections.Clear();
        _available.Clear();
        _semaphore.Dispose();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        _cleanupCts.Cancel();

        try
        {
            _cleanupTask.Wait(TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Ignore cleanup task wait errors
        }

        _cleanupCts.Dispose();

        // Dispose all connections tracked across all pool keys.
        foreach (var connection in _allConnections.Keys)
        {
            DisposeConnection(connection);
        }

        _allConnections.Clear();
        _available.Clear();
        _semaphore.Dispose();
    }

    /// <summary>
    ///     Acquires a connection from the pool or creates a new one.
    /// </summary>
    public async Task<IPooledConnection> AcquireAsync(
        StorageUri uri,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var poolKey = BuildPoolKey(uri);

        // Wait for a slot in the semaphore
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            // Try to get an available connection for this specific server/credential combination.
            var queue = _available.GetOrAdd(poolKey, static _ => new ConcurrentQueue<PooledConnection>());

            while (queue.TryDequeue(out var pooledConnection))
            {
                // Validate connection health if configured
                if (_options.ValidateOnAcquire && !IsConnectionHealthy(pooledConnection))
                {
                    DisposeConnection(pooledConnection);
                    continue;
                }

                pooledConnection.Touch();
                return pooledConnection;
            }

            // No available connection, create a new one
            var client = await _clientFactory(uri, cancellationToken).ConfigureAwait(false);
            var newConnection = new PooledConnection(client, poolKey, this, InternalReturn);
            _allConnections.TryAdd(newConnection, true);
            return newConnection;
        }
        catch
        {
            _semaphore.Release();
            throw;
        }
    }

    /// <summary>
    ///     Returns a connection to the pool.
    /// </summary>
    public void Return(PooledConnection connection)
    {
        if (connection is null)
            return;

        ObjectDisposedException.ThrowIf(_disposed, this);

        // Check if connection is still healthy
        if (!IsConnectionHealthy(connection))
        {
            DisposeConnection(connection);
            _semaphore.Release();
            return;
        }

        connection.Touch();

        // Return to the queue that matches this connection's pool key.
        var queue = _available.GetOrAdd(connection.PoolKey, static _ => new ConcurrentQueue<PooledConnection>());
        queue.Enqueue(connection);
        _semaphore.Release();
    }

    private void InternalReturn(PooledConnection connection)
    {
        Return(connection);
    }

    private static bool IsConnectionHealthy(PooledConnection connection)
    {
        if (connection.IsMarkedInvalid)
            return false;

        try
        {
            return connection.Client.IsConnected;
        }
        catch
        {
            return false;
        }
    }

    private void DisposeConnection(PooledConnection connection)
    {
        // Mark invalid first so concurrent health checks see it as dead immediately.
        connection.IsMarkedInvalid = true;

        _allConnections.TryRemove(connection, out _);

        try
        {
            if (connection.Client.IsConnected)
                connection.Client.Disconnect();
        }
        catch
        {
            // Ignore disconnection errors
        }

        try
        {
            connection.Client.Dispose();
        }
        catch
        {
            // Ignore disposal errors
        }
    }

    private async Task CleanupLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false);

                var now = DateTime.UtcNow;

                // Process each per-key queue independently.
                foreach (var (_, queue) in _available)
                {
                    var snapshot = new List<PooledConnection>();

                    // Drain the queue for this pool key.
                    while (queue.TryDequeue(out var pc))
                    {
                        snapshot.Add(pc);
                    }

                    foreach (var pc in snapshot)
                    {
                        var isStale = now - pc.LastUsed > _options.ConnectionIdleTimeout;
                        var isDead = !IsConnectionHealthy(pc);

                        if (isStale || isDead)
                        {
                            DisposeConnection(pc);

                            // Note: No semaphore release here — the semaphore was already released
                            // when the connection was returned to the available pool via Return().
                        }
                        else
                            queue.Enqueue(pc);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                // Ignore cleanup errors and continue
            }
        }
    }

    private static string BuildPoolKey(StorageUri uri)
    {
        var host = uri.Host ?? "";
        var port = uri.Port ?? 22;

        // Extract username from UserInfo or parameters
        var username = uri.UserInfo?.Split(':')[0];

        if (string.IsNullOrEmpty(username) && uri.Parameters.TryGetValue("username", out var userParam))
            username = userParam;

        // Build credential hash
        string authHash;

        if (uri.Parameters.TryGetValue("password", out var password) && !string.IsNullOrEmpty(password))
            authHash = ComputeHash($"password:{password}");
        else if (uri.Parameters.TryGetValue("keyPath", out var keyPath) && !string.IsNullOrEmpty(keyPath))
            authHash = ComputeHash($"key:{keyPath}");
        else
            authHash = "default";

        return $"{host}:{port}:{username ?? "anonymous"}:{authHash}";
    }

    private static string ComputeHash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(bytes).AsSpan(0, 16).ToString();
    }
}
