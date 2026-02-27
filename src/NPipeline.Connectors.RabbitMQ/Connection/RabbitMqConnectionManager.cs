using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.RabbitMQ.Configuration;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Connection;

/// <summary>
///     Default <see cref="IRabbitMqConnectionManager" /> implementation with lazy connection creation
///     and bounded channel pooling.
/// </summary>
public sealed class RabbitMqConnectionManager : IRabbitMqConnectionManager
{
    private readonly Channel<IChannel> _channelPool;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly ILogger<RabbitMqConnectionManager> _logger;
    private readonly RabbitMqConnectionOptions _options;
    private IConnection? _connection;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of <see cref="RabbitMqConnectionManager" />.
    /// </summary>
    /// <param name="options">Connection options.</param>
    /// <param name="logger">Logger instance.</param>
    public RabbitMqConnectionManager(RabbitMqConnectionOptions options, ILogger<RabbitMqConnectionManager> logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options.Validate();

        _channelPool = Channel.CreateBounded<IChannel>(new BoundedChannelOptions(_options.MaxChannelPoolSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
        });
    }

    /// <inheritdoc />
    public bool IsConnected => _connection is { IsOpen: true };

    /// <inheritdoc />
    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_connection is { IsOpen: true })
            return _connection;

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_connection is { IsOpen: true })
                return _connection;

            var factory = BuildConnectionFactory();
            _connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);

            LogMessages.ConnectionEstablished(_logger, _options.HostName, _options.Port, _options.VirtualHost);

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    /// <inheritdoc />
    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        LogMessages.ChannelCreated(_logger);

        return channel;
    }

    /// <inheritdoc />
    public async Task<IChannel> GetPooledChannelAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Try to get a channel from the pool
        while (_channelPool.Reader.TryRead(out var pooledChannel))
        {
            if (pooledChannel.IsOpen)
                return pooledChannel;

            // Channel is closed/faulted — discard it
            try
            {
                await pooledChannel.CloseAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Ignore close errors on faulted channels
            }

            LogMessages.ChannelClosed(_logger);
        }

        // Pool exhausted — create a new channel with publisher confirms enabled
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);

        var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(true, true),
            cancellationToken).ConfigureAwait(false);

        LogMessages.ChannelCreated(_logger);

        return channel;
    }

    /// <inheritdoc />
    public void ReturnChannel(IChannel channel)
    {
        if (_disposed || !channel.IsOpen)
        {
            // Don't return closed channels to the pool
            try
            {
                channel.Dispose();
            }
            catch
            {
                // Ignore dispose errors
            }

            return;
        }

        if (!_channelPool.Writer.TryWrite(channel))
        {
            // Pool is full — dispose the excess channel
            try
            {
                channel.Dispose();
            }
            catch
            {
                // Ignore dispose errors
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Drain and close all pooled channels
        _channelPool.Writer.TryComplete();

        await foreach (var channel in _channelPool.Reader.ReadAllAsync())
        {
            try
            {
                await channel.CloseAsync().ConfigureAwait(false);
                channel.Dispose();
            }
            catch
            {
                // Best-effort cleanup
            }
        }

        // Close and dispose the connection
        if (_connection is not null)
        {
            try
            {
                await _connection.CloseAsync().ConfigureAwait(false);
                _connection.Dispose();
            }
            catch
            {
                // Best-effort cleanup
            }
        }

        _connectionLock.Dispose();
    }

    private ConnectionFactory BuildConnectionFactory()
    {
        var factory = new ConnectionFactory();

        if (_options.Uri is not null)
            factory.Uri = _options.Uri;
        else
        {
            factory.HostName = _options.HostName;
            factory.Port = _options.Port;
            factory.VirtualHost = _options.VirtualHost;
            factory.UserName = _options.UserName;
            factory.Password = _options.Password;
        }

        factory.RequestedHeartbeat = _options.RequestedHeartbeat;
        factory.NetworkRecoveryInterval = _options.NetworkRecoveryInterval;
        factory.AutomaticRecoveryEnabled = _options.AutomaticRecoveryEnabled;
        factory.TopologyRecoveryEnabled = _options.TopologyRecoveryEnabled;

        if (_options.ClientProvidedName is not null)
            factory.ClientProvidedName = _options.ClientProvidedName;

        if (_options.Tls is { Enabled: true })
        {
            factory.Ssl = new SslOption
            {
                Enabled = true,
                ServerName = _options.Tls.ServerName ?? _options.HostName,
                CertPath = _options.Tls.CertificatePath ?? "",
                CertPassphrase = _options.Tls.CertificatePassphrase,
                Version = _options.Tls.SslProtocols,
            };
        }

        return factory;
    }
}
