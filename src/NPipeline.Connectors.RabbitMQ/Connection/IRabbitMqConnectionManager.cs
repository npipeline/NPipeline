using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Connection;

/// <summary>
///     Manages the RabbitMQ connection lifecycle and channel pooling.
///     Shared between source and sink nodes.
/// </summary>
public interface IRabbitMqConnectionManager : IAsyncDisposable
{
    /// <summary>
    ///     Gets whether the connection is currently open.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    ///     Gets or creates the shared connection.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The active <see cref="IConnection" />.</returns>
    Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Creates a new dedicated channel (not pooled). Used by source nodes that need
    ///     a long-lived consumer channel.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A new <see cref="IChannel" />.</returns>
    Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Borrows a channel from the pool. Used by sink nodes for publish operations.
    ///     The caller must return the channel via <see cref="ReturnChannel" />.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A pooled <see cref="IChannel" />.</returns>
    Task<IChannel> GetPooledChannelAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns a borrowed channel to the pool.
    /// </summary>
    /// <param name="channel">The channel to return.</param>
    void ReturnChannel(IChannel channel);
}
