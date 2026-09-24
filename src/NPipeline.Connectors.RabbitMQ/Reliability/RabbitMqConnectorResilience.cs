using NResilience;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace NPipeline.Connectors.RabbitMQ.Reliability;

/// <summary>
///     Resilience presets for <see cref="Nodes.RabbitMqSinkNode{T}" />. Assign one to
///     <see cref="Configuration.RabbitMqSinkOptions.Resilience" />, or derive your own with a <c>with</c> expression.
/// </summary>
/// <remarks>
///     <para>
///         The sink owns the publish protocol, so NResilience is the only layer that retries a publish. The client's
///         automatic connection recovery still reconnects in the background; a retry that finds its channel closed
///         takes a fresh one from the pool.
///     </para>
///     <para>
///         Only the publish is retried. The source message is acknowledged after the publish succeeds, outside the
///         retried call, so a failed acknowledgement never publishes the message again.
///     </para>
/// </remarks>
public static class RabbitMqConnectorResilience
{
    /// <summary>
    ///     <see cref="NResilience.Classifier.Default" /> (timeouts and I/O errors are transient), plus the RabbitMQ
    ///     client's exceptions:
    ///     <list type="bullet">
    ///         <item>
    ///             <see cref="BrokerUnreachableException" /> and <see cref="ConnectFailureException" /> are transient,
    ///             unless the broker refused the credentials or the protocol version.
    ///         </item>
    ///         <item>
    ///             <see cref="OperationInterruptedException" />, including <see cref="AlreadyClosedException" />, is
    ///             judged by the reply code that closed the channel or connection: access refused (403), not found
    ///             (404), resource locked (405), precondition failed (406), invalid path (402), content too large (311),
    ///             no route (312), not allowed (530), not implemented (540), and the client-error codes 501 to 503 are
    ///             permanent. A close the application asked for is permanent. Anything else, such as a forced close
    ///             (320), a broker internal error (541), or a lost connection, is transient.
    ///         </item>
    ///         <item>
    ///             A <see cref="PublishException" /> is transient when the broker nacked the message, and permanent when
    ///             it returned the message as unroutable (<see cref="PublishReturnException" />, only with
    ///             <c>Mandatory</c>).
    ///         </item>
    ///         <item>
    ///             Authentication failures, protocol violations, and every other exception are permanent.
    ///         </item>
    ///     </list>
    /// </summary>
    public static Classifier Classifier { get; } = Classifier.Default
        .On<ChannelAllocationException>(Verdict.Transient)
        .On<ConnectFailureException>(static e => IsRefusedConnection(e)
            ? Verdict.Permanent
            : Verdict.Transient)
        .On<BrokerUnreachableException>(static e => IsRefusedConnection(e)
            ? Verdict.Permanent
            : Verdict.Transient)
        .On<PossibleAuthenticationFailureException>(Verdict.Permanent)
        .On<PublishException>(static e => e.IsReturn
            ? Verdict.Permanent
            : Verdict.Transient)
        .On<OperationInterruptedException>(static e => ClassifyShutdown(e.ShutdownReason));

    /// <summary>
    ///     Four attempts (three retries) with exponential backoff and full jitter from 100 milliseconds up to
    ///     30 seconds. There is no attempt timeout or overall deadline, so the attempt count bounds the call.
    /// </summary>
    /// <remarks>
    ///     Replaces <c>MaxRetries = 3</c> and <c>RetryBaseDelayMs = 100</c>, which made four calls with delays of 100,
    ///     200, and 400 milliseconds, retried every exception, and had no jitter or cap. A publish that waits on a
    ///     publisher confirm has no timeout, as before. To bound it, set <see cref="NResilience.Resilience.AttemptTimeout" />;
    ///     a publish that times out may still have reached the broker, so its retry can duplicate the message.
    /// </remarks>
    public static Resilience Default { get; } = new()
    {
        Name = "npipeline.rabbitmq.publish",
        Attempts = 4,
        AttemptTimeout = Timeout.InfiniteTimeSpan,
        Deadline = Timeout.InfiniteTimeSpan,
        Backoff = Backoff.Default with
        {
            TransientBase = TimeSpan.FromMilliseconds(100),
            MaximumDelay = TimeSpan.FromSeconds(30),
        },

        // Declared above so it is initialized first; a static initializer reads fields in declaration order.
        Classifier = Classifier,
        Adaptive = false,
    };

    private static bool IsRefusedConnection(Exception exception)
    {
        for (var inner = exception.InnerException; inner is not null; inner = inner.InnerException)
        {
            if (inner is PossibleAuthenticationFailureException or ProtocolVersionMismatchException)
                return true;
        }

        return false;
    }

    private static Verdict ClassifyShutdown(ShutdownEventArgs? reason)
    {
        if (reason is null)
            return Verdict.Transient;

        // The application closed the channel or connection itself (for example, on shutdown); retrying fights it.
        if (reason.Initiator == ShutdownInitiator.Application)
            return Verdict.Permanent;

        return reason.ReplyCode switch
        {
            Constants.ContentTooLarge
                or Constants.NoRoute
                or Constants.InvalidPath
                or Constants.AccessRefused
                or Constants.NotFound
                or Constants.ResourceLocked
                or Constants.PreconditionFailed
                or Constants.FrameError
                or Constants.SyntaxError
                or Constants.CommandInvalid
                or Constants.NotAllowed
                or Constants.NotImplemented => Verdict.Permanent,
            _ => Verdict.Transient,
        };
    }
}
