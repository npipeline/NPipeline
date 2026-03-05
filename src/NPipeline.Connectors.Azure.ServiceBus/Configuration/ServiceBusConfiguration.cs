using System.Text.Json;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.Configuration;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.Azure.ServiceBus.Configuration;

/// <summary>
///     Configuration for Azure Service Bus connectors (source and sink nodes).
/// </summary>
/// <remarks>
///     <para>
///         Supports connection string authentication, Azure AD credential, and named connections
///         managed via <see cref="AzureConnectionOptions" />.
///     </para>
///     <para>
///         Call <see cref="ValidateSource" /> before using as a source node and
///         <see cref="ValidateSink" /> before using as a sink node.
///     </para>
/// </remarks>
public class ServiceBusConfiguration
{
    // ── Connection ──────────────────────────────────────────────────────────────

    /// <summary>
    ///     Gets or sets the Service Bus connection string.
    ///     Required when <see cref="AuthenticationMode" /> is <see cref="AzureAuthenticationMode.ConnectionString" />
    ///     and <see cref="NamedConnection" /> is not set.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    ///     Gets or sets the fully qualified Service Bus namespace, e.g.
    ///     <c>my-namespace.servicebus.windows.net</c>.
    ///     Required when using Azure AD authentication and <see cref="NamedConnection" /> is not set.
    /// </summary>
    public string? FullyQualifiedNamespace { get; set; }

    /// <summary>
    ///     Gets or sets the name of a pre-registered connection in <see cref="AzureConnectionOptions" />.
    ///     When set, overrides <see cref="ConnectionString" /> and <see cref="FullyQualifiedNamespace" />.
    /// </summary>
    public string? NamedConnection { get; set; }

    /// <summary>
    ///     Gets or sets the name of a pre-registered connection in <see cref="AzureConnectionOptions" />.
    ///     Alias for <see cref="NamedConnection" />.
    /// </summary>
    public string? NamedConnectionName
    {
        get => NamedConnection;
        set => NamedConnection = value;
    }

    /// <summary>
    ///     Gets or sets the authentication mode. Defaults to <see cref="AzureAuthenticationMode.ConnectionString" />.
    /// </summary>
    public AzureAuthenticationMode AuthenticationMode { get; set; } = AzureAuthenticationMode.ConnectionString;

    /// <summary>
    ///     Gets or sets a <see cref="TokenCredential" /> (e.g. <c>DefaultAzureCredential</c>) for Azure AD authentication.
    ///     Used when <see cref="AuthenticationMode" /> is <see cref="AzureAuthenticationMode.AzureAdCredential" />.
    ///     When omitted, the connector falls back to <c>DefaultAzureCredential</c>.
    /// </summary>
    public TokenCredential? Credential { get; set; }

    /// <summary>
    ///     Gets or sets the shared access key name used when
    ///     <see cref="AuthenticationMode" /> is <see cref="AzureAuthenticationMode.EndpointWithKey" />.
    /// </summary>
    public string? SharedAccessKeyName { get; set; }

    /// <summary>
    ///     Gets or sets the shared access key used when
    ///     <see cref="AuthenticationMode" /> is <see cref="AzureAuthenticationMode.EndpointWithKey" />.
    /// </summary>
    public string? SharedAccessKey { get; set; }

    // ── Entity Names ─────────────────────────────────────────────────────────────

    /// <summary>Gets or sets the queue name for queue-based source/sink nodes.</summary>
    public string? QueueName { get; set; }

    /// <summary>Gets or sets the topic name for topic-based sink or subscription source nodes.</summary>
    public string? TopicName { get; set; }

    /// <summary>Gets or sets the subscription name for <see cref="TopicName" />-based source nodes.</summary>
    public string? SubscriptionName { get; set; }

    // ── Source Options ───────────────────────────────────────────────────────────

    /// <summary>
    ///     Gets or sets the message prefetch count.
    ///     Higher values increase throughput at the cost of reduced message distribution across instances.
    ///     Defaults to 0 (no prefetch).
    /// </summary>
    public int PrefetchCount { get; set; }

    /// <summary>
    ///     Gets or sets the maximum number of concurrent messages processed at a time.
    ///     Set to 1 to preserve message ordering. Defaults to 1.
    /// </summary>
    public int MaxConcurrentCalls { get; set; } = 1;

    /// <summary>
    ///     Gets or sets the bounded in-memory channel capacity used to bridge the push-based
    ///     ServiceBusProcessor to the pull-based pipeline.
    ///     0 means the capacity equals <see cref="MaxConcurrentCalls" />.
    /// </summary>
    public int InternalBufferCapacity { get; set; }

    /// <summary>
    ///     Gets or sets the maximum duration for which message locks are automatically renewed.
    ///     Defaults to 5 minutes.
    /// </summary>
    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    ///     Gets or sets whether messages are automatically completed after the handler returns.
    ///     When <c>false</c> (default), you must explicitly call a settlement method on
    ///     <see cref="Models.ServiceBusMessage{T}" />.
    /// </summary>
    public bool EnableAutoComplete { get; set; }

    /// <summary>
    ///     Gets or sets the sub-queue to read from (e.g. dead-letter sub-queue). Defaults to None.
    /// </summary>
    public SubQueue SubQueue { get; set; } = SubQueue.None;

    // ── Sink Options ─────────────────────────────────────────────────────────────

    /// <summary>
    ///     Gets or sets whether batch sending is enabled. Defaults to <c>true</c>.
    /// </summary>
    public bool EnableBatchSending { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum number of messages per batch. Maximum is 100. Defaults to 100.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    ///     Gets or sets whether send operations should use Service Bus transactions.
    ///     Defaults to <c>false</c>.
    /// </summary>
    public bool EnableTransactionalSends { get; set; }

    // ── Session Options ──────────────────────────────────────────────────────────

    /// <summary>
    ///     Gets or sets whether session-enabled processing is active.
    ///     When <c>true</c>, use <see cref="Nodes.ServiceBusSessionSourceNode{T}" />.
    /// </summary>
    public bool EnableSessions { get; set; }

    /// <summary>Gets or sets the maximum number of concurrent sessions. Defaults to 8.</summary>
    public int MaxConcurrentSessions { get; set; } = 8;

    /// <summary>Gets or sets the maximum concurrent calls per session. Defaults to 1.</summary>
    public int SessionMaxConcurrentCallsPerSession { get; set; } = 1;

    /// <summary>Gets or sets the session idle timeout. Defaults to 1 minute.</summary>
    public TimeSpan SessionIdleTimeout { get; set; } = TimeSpan.FromMinutes(1);

    // ── Acknowledgment ───────────────────────────────────────────────────────────

    /// <summary>
    ///     Gets or sets the acknowledgment strategy. Defaults to
    ///     <see cref="Connectors.Configuration.AcknowledgmentStrategy.AutoOnSinkSuccess" />.
    /// </summary>
    public AcknowledgmentStrategy AcknowledgmentStrategy { get; set; } = AcknowledgmentStrategy.AutoOnSinkSuccess;

    // ── Retry ────────────────────────────────────────────────────────────────────

    /// <summary>Gets or sets the retry configuration for Service Bus client operations.</summary>
    public ServiceBusRetryConfiguration Retry { get; set; } = new();

    // ── Serialization ────────────────────────────────────────────────────────────

    /// <summary>
    ///     Gets or sets custom <see cref="System.Text.Json.JsonSerializerOptions" /> for message
    ///     serialization/deserialization. When <c>null</c>, uses camelCase with null-ignoring defaults.
    /// </summary>
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    // ── Error Handling ───────────────────────────────────────────────────────────

    /// <summary>
    ///     Gets or sets whether to continue processing on deserialization errors.
    ///     When <c>false</c>, an error stops the pipeline. Defaults to <c>false</c>.
    /// </summary>
    public bool ContinueOnDeserializationError { get; set; }

    /// <summary>
    ///     Gets or sets whether to dead-letter messages that fail deserialization.
    ///     Requires <see cref="ContinueOnDeserializationError" /> = <c>true</c>. Defaults to <c>true</c>.
    /// </summary>
    public bool DeadLetterOnDeserializationError { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to continue on general errors (e.g. send failures in the sink).
    ///     Defaults to <c>true</c>.
    /// </summary>
    public bool ContinueOnError { get; set; } = true;

    /// <summary>Resolves the effective internal buffer capacity.</summary>
    internal int EffectiveBufferCapacity =>
        InternalBufferCapacity > 0
            ? InternalBufferCapacity
            : Math.Max(MaxConcurrentCalls * 2, 8);

    // ── Validation ───────────────────────────────────────────────────────────────

    /// <summary>Validates source and sink settings together.</summary>
    public virtual void Validate()
    {
        ValidateSource();
        ValidateSink();
    }

    /// <summary>Validates that all settings required for a source node are present and valid.</summary>
    public virtual void ValidateSource()
    {
        ValidateConnection();
        ValidateQueueOrSubscription();
        ValidateRetry();
        ValidateBufferCapacity();
        ValidateConcurrency();

        if (EnableSessions)
        {
            if (MaxConcurrentSessions < 1)
                throw new InvalidOperationException("MaxConcurrentSessions must be at least 1.");

            if (SessionMaxConcurrentCallsPerSession < 1)
                throw new InvalidOperationException("SessionMaxConcurrentCallsPerSession must be at least 1.");
        }
    }

    /// <summary>Validates that all settings required for a sink node are present and valid.</summary>
    public virtual void ValidateSink()
    {
        ValidateConnection();

        if (string.IsNullOrWhiteSpace(QueueName) && string.IsNullOrWhiteSpace(TopicName))
        {
            throw new InvalidOperationException(
                "Either QueueName or TopicName must be specified for a sink node.");
        }

        if (BatchSize is < 1 or > 100)
            throw new InvalidOperationException("BatchSize must be between 1 and 100.");

        ValidateRetry();
    }

    private void ValidateConnection()
    {
        if (!string.IsNullOrWhiteSpace(NamedConnection))
            return; // Named connection will be resolved at runtime

        switch (AuthenticationMode)
        {
            case AzureAuthenticationMode.ConnectionString:
                if (string.IsNullOrWhiteSpace(ConnectionString))
                {
                    throw new InvalidOperationException(
                        "ConnectionString must be specified when AuthenticationMode is ConnectionString " +
                        "and NamedConnection is not used.");
                }

                break;
            case AzureAuthenticationMode.AzureAdCredential:
                if (string.IsNullOrWhiteSpace(FullyQualifiedNamespace))
                {
                    throw new InvalidOperationException(
                        "FullyQualifiedNamespace must be specified when AuthenticationMode is AzureAdCredential " +
                        "and NamedConnection is not used.");
                }

                break;
            case AzureAuthenticationMode.EndpointWithKey:
                if (string.IsNullOrWhiteSpace(FullyQualifiedNamespace))
                {
                    throw new InvalidOperationException(
                        "FullyQualifiedNamespace must be specified when AuthenticationMode is EndpointWithKey " +
                        "and NamedConnection is not used.");
                }

                if (string.IsNullOrWhiteSpace(SharedAccessKeyName))
                {
                    throw new InvalidOperationException(
                        "SharedAccessKeyName must be specified when AuthenticationMode is EndpointWithKey.");
                }

                if (string.IsNullOrWhiteSpace(SharedAccessKey))
                {
                    throw new InvalidOperationException(
                        "SharedAccessKey must be specified when AuthenticationMode is EndpointWithKey.");
                }

                break;
        }
    }

    private void ValidateQueueOrSubscription()
    {
        var hasQueue = !string.IsNullOrWhiteSpace(QueueName);
        var hasTopicAndSub = !string.IsNullOrWhiteSpace(TopicName) && !string.IsNullOrWhiteSpace(SubscriptionName);

        if (!hasQueue && !hasTopicAndSub)
        {
            throw new InvalidOperationException(
                "Either QueueName or (TopicName + SubscriptionName) must be specified for a source node.");
        }
    }

    private void ValidateRetry()
    {
        if (Retry.MaxRetries < 0)
            throw new InvalidOperationException("Retry.MaxRetries must be non-negative.");

        if (Retry.Delay < TimeSpan.Zero)
            throw new InvalidOperationException("Retry.Delay must be non-negative.");

        if (Retry.MaxDelay < TimeSpan.Zero)
            throw new InvalidOperationException("Retry.MaxDelay must be non-negative.");

        if (Retry.MaxDelay < Retry.Delay)
            throw new InvalidOperationException("Retry.MaxDelay must be greater than or equal to Retry.Delay.");
    }

    private void ValidateBufferCapacity()
    {
        if (InternalBufferCapacity < 0)
            throw new InvalidOperationException("InternalBufferCapacity must be non-negative (0 = auto).");
    }

    private void ValidateConcurrency()
    {
        if (MaxConcurrentCalls < 1)
            throw new InvalidOperationException("MaxConcurrentCalls must be at least 1.");
    }
}
