using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Connection;
using NPipeline.Connectors.Azure.ServiceBus.Models;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.ServiceBus.Nodes;

/// <summary>
///     Source node that consumes session-enabled messages from an Azure Service Bus queue or
///     topic subscription, preserving per-session ordering guarantees.
/// </summary>
/// <typeparam name="T">The type to deserialize message bodies into.</typeparam>
/// <remarks>
///     <para>
///         Requires <see cref="ServiceBusConfiguration.EnableSessions" /> = <c>true</c> and the
///         target entity to have sessions enabled in Azure.
///     </para>
///     <para>
///         Uses <see cref="ServiceBusSessionProcessor" /> which acquires session locks and dispatches
///         messages in session order.  Multiple sessions are processed concurrently up to
///         <see cref="ServiceBusConfiguration.MaxConcurrentSessions" />.
///     </para>
/// </remarks>
public sealed class ServiceBusSessionSourceNode<T> : SourceNode<ServiceBusMessage<T>>
{
    private readonly ServiceBusConfiguration _configuration;
    private readonly ILogger _logger;
    private readonly ServiceBusSessionProcessor _processor;
    private readonly JsonSerializerOptions _serializerOptions;
    private Channel<ServiceBusMessage<T>>? _messageChannel;

    /// <summary>
    ///     Creates a <see cref="ServiceBusSessionSourceNode{T}" /> from the supplied configuration.
    /// </summary>
    public ServiceBusSessionSourceNode(ServiceBusConfiguration configuration, ILogger? logger = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();

        if (!_configuration.EnableSessions)
        {
            throw new ArgumentException(
                "EnableSessions must be set to true in ServiceBusConfiguration for the session source node.",
                nameof(configuration));
        }

        _serializerOptions = CreateSerializerOptions(_configuration);
        _logger = logger ?? NullLogger.Instance;
        var client = ServiceBusClientFactory.Create(_configuration);
        _processor = CreateSessionProcessor(client, _configuration);
    }

    /// <summary>
    ///     Creates a <see cref="ServiceBusSessionSourceNode{T}" /> injecting a pre-constructed client (testing).
    /// </summary>
    public ServiceBusSessionSourceNode(
        ServiceBusClient client,
        ServiceBusConfiguration configuration,
        ILogger? logger = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();
        _serializerOptions = CreateSerializerOptions(_configuration);
        _logger = logger ?? NullLogger.Instance;

        _processor = CreateSessionProcessor(
            client ?? throw new ArgumentNullException(nameof(client)),
            _configuration);
    }

    internal ServiceBusSessionSourceNode(
        ServiceBusSessionProcessor processor,
        ServiceBusConfiguration configuration,
        ILogger? logger = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));
        _serializerOptions = CreateSerializerOptions(_configuration);
        _logger = logger ?? NullLogger.Instance;
    }

    /// <inheritdoc />
    public override IDataPipe<ServiceBusMessage<T>> Initialize(
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var capacity = _configuration.EffectiveBufferCapacity;

        _messageChannel = Channel.CreateBounded<ServiceBusMessage<T>>(
            new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
            });

        _processor.ProcessMessageAsync += args => OnMessageReceivedAsync(args, cancellationToken);
        _processor.ProcessErrorAsync += OnErrorAsync;

        var stream = ConsumeFromChannelAsync(cancellationToken);
        var entityName = _configuration.QueueName ?? $"{_configuration.TopicName}/{_configuration.SubscriptionName}";

        return new StreamingDataPipe<ServiceBusMessage<T>>(stream,
            $"ServiceBusSessionSourceNode<{typeof(T).Name}>[{entityName}]");
    }

    private async IAsyncEnumerable<ServiceBusMessage<T>> ConsumeFromChannelAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var entityName = _configuration.QueueName ?? $"{_configuration.TopicName}/{_configuration.SubscriptionName}";
        await _processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
        ServiceBusLogMessages.SessionProcessorStarted(_logger, entityName);

        try
        {
            await foreach (var message in _messageChannel!.Reader
                               .ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return message;
            }
        }
        finally
        {
            try
            {
                await _processor.StopProcessingAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ServiceBusLogMessages.ProcessorStopFailed(_logger, ex);
            }
        }
    }

    private async Task OnMessageReceivedAsync(
        ProcessSessionMessageEventArgs args,
        CancellationToken externalCt)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            args.CancellationToken, externalCt);

        var ct = linkedCts.Token;

        T body;

        try
        {
            body = JsonSerializer.Deserialize<T>(args.Message.Body.ToArray(), _serializerOptions)
                   ?? throw new JsonException($"Deserialization returned null for type {typeof(T).Name}.");
        }
        catch (JsonException ex)
        {
            var entity = _configuration.QueueName ?? $"{_configuration.TopicName}/{_configuration.SubscriptionName}";
            ServiceBusLogMessages.DeserializationFailed(_logger, ex, args.Message.MessageId, entity);

            if (_configuration.DeadLetterOnDeserializationError)
            {
                await args.DeadLetterMessageAsync(args.Message,
                    "DeserializationError",
                    ex.Message, ct).ConfigureAwait(false);

                return;
            }

            if (_configuration.ContinueOnDeserializationError)
                return;

            _messageChannel?.Writer.TryComplete(ex);
            return;
        }

        var settlementTcs = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var message = new ServiceBusMessage<T>(
            body,
            args.Message.MessageId,
            args.Message,
            ct2 => args.CompleteMessageAsync(args.Message, ct2),
            (props, ct2) => args.AbandonMessageAsync(args.Message, props, ct2),
            (reason, desc, ct2) => args.DeadLetterMessageAsync(args.Message, reason, desc, ct2),
            (props, ct2) => args.DeferMessageAsync(args.Message, props, ct2),
            settlementTcs);

        await _messageChannel!.Writer.WriteAsync(message, ct).ConfigureAwait(false);
        ServiceBusLogMessages.MessageEnqueued(_logger, args.Message.MessageId);

        try
        {
            await settlementTcs.Task.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            if (!message.IsSettled)
            {
                try
                {
                    await args.AbandonMessageAsync(args.Message, null, CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    /* shutdown path */
                }
            }
        }
    }

    private Task OnErrorAsync(ProcessErrorEventArgs args)
    {
        ServiceBusLogMessages.SessionProcessorError(_logger, args.Exception,
            args.EntityPath, args.ErrorSource.ToString());

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        _messageChannel?.Writer.TryComplete();

        try
        {
            if (_processor.IsProcessing)
                await _processor.StopProcessingAsync().ConfigureAwait(false);
        }
        catch
        {
            /* best-effort */
        }

        await _processor.DisposeAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }

    private static ServiceBusSessionProcessor CreateSessionProcessor(
        ServiceBusClient client,
        ServiceBusConfiguration configuration)
    {
        var options = new ServiceBusSessionProcessorOptions
        {
            PrefetchCount = configuration.PrefetchCount,
            MaxConcurrentSessions = configuration.MaxConcurrentSessions,
            MaxConcurrentCallsPerSession = configuration.SessionMaxConcurrentCallsPerSession,
            MaxAutoLockRenewalDuration = configuration.MaxAutoLockRenewalDuration,
            AutoCompleteMessages = false,
            SessionIdleTimeout = configuration.SessionIdleTimeout,
        };

        return configuration.QueueName is { Length: > 0 }
            ? client.CreateSessionProcessor(configuration.QueueName, options)
            : client.CreateSessionProcessor(
                configuration.TopicName!,
                configuration.SubscriptionName!,
                options);
    }

    private static JsonSerializerOptions CreateSerializerOptions(ServiceBusConfiguration config)
    {
        return config.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
    }
}
