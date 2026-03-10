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
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.ServiceBus.Nodes;

/// <summary>
///     Source node that consumes messages from an Azure Service Bus <em>topic subscription</em>.
///     Messages are deserialized from JSON and wrapped as <see cref="ServiceBusMessage{T}" />
///     for explicit settlement support.
/// </summary>
/// <typeparam name="T">The type to deserialize message bodies into.</typeparam>
/// <remarks>
///     Requires both <see cref="ServiceBusConfiguration.TopicName" /> and
///     <see cref="ServiceBusConfiguration.SubscriptionName" /> to be set.
///     See <see cref="ServiceBusQueueSourceNode{T}" /> for the queue variant.
/// </remarks>
public sealed class ServiceBusSubscriptionSourceNode<T> : SourceNode<ServiceBusMessage<T>>
{
    private readonly ServiceBusConfiguration _configuration;
    private readonly ILogger _logger;
    private readonly ServiceBusProcessor _processor;
    private readonly JsonSerializerOptions _serializerOptions;
    private Channel<ServiceBusMessage<T>>? _messageChannel;

    /// <summary>
    ///     Creates a <see cref="ServiceBusSubscriptionSourceNode{T}" /> from the supplied configuration.
    /// </summary>
    public ServiceBusSubscriptionSourceNode(ServiceBusConfiguration configuration, ILogger? logger = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();

        if (string.IsNullOrWhiteSpace(_configuration.TopicName) ||
            string.IsNullOrWhiteSpace(_configuration.SubscriptionName))
        {
            throw new ArgumentException(
                "TopicName and SubscriptionName must be specified for a subscription source node.",
                nameof(configuration));
        }

        _serializerOptions = CreateSerializerOptions(_configuration);
        _logger = logger ?? NullLogger.Instance;
        var client = ServiceBusClientFactory.Create(_configuration);
        _processor = CreateProcessor(client, _configuration);
    }

    /// <summary>
    ///     Creates a <see cref="ServiceBusSubscriptionSourceNode{T}" /> injecting a pre-constructed client (testing).
    /// </summary>
    public ServiceBusSubscriptionSourceNode(
        ServiceBusClient client,
        ServiceBusConfiguration configuration,
        ILogger? logger = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();
        _serializerOptions = CreateSerializerOptions(_configuration);
        _logger = logger ?? NullLogger.Instance;

        _processor = CreateProcessor(
            client ?? throw new ArgumentNullException(nameof(client)),
            _configuration);
    }

    internal ServiceBusSubscriptionSourceNode(
        ServiceBusProcessor processor,
        ServiceBusConfiguration configuration,
        ILogger? logger = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));
        _serializerOptions = CreateSerializerOptions(_configuration);
        _logger = logger ?? NullLogger.Instance;
    }

    /// <inheritdoc />
    public override IDataStream<ServiceBusMessage<T>> OpenStream(
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
        var entityName = $"{_configuration.TopicName}/{_configuration.SubscriptionName}";

        return new DataStream<ServiceBusMessage<T>>(stream,
            $"ServiceBusSubscriptionSourceNode<{typeof(T).Name}>[{entityName}]");
    }

    private async IAsyncEnumerable<ServiceBusMessage<T>> ConsumeFromChannelAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var entityName = $"{_configuration.TopicName}/{_configuration.SubscriptionName}";
        await _processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
        ServiceBusLogMessages.ProcessorStarted(_logger, entityName);

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
            await StopProcessorSafeAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task OnMessageReceivedAsync(
        ProcessMessageEventArgs args,
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
            var entity = $"{_configuration.TopicName}/{_configuration.SubscriptionName}";
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
        ServiceBusLogMessages.ProcessorError(_logger, args.Exception,
            args.EntityPath, args.ErrorSource.ToString());

        return Task.CompletedTask;
    }

    private async Task StopProcessorSafeAsync(CancellationToken ct)
    {
        try
        {
            await _processor.StopProcessingAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            ServiceBusLogMessages.ProcessorStopFailed(_logger, ex);
        }
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

    private static ServiceBusProcessor CreateProcessor(
        ServiceBusClient client,
        ServiceBusConfiguration configuration)
    {
        var options = new ServiceBusProcessorOptions
        {
            PrefetchCount = configuration.PrefetchCount,
            MaxConcurrentCalls = configuration.MaxConcurrentCalls,
            MaxAutoLockRenewalDuration = configuration.MaxAutoLockRenewalDuration,
            AutoCompleteMessages = false,
            SubQueue = configuration.SubQueue,
        };

        return client.CreateProcessor(
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
