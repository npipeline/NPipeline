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
///     Source node that consumes messages from an Azure Service Bus <em>queue</em>.
///     Messages are deserialized from JSON and wrapped as <see cref="ServiceBusMessage{T}" />
///     for explicit settlement support.
/// </summary>
/// <typeparam name="T">The type to deserialize message bodies into.</typeparam>
/// <remarks>
///     <para>
///         Uses <see cref="ServiceBusProcessor" /> with a bounded <see cref="Channel{T}" /> to bridge the
///         push-based SDK delivery model to the pull-based NPipeline <see cref="IDataStream{T}" /> contract.
///     </para>
///     <para>
///         Each message handler blocks on a per-message <see cref="TaskCompletionSource{T}" /> until
///         settlement (Complete / Abandon / DeadLetter / Defer) is called on the
///         <see cref="ServiceBusMessage{T}" /> wrapper.  This guarantees the message lock is never held
///         beyond the settlement point, and lock renewal (up to <see cref="ServiceBusConfiguration.MaxAutoLockRenewalDuration" />)
///         operates correctly for long-running downstream transforms.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// var config = new ServiceBusConfiguration
/// {
///     ConnectionString = connectionString,
///     QueueName = "orders",
///     MaxConcurrentCalls = 5,
///     PrefetchCount = 20,
/// };
/// var source = builder.AddSource(new ServiceBusQueueSourceNode&lt;Order&gt;(config), "orders-source");
///     </code>
/// </example>
public sealed class ServiceBusQueueSourceNode<T> : SourceNode<ServiceBusMessage<T>>
{
    private readonly ServiceBusConfiguration _configuration;
    private readonly ILogger _logger;
    private readonly ServiceBusProcessor _processor;
    private readonly JsonSerializerOptions _serializerOptions;
    private Channel<ServiceBusMessage<T>>? _messageChannel;

    /// <summary>
    ///     Creates a <see cref="ServiceBusQueueSourceNode{T}" /> from the supplied configuration.
    ///     A dedicated <see cref="ServiceBusClient" /> is created for this node.
    /// </summary>
    public ServiceBusQueueSourceNode(ServiceBusConfiguration configuration, ILogger? logger = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();
        _serializerOptions = CreateSerializerOptions(_configuration);
        _logger = logger ?? NullLogger.Instance;

        var client = ServiceBusClientFactory.Create(_configuration);
        _processor = CreateProcessor(client, _configuration);
    }

    /// <summary>
    ///     Creates a <see cref="ServiceBusQueueSourceNode{T}" /> injecting a pre-constructed client (testing).
    /// </summary>
    public ServiceBusQueueSourceNode(
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

    /// <summary>
    ///     Internal constructor for unit tests that inject a processor directly.
    /// </summary>
    internal ServiceBusQueueSourceNode(
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

        // Wire processor event handlers
        _processor.ProcessMessageAsync += args => OnMessageReceivedAsync(args, cancellationToken);
        _processor.ProcessErrorAsync += OnErrorAsync;

        var stream = ConsumeFromChannelAsync(cancellationToken);

        return new DataStream<ServiceBusMessage<T>>(stream,
            $"ServiceBusQueueSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<ServiceBusMessage<T>> ConsumeFromChannelAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await _processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
        ServiceBusLogMessages.ProcessorStarted(_logger, _configuration.QueueName ?? string.Empty);

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
                   ?? throw new JsonException(
                       $"Deserialization returned null for type {typeof(T).Name}.");
        }
        catch (JsonException ex)
        {
            ServiceBusLogMessages.DeserializationFailed(_logger, ex,
                args.Message.MessageId, _configuration.QueueName ?? string.Empty);

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
            (reason, desc, ct2) => args.DeadLetterMessageAsync(
                args.Message, reason, desc, ct2),
            (props, ct2) => args.DeferMessageAsync(args.Message, props, ct2),
            settlementTcs);

        // Write to the channel (blocks with backpressure if full)
        await _messageChannel!.Writer.WriteAsync(message, ct).ConfigureAwait(false);
        ServiceBusLogMessages.MessageEnqueued(_logger, args.Message.MessageId);

        // Block until settlement — keeps the message lock alive and prevents the SDK
        // from auto-completing / auto-abandoning when this handler returns.
        try
        {
            await settlementTcs.Task.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Processor is shutting down; best-effort abandon
            if (!message.IsSettled)
            {
                try
                {
                    await args.AbandonMessageAsync(args.Message, null, CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    /* swallow — shutdown path */
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

        return configuration.QueueName is { Length: > 0 }
            ? client.CreateProcessor(configuration.QueueName, options)
            : client.CreateProcessor(
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
