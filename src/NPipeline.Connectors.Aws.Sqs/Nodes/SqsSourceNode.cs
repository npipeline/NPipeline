using System.Runtime.CompilerServices;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Internal;
using NPipeline.Connectors.Aws.Sqs.Models;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Aws.Sqs.Nodes;

/// <summary>
///     Source node that continuously polls an SQS queue and yields messages.
/// </summary>
/// <typeparam name="T">Type to deserialize message body to.</typeparam>
public sealed class SqsSourceNode<T> : SourceNode<SqsMessage<T>>
{
    private readonly SqsConfiguration _configuration;
    private readonly JsonSerializerOptions _serializerOptions;
    private readonly IAmazonSQS _sqsClient;

    /// <summary>
    ///     Creates a new SqsSourceNode with the specified configuration.
    /// </summary>
    public SqsSourceNode(SqsConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();

        _sqsClient = SqsClientFactory.Create(configuration);
        _serializerOptions = CreateSerializerOptions(configuration);
    }

    /// <summary>
    ///     Creates a new SqsSourceNode with a custom SQS client.
    /// </summary>
    public SqsSourceNode(IAmazonSQS sqsClient, SqsConfiguration configuration)
    {
        _sqsClient = sqsClient ?? throw new ArgumentNullException(nameof(sqsClient));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();

        _serializerOptions = CreateSerializerOptions(configuration);
    }

    /// <inheritdoc />
    public override IDataStream<SqsMessage<T>> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var stream = PollMessagesAsync(cancellationToken);
        return new DataStream<SqsMessage<T>>(stream, $"SqsSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<SqsMessage<T>> PollMessagesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // A failed receive is not retried here. The SDK client already retried it (see SqsConfiguration.RetryMode and
        // MaxErrorRetry), so an exception that reaches this loop has used up its retries and fails the stream.
        while (true)
        {
            // Cancellation surfaces as OperationCanceledException rather than ending the stream as if it had drained.
            cancellationToken.ThrowIfCancellationRequested();

            var receiveRequest = new ReceiveMessageRequest
            {
                QueueUrl = _configuration.SourceQueueUrl,
                MaxNumberOfMessages = _configuration.MaxNumberOfMessages,
                WaitTimeSeconds = _configuration.WaitTimeSeconds,
                VisibilityTimeout = _configuration.VisibilityTimeout,
                MessageSystemAttributeNames = ["All"],
                MessageAttributeNames = ["All"],
            };

            var response = await _sqsClient.ReceiveMessageAsync(receiveRequest, cancellationToken).ConfigureAwait(false);
            var messagesToYield = new List<SqsMessage<T>>(response.Messages.Count);

            foreach (var message in response.Messages)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var sqsMessage = CreateSqsMessage(message);

                if (sqsMessage != null)
                    messagesToYield.Add(sqsMessage);
            }

            // No messages, or all of them filtered out (e.g., invalid JSON with ContinueOnError): wait before polling again.
            if (messagesToYield.Count == 0)
            {
                if (_configuration.PollingIntervalMs > 0)
                    await Task.Delay(_configuration.PollingIntervalMs, cancellationToken).ConfigureAwait(false);

                continue;
            }

            foreach (var message in messagesToYield)
            {
                yield return message;
            }
        }
    }

    private SqsMessage<T>? CreateSqsMessage(Message sqsMessage)
    {
        try
        {
            // Deserialize JSON body
            var body = JsonSerializer.Deserialize<T>(sqsMessage.Body, _serializerOptions);

            if (body == null)
                return null;

            // Parse timestamp
            var timestamp = DateTime.UtcNow;

            if (sqsMessage.Attributes.TryGetValue("SentTimestamp", out var sentTimestampStr) &&
                long.TryParse(sentTimestampStr, out var sentTimestamp))
                timestamp = DateTimeOffset.FromUnixTimeMilliseconds(sentTimestamp).UtcDateTime;

            return new SqsMessage<T>(
                body,
                sqsMessage.MessageId,
                sqsMessage.ReceiptHandle,
                sqsMessage.MessageAttributes ?? new Dictionary<string, MessageAttributeValue>(),
                timestamp,
                _sqsClient,
                _configuration.SourceQueueUrl);
        }
        catch (JsonException ex)
        {
            var handler = _configuration.MessageErrorHandler;

            if (handler != null)
            {
                var errorWrapper = new SqsMessage<object>(
                    sqsMessage.Body,
                    sqsMessage.MessageId,
                    sqsMessage.ReceiptHandle,
                    sqsMessage.MessageAttributes ?? new Dictionary<string, MessageAttributeValue>(),
                    DateTime.UtcNow,
                    _sqsClient,
                    _configuration.SourceQueueUrl);

                if (handler(ex, errorWrapper))
                    return null; // Handler opted to skip
            }

            if (_configuration.ContinueOnError)
                return null;

            throw;
        }
    }

    private static JsonSerializerOptions CreateSerializerOptions(SqsConfiguration configuration)
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = configuration.PropertyNameCaseInsensitive,
            PropertyNamingPolicy = configuration.PropertyNamingPolicy switch
            {
                JsonPropertyNamingPolicy.CamelCase => JsonNamingPolicy.CamelCase,
                JsonPropertyNamingPolicy.SnakeCase => JsonNamingPolicy.SnakeCaseLower,
                JsonPropertyNamingPolicy.LowerCase => new LowerCaseNamingPolicy(),
                JsonPropertyNamingPolicy.PascalCase => new PascalCaseNamingPolicy(),
                JsonPropertyNamingPolicy.AsIs => null,
                _ => JsonNamingPolicy.CamelCase,
            },
        };

        return options;
    }

    private sealed class LowerCaseNamingPolicy : JsonNamingPolicy
    {
        public static readonly LowerCaseNamingPolicy Instance = new();

        public override string ConvertName(string name)
        {
            return string.IsNullOrEmpty(name)
                ? name
                : name.ToLowerInvariant();
        }
    }

    private sealed class PascalCaseNamingPolicy : JsonNamingPolicy
    {
        public static readonly PascalCaseNamingPolicy Instance = new();

        public override string ConvertName(string name)
        {
            if (string.IsNullOrEmpty(name))
                return name;

            return char.ToUpperInvariant(name[0]) + name[1..];
        }
    }
}
