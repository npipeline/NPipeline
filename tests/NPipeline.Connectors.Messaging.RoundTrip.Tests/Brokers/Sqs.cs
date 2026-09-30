using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using NPipeline.Connectors.Aws.Sqs;
using NPipeline.Connectors.Aws.Sqs.Models;
using NPipeline.Connectors.Messaging.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.Messaging.RoundTrip.Tests.Infrastructure;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Messaging.RoundTrip.Tests.Brokers;

public sealed class SqsHarness(string serviceUrl) : MessagingHarness
{
    private const int VisibilityTimeoutSeconds = 2;

    private readonly AmazonSQSClient _client = new(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = serviceUrl });

    public override string Name => "SQS";

    public override TimeSpan RedeliveryDelay => TimeSpan.FromSeconds(VisibilityTimeoutSeconds + 1);

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        _client.Dispose();
    }

    public override async Task<string> CreateDestinationAsync() =>
        (await _client.CreateQueueAsync(new CreateQueueRequest($"rt-{Guid.NewGuid():N}"))).QueueUrl;

    public override async Task PublishRawAsync(string destination, params string[] bodies)
    {
        foreach (var body in bodies)
        {
            _ = await _client.SendMessageAsync(destination, body);
        }
    }

    public override async Task<List<string>> ReceiveRawAsync(string destination, int count, TimeSpan timeout)
    {
        var bodies = new List<string>();
        var deadline = DateTime.UtcNow + timeout;

        while (bodies.Count < count && DateTime.UtcNow < deadline)
        {
            var response = await _client.ReceiveMessageAsync(new ReceiveMessageRequest(destination) { MaxNumberOfMessages = 10, WaitTimeSeconds = 1 });

            foreach (var message in response.Messages ?? [])
            {
                bodies.Add(message.Body);
                _ = await _client.DeleteMessageAsync(destination, message.ReceiptHandle);
            }
        }

        return bodies;
    }

    public override IAsyncEnumerable<IAcknowledgableMessage<T>> ReadAsync<T>(string destination, ReadSettings settings, PipelineContext context,
        CancellationToken cancellationToken) =>
        Enumerate<SqsMessage<T>, T>(SqsConnector.Source<T>(destination, o => o with
        {
            Client = _client,
            VisibilityTimeout = TimeSpan.FromSeconds(VisibilityTimeoutSeconds),
            WaitTime = TimeSpan.FromSeconds(1),
            RowErrorHandler = settings.Handler,
        }), context, cancellationToken);

    protected override SinkNode<T> Sink<T>(string destination) => SqsConnector.Sink<T>(destination, o => o with { Client = _client });
}

[Collection(SqsBrokers.Name)]
public sealed class SqsRoundTripTests(SqsBrokerFixture broker) : MessagingRoundTripTests(new SqsHarness(broker.ServiceUrl));
