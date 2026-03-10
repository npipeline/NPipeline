using System.Text.Json;
using System.Text.Json.Serialization;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Models;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Integration;

[Collection("ServiceBusIntegration")]
public sealed class ServiceBusConnectorIntegrationTests
{
    private readonly ServiceBusIntegrationFixture _fixture;

    public ServiceBusConnectorIntegrationTests(ServiceBusIntegrationFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task QueueSinkNode_SendsMessage_AndMessageCanBeReceived()
    {
        if (!_fixture.IsAvailable)
            return;

        var queueName = $"np-sb-int-roundtrip-{Guid.NewGuid():N}";
        await _fixture.EnsureQueueExistsAsync(queueName);

        try
        {
            var config = new ServiceBusConfiguration
            {
                ConnectionString = _fixture.ConnectionString,
                QueueName = queueName,
                EnableBatchSending = false,
            };

            var sink = new ServiceBusQueueSinkNode<PublishOrder>(config);

            IDataStream<PublishOrder> input = new DataStream<PublishOrder>(
                new[]
                {
                    new PublishOrder
                    {
                        OrderId = 42,
                        Description = "Roundtrip",
                        MessageId = $"order-{Guid.NewGuid():N}",
                    },
                }.ToAsyncEnumerable());

            await sink.ConsumeAsync(input, PipelineContext.Default, CancellationToken.None);
            await sink.DisposeAsync();

            await using var client = _fixture.CreateClient();
            var receiver = client.CreateReceiver(queueName);
            var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(20));

            received.Should().NotBeNull();
            received!.MessageId.Should().StartWith("order-");

            var payload = JsonSerializer.Deserialize<PublishOrder>(received.Body.ToArray());
            payload.Should().NotBeNull();
            payload!.OrderId.Should().Be(42);
            payload.Description.Should().Be("Roundtrip");

            await receiver.CompleteMessageAsync(received);
            await receiver.DisposeAsync();
        }
        finally
        {
            await _fixture.DeleteQueueIfExistsAsync(queueName);
        }
    }

    [Fact]
    public async Task QueueSinkNode_AppliesScheduledEnqueueTime_FromPublishMetadata()
    {
        if (!_fixture.IsAvailable)
            return;

        var queueName = $"np-sb-int-scheduled-{Guid.NewGuid():N}";
        await _fixture.EnsureQueueExistsAsync(queueName);

        try
        {
            var dueAt = DateTimeOffset.UtcNow.AddSeconds(6);

            var config = new ServiceBusConfiguration
            {
                ConnectionString = _fixture.ConnectionString,
                QueueName = queueName,
                EnableBatchSending = false,
            };

            var sink = new ServiceBusQueueSinkNode<PublishOrder>(config);

            IDataStream<PublishOrder> input = new DataStream<PublishOrder>(
                new[]
                {
                    new PublishOrder
                    {
                        OrderId = 7,
                        Description = "Scheduled",
                        ScheduledEnqueueTimeUtc = dueAt,
                    },
                }.ToAsyncEnumerable());

            await sink.ConsumeAsync(input, PipelineContext.Default, CancellationToken.None);
            await sink.DisposeAsync();

            await using var client = _fixture.CreateClient();
            var receiver = client.CreateReceiver(queueName);

            var beforeDue = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
            beforeDue.Should().BeNull();

            await Task.Delay(TimeSpan.FromSeconds(6));
            var afterDue = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(20));
            afterDue.Should().NotBeNull();
            await receiver.CompleteMessageAsync(afterDue!);
            await receiver.DisposeAsync();
        }
        finally
        {
            await _fixture.DeleteQueueIfExistsAsync(queueName);
        }
    }

    [Fact]
    public async Task QueueSinkNode_WithTransactionalSends_EmitsMessage()
    {
        if (!_fixture.IsAvailable)
            return;

        var queueName = $"np-sb-int-tx-{Guid.NewGuid():N}";
        await _fixture.EnsureQueueExistsAsync(queueName);

        try
        {
            var config = new ServiceBusConfiguration
            {
                ConnectionString = _fixture.ConnectionString,
                QueueName = queueName,
                EnableBatchSending = false,
                EnableTransactionalSends = true,
            };

            var sink = new ServiceBusQueueSinkNode<PublishOrder>(config);

            IDataStream<PublishOrder> input = new DataStream<PublishOrder>(
                new[]
                {
                    new PublishOrder
                    {
                        OrderId = 99,
                        Description = "Transactional",
                    },
                }.ToAsyncEnumerable());

            await sink.ConsumeAsync(input, PipelineContext.Default, CancellationToken.None);
            await sink.DisposeAsync();

            await using var client = _fixture.CreateClient();
            var receiver = client.CreateReceiver(queueName);
            var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(20));
            received.Should().NotBeNull();
            await receiver.CompleteMessageAsync(received!);
            await receiver.DisposeAsync();
        }
        finally
        {
            await _fixture.DeleteQueueIfExistsAsync(queueName);
        }
    }

    private sealed class PublishOrder : IServiceBusPublishMetadata
    {
        public int OrderId { get; set; }

        public string Description { get; set; } = string.Empty;

        [JsonIgnore]
        public string? MessageId { get; set; }

        [JsonIgnore]
        public string? CorrelationId { get; set; }

        [JsonIgnore]
        public string? SessionId { get; set; }

        [JsonIgnore]
        public string? PartitionKey { get; set; }

        [JsonIgnore]
        public string? Subject { get; set; }

        [JsonIgnore]
        public string? ContentType { get; set; }

        [JsonIgnore]
        public TimeSpan? TimeToLive { get; set; }

        [JsonIgnore]
        public DateTimeOffset? ScheduledEnqueueTimeUtc { get; set; }

        [JsonIgnore]
        public IReadOnlyDictionary<string, object>? ApplicationProperties { get; set; }
    }
}
