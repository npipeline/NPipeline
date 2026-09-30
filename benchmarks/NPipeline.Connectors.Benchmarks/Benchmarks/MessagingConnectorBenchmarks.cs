using System.Text;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.Aws.Sqs;
using NPipeline.Connectors.Azure.ServiceBus;
using NPipeline.Connectors.Kafka;
using NPipeline.Connectors.RabbitMQ;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using RabbitMQ.Client;
using Testcontainers.Kafka;
using Testcontainers.Floci;
using Testcontainers.RabbitMq;
using Testcontainers.ServiceBus;

namespace NPipeline.Connectors.Benchmarks.Benchmarks;

/// <summary>
///     Shared setup for message-queue connectors. <c>Publish</c> writes the messages through the connector's sink with its
///     defaults; <c>Consume</c> reads them through its source and acknowledges each one. Every iteration uses a new
///     queue or topic, filled with the broker's own client before <c>Consume</c>. The brokers run in Docker, so the
///     numbers include the round trips to a local container.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(InProcessConfig))]
public abstract class MessagingConnectorBenchmark
{
    /// <summary>Messages per operation; each is one 20-column record as JSON.</summary>
    protected virtual int Messages => 2_000;

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private string[] _bodies = [];
    private string _destination = string.Empty;

    protected WideRecord[] Records { get; private set; } = [];

    /// <summary>How the bodies <c>Consume</c> reads are written.</summary>
    protected virtual JsonSerializerOptions ConsumeJson => Web;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        await StartAsync();
        Records = WideRecord.Generate(Messages);
        _bodies = [.. Records.Select(r => JsonSerializer.Serialize(r, ConsumeJson))];
    }

    [IterationSetup(Target = nameof(Publish))]
    public void NewDestination() => _destination = CreateDestinationAsync().GetAwaiter().GetResult();

    [IterationSetup(Target = nameof(Consume))]
    public void FillDestination()
    {
        _destination = CreateDestinationAsync().GetAwaiter().GetResult();
        PublishRawAsync(_destination, _bodies).GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task Publish()
    {
        var sink = CreateSink(_destination);
        await NodeRunner.WriteAsync(sink, Records);
    }

    [Benchmark]
    public async Task<int> Consume()
    {
        var source = CreateSource(_destination);
        var count = 0;

        try
        {
            await foreach (var message in source.OpenStream(PipelineContext.CreateDefault(), CancellationToken.None))
            {
                await ((IAcknowledgableMessage)message!).AcknowledgeAsync();

                if (++count == Messages)
                    break;
            }
        }
        finally
        {
            if (source is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }

        return count;
    }

    protected abstract Task StartAsync();

    protected abstract Task<string> CreateDestinationAsync();

    protected abstract Task PublishRawAsync(string destination, string[] bodies);

    protected abstract SinkNode<WideRecord> CreateSink(string destination);

    protected abstract SourceNode<object> CreateSource(string destination);

    /// <summary>A source of <typeparamref name="TMessage" /> seen as a source of objects, so the base can drain it.</summary>
    protected sealed class ObjectSource<TMessage>(SourceNode<TMessage> inner) : SourceNode<object>, IAsyncDisposable
    {
        public override DataFlow.IDataStream<object> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new DataFlow.DataStreams.DataStream<object>(Items(context, cancellationToken), "benchmark");

        public async ValueTask DisposeAsync()
        {
            if (inner is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }

        private async IAsyncEnumerable<object> Items(PipelineContext context,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var item in inner.OpenStream(context, cancellationToken).WithCancellation(cancellationToken))
            {
                yield return item!;
            }
        }
    }
}

public class KafkaBenchmarks : MessagingConnectorBenchmark
{
    private string _bootstrap = string.Empty;


    protected override async Task StartAsync()
    {
        var container = new KafkaBuilder("confluentinc/cp-kafka:7.5.0").WithReuse(true).WithLabel("npipeline-bench", "kafka").Build();
        await container.StartAsync();
        _bootstrap = container.GetBootstrapAddress();
    }

    protected override async Task<string> CreateDestinationAsync()
    {
        var topic = $"bench-{Guid.NewGuid():N}";
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrap }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
        return topic;
    }

    protected override Task PublishRawAsync(string destination, string[] bodies)
    {
        using var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = _bootstrap, LingerMs = 5 }).Build();

        foreach (var body in bodies)
        {
            producer.Produce(destination, new Message<Null, string> { Value = body });
        }

        _ = producer.Flush(TimeSpan.FromSeconds(30));
        return Task.CompletedTask;
    }

    protected override SinkNode<WideRecord> CreateSink(string destination) => KafkaConnector.Sink<WideRecord>(_bootstrap, destination);

    protected override SourceNode<object> CreateSource(string destination) =>
        new ObjectSource<Kafka.Models.KafkaMessage<WideRecord>>(KafkaConnector.Source<WideRecord>(_bootstrap, destination, $"{destination}-group",
            o => o with { AutoOffsetReset = AutoOffsetReset.Earliest }));
}

public class RabbitMqBenchmarks : MessagingConnectorBenchmark, IDisposable
{
    [GlobalCleanup]
    public void Dispose()
    {
        _connections?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        GC.SuppressFinalize(this);
    }

    private RabbitMqConnectionManager? _connections;

    private RabbitMqConnectionManager Connections => _connections ?? throw new InvalidOperationException("Not started.");

    protected override async Task StartAsync()
    {
        var container = new RabbitMqBuilder("rabbitmq:4-management-alpine").WithUsername("bench").WithPassword("bench").WithReuse(true)
            .WithLabel("npipeline-bench", "rabbitmq").Build();

        await container.StartAsync();

        _connections = new RabbitMqConnectionManager(
            new RabbitMqConnectionOptions { HostName = container.Hostname, Port = container.GetMappedPublicPort(5672), UserName = "bench", Password = "bench" },
            NullLogger<RabbitMqConnectionManager>.Instance);
    }

    protected override async Task<string> CreateDestinationAsync()
    {
        var queue = $"bench-{Guid.NewGuid():N}";
        await using var channel = await Connections.CreateChannelAsync();
        _ = await channel.QueueDeclareAsync(queue, true, false, false);
        return queue;
    }

    protected override async Task PublishRawAsync(string destination, string[] bodies)
    {
        await using var channel = await Connections.CreateChannelAsync();

        foreach (var body in bodies)
        {
            await channel.BasicPublishAsync(string.Empty, destination, false, new BasicProperties(), Encoding.UTF8.GetBytes(body));
        }
    }

    protected override SinkNode<WideRecord> CreateSink(string destination) => RabbitMqConnector.Sink<WideRecord>(Connections, string.Empty, destination);

    protected override SourceNode<object> CreateSource(string destination) =>
        new ObjectSource<RabbitMQ.Models.RabbitMqMessage<WideRecord>>(RabbitMqConnector.Source<WideRecord>(Connections, destination));
}

public class SqsBenchmarks : MessagingConnectorBenchmark, IDisposable
{
    [GlobalCleanup]
    public void Dispose()
    {
        _client?.Dispose();
        GC.SuppressFinalize(this);
    }

    private AmazonSQSClient? _client;

    private AmazonSQSClient Client => _client ?? throw new InvalidOperationException("Not started.");

    protected override async Task StartAsync()
    {
        var container = new FlociBuilder("floci/floci:2.1.0").WithReuse(true)
            .WithLabel("npipeline-bench", "sqs").Build();

        await container.StartAsync();
        _client = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = container.GetConnectionString() });
    }

    protected override async Task<string> CreateDestinationAsync() =>
        (await Client.CreateQueueAsync(new CreateQueueRequest($"bench-{Guid.NewGuid():N}"))).QueueUrl;

    protected override async Task PublishRawAsync(string destination, string[] bodies)
    {
        foreach (var chunk in bodies.Chunk(10))
        {
            _ = await Client.SendMessageBatchAsync(destination,
                [.. chunk.Select((body, i) => new SendMessageBatchRequestEntry(i.ToString(System.Globalization.CultureInfo.InvariantCulture), body))]);
        }
    }

    protected override SinkNode<WideRecord> CreateSink(string destination) => SqsConnector.Sink<WideRecord>(destination, o => o with { Client = Client });

    protected override SourceNode<object> CreateSource(string destination) =>
        new ObjectSource<Aws.Sqs.Models.SqsMessage<WideRecord>>(SqsConnector.Source<WideRecord>(destination,
            o => o with { Client = Client, WaitTime = TimeSpan.FromSeconds(1) }));
}

public class ServiceBusBenchmarks : MessagingConnectorBenchmark, IDisposable
{
    // 200, as in the phase 6 baseline, whose source settled one message at a time.
    protected override int Messages => 200;

    [GlobalCleanup]
    public void Dispose()
    {
        _client?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        GC.SuppressFinalize(this);
    }

    private ServiceBusAdministrationClient? _administration;
    private ServiceBusClient? _client;
    private string _connectionString = string.Empty;

    private ServiceBusClient Client => _client ?? throw new InvalidOperationException("Not started.");

    protected override async Task StartAsync()
    {
        var container = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest").WithAcceptLicenseAgreement(true)
            .WithLabel("npipeline-bench", "servicebus").Build();

        await container.StartAsync();
        _connectionString = container.GetConnectionString();
        _client = new ServiceBusClient(_connectionString);
        _administration = new ServiceBusAdministrationClient(container.GetHttpConnectionString());
    }

    protected override async Task<string> CreateDestinationAsync()
    {
        var queue = $"bench-{Guid.NewGuid():N}";
        _ = await _administration!.CreateQueueAsync(queue);
        return queue;
    }

    protected override async Task PublishRawAsync(string destination, string[] bodies)
    {
        await using var sender = Client.CreateSender(destination);
        var pending = new Queue<string>(bodies);

        while (pending.Count > 0)
        {
            using var batch = await sender.CreateMessageBatchAsync();

            while (pending.Count > 0 && batch.TryAddMessage(new ServiceBusMessage(pending.Peek())))
            {
                _ = pending.Dequeue();
            }

            await sender.SendMessagesAsync(batch);
        }
    }

    protected override SinkNode<WideRecord> CreateSink(string destination) => ServiceBusConnector.Sink<WideRecord>(Client, destination);

    protected override SourceNode<object> CreateSource(string destination) =>
        new ObjectSource<Azure.ServiceBus.Models.ServiceBusMessage<WideRecord>>(ServiceBusConnector.Source<WideRecord>(Client, destination));
}
