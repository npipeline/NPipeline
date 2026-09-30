using System.Text.Json;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.Messaging.RoundTrip.Tests.Harnesses;
using NPipeline.Configuration;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Messaging.RoundTrip.Tests;

/// <summary>
///     The scenarios every message-queue connector must pass, through a real broker. Each broker's test class runs them
///     all and overrides the ones a known bug breaks with <c>[KnownBugFact]</c>.
/// </summary>
public abstract class MessagingRoundTripTests(MessagingHarness harness) : IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);
    private static readonly JsonSerializerOptions WebJson = new(JsonSerializerDefaults.Web);

    protected MessagingHarness Harness { get; } = harness;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await Harness.DisposeAsync();

    /// <summary>How long to watch for a message that should not come back: longer than a redelivery.</summary>
    private TimeSpan Quiet => Harness.RedeliveryDelay + TimeSpan.FromSeconds(3);

    [Fact]
    public virtual async Task Records_round_trip_with_default_settings()
    {
        var queue = await Harness.CreateDestinationAsync();
        var orders = Enumerable.Range(1, 5).Select(Order.Create).ToList();

        await WriteAsync(queue, orders);
        var read = await Harness.ReadAsync<Order>(queue, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(5, Wait, m => m.AcknowledgeAsync());

        read.Select(m => m.Body).Should().BeEquivalentTo(orders);
    }

    [Fact]
    public virtual async Task Sinks_write_web_style_json()
    {
        var queue = await Harness.CreateDestinationAsync();

        await WriteAsync(queue, [Order.Create(1)]);
        var body = (await Harness.ReceiveRawAsync(queue, 1, Wait)).Should().ContainSingle().Subject;

        using var json = JsonDocument.Parse(body);
        json.RootElement.EnumerateObject().Select(p => p.Name).Should().Equal("id", "customerName", "total", "placedAt");
    }

    [Fact]
    public virtual async Task Sources_read_camel_and_pascal_case_json()
    {
        var queue = await Harness.CreateDestinationAsync();

        await Harness.PublishRawAsync(queue,
            """{"id":1,"customerName":"camel","total":1.5,"placedAt":"2026-09-30T10:00:00+10:00"}""",
            """{"Id":2,"CustomerName":"Pascal","Total":2.5,"PlacedAt":"2026-09-30T10:00:00+10:00"}""");

        var read = await Harness.ReadAsync<Order>(queue, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(2, Wait, m => m.AcknowledgeAsync());

        read.Select(m => m.Body.CustomerName).Should().Equal("camel", "Pascal");
    }

    [Fact]
    public virtual async Task Acknowledged_messages_are_not_delivered_again()
    {
        var queue = await Harness.CreateDestinationAsync();
        await WriteAsync(queue, Enumerable.Range(1, 3).Select(Order.Create).ToList());

        (await Harness.ReadAsync<Order>(queue, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(3, Wait, m => m.AcknowledgeAsync())).Should().HaveCount(3);

        var again = await Harness.ReadAsync<Order>(queue, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(1, Quiet, m => m.AcknowledgeAsync());

        again.Select(m => m.Body.Id).Should().BeEmpty();
    }

    [Fact]
    public virtual async Task Unacknowledged_messages_are_delivered_again()
    {
        var queue = await Harness.CreateDestinationAsync();
        await WriteAsync(queue, [Order.Create(1)]);

        (await Harness.ReadAsync<Order>(queue, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(1, Wait)).Should().ContainSingle();

        // The pipeline that read it has ended without settling it.
        await Harness.DisposeNodesAsync();
        await Task.Delay(Harness.RedeliveryDelay);

        var again = await Harness.ReadAsync<Order>(queue, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(1, Wait, m => m.AcknowledgeAsync());

        again.Select(m => m.Body.Id).Should().Equal(1);
    }

    [Fact]
    public virtual async Task A_source_feeds_a_sink_with_default_settings_and_is_acknowledged()
    {
        var from = await Harness.CreateDestinationAsync();
        var to = await Harness.CreateDestinationAsync();
        await WriteAsync(from, Enumerable.Range(1, 5).Select(Order.Create).ToList());

        using (var cts = new CancellationTokenSource(Wait))
        {
            var messages = Harness.ReadAsync<Order>(from, ReadSettings.Defaults, new PipelineContext(), cts.Token).Limit(5, cts.Token);
            var write = () => Harness.WriteMessagesAsync(to, messages, new PipelineContext(), cts.Token);

            await write.Should().NotThrowAsync("the pipeline must finish, not hang");
        }

        (await Harness.ReceiveRawAsync(to, 5, Wait)).Should().HaveCount(5);

        var left = await Harness.ReadAsync<Order>(from, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(1, Quiet, m => m.AcknowledgeAsync());

        left.Should().BeEmpty("the sink acknowledged every message it wrote");
    }

    [Fact]
    public virtual async Task A_message_written_by_another_sink_is_acknowledged()
    {
        var queue = await Harness.CreateDestinationAsync();
        await WriteAsync(queue, Enumerable.Range(1, 3).Select(Order.Create).ToList());
        var sink = new CollectingSink<Order>();

        using (var cts = new CancellationTokenSource(Wait))
        {
            var messages = Harness.ReadAsync<Order>(queue, ReadSettings.Defaults, new PipelineContext(), cts.Token).Limit(3, cts.Token);
            await sink.Acknowledging().ConsumeAsync(MessagingHarness.Stream(messages), new PipelineContext(), cts.Token);
        }

        sink.Items.Select(o => o.Id).Should().Equal(1, 2, 3);

        var left = await Harness.ReadAsync<Order>(queue, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(1, Quiet, m => m.AcknowledgeAsync());

        left.Should().BeEmpty("the sink wrote every message, so each was acknowledged");
    }

    [Fact]
    public virtual async Task A_message_that_does_not_deserialize_fails_the_read_by_default()
    {
        var queue = await Harness.CreateDestinationAsync();
        await Harness.PublishRawAsync(queue, "not json");

        var read = () => Harness.ReadAsync<Order>(queue, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(1, Wait, m => m.AcknowledgeAsync());

        await read.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public virtual async Task A_skipped_message_is_not_delivered_again()
    {
        var queue = await Harness.CreateDestinationAsync();
        await Harness.PublishRawAsync(queue, "not json", JsonSerializer.Serialize(Order.Create(2), WebJson));
        var settings = new ReadSettings { OnUndeserializable = Undeserializable.Skip };

        var read = await Harness.ReadAsync<Order>(queue, settings, new PipelineContext(), CancellationToken.None)
            .TakeAsync(1, Wait, m => m.AcknowledgeAsync());

        read.Select(m => m.Body.Id).Should().Equal(2);

        var again = await Harness.ReadAsync<Order>(queue, settings, new PipelineContext(), CancellationToken.None)
            .TakeAsync(1, Quiet, m => m.AcknowledgeAsync());

        again.Should().BeEmpty();

        if (Harness.RemovesSettledMessages)
            (await Harness.ReceiveRawAsync(queue, 1, TimeSpan.FromSeconds(2))).Should().BeEmpty("the skipped message was settled");
    }

    [Fact]
    public virtual async Task A_dead_lettered_message_reaches_the_dead_letter_sink()
    {
        var queue = await Harness.CreateDestinationAsync();
        await Harness.PublishRawAsync(queue, "not json", JsonSerializer.Serialize(Order.Create(2), WebJson));
        var deadLetters = new CapturingDeadLetterSink();
        var context = new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: deadLetters));

        var read = await Harness.ReadAsync<Order>(queue, new ReadSettings { OnUndeserializable = Undeserializable.DeadLetter }, context, CancellationToken.None)
            .TakeAsync(1, Wait, m => m.AcknowledgeAsync());

        read.Select(m => m.Body.Id).Should().Equal(2);
        deadLetters.Captured.Should().ContainSingle();

        var again = await Harness.ReadAsync<Order>(queue, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(1, Quiet, m => m.AcknowledgeAsync());

        again.Should().BeEmpty("the dead-lettered message was settled");
    }

    private async Task WriteAsync(string destination, IReadOnlyList<Order> orders)
    {
        using var cts = new CancellationTokenSource(Wait);
        await Harness.WriteAsync(destination, orders.ToAsync(), new PipelineContext(), cts.Token);
    }
}
