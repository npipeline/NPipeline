using Confluent.Kafka;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.Connectors.Kafka.Reliability;
using NPipeline.Connectors.Messaging;
using NResilience;

namespace NPipeline.Connectors.Kafka.Tests.Configuration;

/// <summary>
///     Unit tests for <see cref="KafkaReadOptions" />, <see cref="KafkaWriteOptions{T}" /> and the validation the nodes run.
/// </summary>
public sealed class KafkaOptionsTests
{
    private static KafkaReadOptions Read => new() { BootstrapServers = "localhost:9092", Topic = "orders", GroupId = "billing" };

    private static KafkaWriteOptions<string> Write => new() { BootstrapServers = "localhost:9092", Topic = "invoices" };

    // Defaults

    [Fact]
    public void ReadOptions_HaveTheDocumentedDefaults()
    {
        var options = Read;

        options.AutoOffsetReset.Should().Be(AutoOffsetReset.Latest);
        options.IsolationLevel.Should().BeNull();
        options.CommitInterval.Should().Be(TimeSpan.FromSeconds(5));
        options.RowErrorHandler.Should().BeNull();
        options.RawExcerptLength.Should().Be(256);
        options.SkipTombstones.Should().BeTrue();
        options.PollTimeout.Should().Be(TimeSpan.FromMilliseconds(100));
        options.SettleTimeout.Should().Be(TimeSpan.FromSeconds(30));
        options.Resilience.Should().BeSameAs(KafkaConnectorResilience.Default);
        options.Serializer.Should().BeSameAs(JsonMessageSerializer.Default);
        options.ClientSettings.Should().BeEmpty();
        options.GroupInstanceId.Should().BeNull();
    }

    [Fact]
    public void WriteOptions_HaveTheDocumentedDefaults()
    {
        var options = Write;

        options.KeySelector.Should().BeNull();
        options.CopyHeaders.Should().BeFalse();
        options.Acks.Should().Be(Acks.All);
        options.EnableIdempotence.Should().BeTrue();
        options.Linger.Should().Be(TimeSpan.FromMilliseconds(5));
        options.Compression.Should().BeNull();
        options.DeliveryTimeout.Should().Be(TimeSpan.FromMinutes(2));
        options.BatchSize.Should().Be(1000);
        options.BatchLinger.Should().Be(TimeSpan.FromMilliseconds(10));
        options.FailedMessages.Should().Be(FailedMessageAction.Fail);
        options.TransactionalId.Should().BeNull();
        options.TransactionTimeout.Should().Be(TimeSpan.FromSeconds(30));
        options.Serializer.Should().BeSameAs(JsonMessageSerializer.Default);
    }

    [Fact]
    public void ValidOptions_PassValidation()
    {
        Read.Invoking(o => o.Validate()).Should().NotThrow();
        Write.Invoking(o => o.Validate()).Should().NotThrow();
    }

    // Read options

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    public void ReadOptions_WithoutBootstrapServers_AreInvalid(string? servers)
    {
        var options = Read with { BootstrapServers = servers! };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentException>().WithParameterName("BootstrapServers");
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void ReadOptions_WithoutTopic_AreInvalid(string? topic)
    {
        var options = Read with { Topic = topic! };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentException>().WithParameterName("Topic");
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void ReadOptions_WithoutGroupId_AreInvalid(string? groupId)
    {
        var options = Read with { GroupId = groupId! };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentException>().WithParameterName("GroupId");
    }

    [Fact]
    public void ReadOptions_WithAZeroCommitInterval_AreInvalid()
    {
        var options = Read with { CommitInterval = TimeSpan.Zero };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>().WithParameterName("CommitInterval");
    }

    [Fact]
    public void ReadOptions_WithAZeroPollTimeout_AreInvalid()
    {
        var options = Read with { PollTimeout = TimeSpan.Zero };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>().WithParameterName("PollTimeout");
    }

    [Fact]
    public void ReadOptions_WithANegativeSettleTimeout_AreInvalid()
    {
        (Read with { SettleTimeout = TimeSpan.FromSeconds(-1) }).Invoking(o => o.Validate())
            .Should().Throw<ArgumentOutOfRangeException>().WithParameterName("SettleTimeout");

        (Read with { SettleTimeout = TimeSpan.Zero }).Invoking(o => o.Validate()).Should().NotThrow("zero closes the consumer at once");
    }

    [Fact]
    public void ReadOptions_WithANegativeRawExcerptLength_AreInvalid()
    {
        var options = Read with { RawExcerptLength = -1 };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>().WithParameterName("RawExcerptLength");
    }

    [Fact]
    public void ReadOptions_WithInvalidResilience_AreInvalid()
    {
#pragma warning disable NRES003 // The invalid value is the point of the test.
        var options = Read with { Resilience = KafkaConnectorResilience.Default with { Attempts = 0 } };
#pragma warning restore NRES003

        options.Invoking(o => o.Validate()).Should().Throw<ResilienceConfigurationException>();
    }

    // SASL

    [Theory]
    [InlineData(SecurityProtocol.SaslPlaintext, null)]
    [InlineData(SecurityProtocol.SaslSsl, SaslMechanism.Plain)]
    [InlineData(SecurityProtocol.SaslSsl, SaslMechanism.ScramSha256)]
    [InlineData(SecurityProtocol.SaslPlaintext, SaslMechanism.ScramSha512)]
    public void Sasl_WithAUserNameMechanism_NeedsCredentials(SecurityProtocol protocol, SaslMechanism? mechanism)
    {
        var withoutCredentials = Read with { SecurityProtocol = protocol, SaslMechanism = mechanism };
        var withoutPassword = withoutCredentials with { SaslUsername = "user" };
        var withCredentials = withoutPassword with { SaslPassword = "secret" };

        withoutCredentials.Invoking(o => o.Validate()).Should().Throw<ArgumentException>().WithParameterName("SaslUsername");
        withoutPassword.Invoking(o => o.Validate()).Should().Throw<ArgumentException>().WithParameterName("SaslUsername");
        withCredentials.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Theory]
    [InlineData(SaslMechanism.Gssapi)]
    [InlineData(SaslMechanism.OAuthBearer)]
    public void Sasl_WithAMechanismThatHasNoPassword_NeedsNoCredentials(SaslMechanism mechanism)
    {
        var options = Write with { SecurityProtocol = SecurityProtocol.SaslSsl, SaslMechanism = mechanism };

        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void Credentials_WithoutASaslProtocol_AreNotRequired()
    {
        var options = Read with { SecurityProtocol = SecurityProtocol.Ssl, SaslMechanism = SaslMechanism.Plain };

        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    // Write options

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void WriteOptions_WithoutTopic_AreInvalid(string? topic)
    {
        var options = Write with { Topic = topic! };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentException>().WithParameterName("Topic");
    }

    [Fact]
    public void WriteOptions_WithoutASerializer_AreInvalid()
    {
        var options = Write with { Serializer = null! };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentNullException>().WithParameterName("Serializer");
    }

    [Fact]
    public void WriteOptions_WithABatchSizeBelowOne_AreInvalid()
    {
        var options = Write with { BatchSize = 0 };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>().WithParameterName("BatchSize");
    }

    [Fact]
    public void WriteOptions_WithANegativeBatchLinger_AreInvalid_UnlessInfinite()
    {
        (Write with { BatchLinger = TimeSpan.FromMilliseconds(-5) }).Invoking(o => o.Validate())
            .Should().Throw<ArgumentOutOfRangeException>().WithParameterName("BatchLinger");

        (Write with { BatchLinger = Timeout.InfiniteTimeSpan }).Invoking(o => o.Validate()).Should().NotThrow();
        (Write with { BatchLinger = TimeSpan.Zero }).Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void WriteOptions_WithANegativeLinger_AreInvalid()
    {
        var options = Write with { Linger = TimeSpan.FromMilliseconds(-1) };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>().WithParameterName("Linger");
    }

    [Fact]
    public void WriteOptions_WithAZeroDeliveryTimeout_AreInvalid()
    {
        var options = Write with { DeliveryTimeout = TimeSpan.Zero };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>().WithParameterName("DeliveryTimeout");
    }

    [Fact]
    public void WriteOptions_WithAZeroTransactionTimeout_AreInvalid()
    {
        var options = Write with { TransactionTimeout = TimeSpan.Zero };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>().WithParameterName("TransactionTimeout");
    }

    [Theory]
    [InlineData(FailedMessageAction.Requeue)]
    [InlineData(FailedMessageAction.DeadLetter)]
    public void TransactionalWrites_NeedFailedMessagesToFail(FailedMessageAction action)
    {
        var options = Write with { TransactionalId = "tx-1", FailedMessages = action };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentException>().WithParameterName("FailedMessages");
        (options with { FailedMessages = FailedMessageAction.Fail }).Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void TransactionalWrites_NeedIdempotence()
    {
        var options = Write with { TransactionalId = "tx-1", EnableIdempotence = false };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentException>().WithParameterName("EnableIdempotence");
    }

    [Fact]
    public void NonTransactionalWrites_MayRequeueOrDeadLetter_WithoutIdempotence()
    {
        (Write with { FailedMessages = FailedMessageAction.Requeue, EnableIdempotence = false }).Invoking(o => o.Validate()).Should().NotThrow();
        (Write with { FailedMessages = FailedMessageAction.DeadLetter }).Invoking(o => o.Validate()).Should().NotThrow();
    }

    // Client settings

    [Fact]
    public void Apply_CopiesTheClientSettings_WithClientSettingsLast()
    {
        var options = Read with
        {
            ClientId = "npipeline",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.ScramSha256,
            SaslUsername = "user",
            SaslPassword = "secret",
            ClientSettings = new Dictionary<string, string> { ["client.id"] = "overridden", ["ssl.ca.location"] = "/ca.pem" },
        };

        var config = new ClientConfig();
        options.Apply(config);

        config.BootstrapServers.Should().Be("localhost:9092");
        config.ClientId.Should().Be("overridden");
        config.SecurityProtocol.Should().Be(SecurityProtocol.SaslSsl);
        config.SaslMechanism.Should().Be(SaslMechanism.ScramSha256);
        config.SaslUsername.Should().Be("user");
        config.SaslPassword.Should().Be("secret");
        config.SslCaLocation.Should().Be("/ca.pem");
    }

    [Fact]
    public void Apply_LeavesUnsetSecuritySettingsToLibrdkafka()
    {
        var config = new ClientConfig();
        Write.Apply(config);

        config.SecurityProtocol.Should().BeNull();
        config.SaslMechanism.Should().BeNull();
        config.SaslUsername.Should().BeNull();
        config.SaslPassword.Should().BeNull();
    }

    // Nodes and factories

    [Fact]
    public void SourceNode_ValidatesItsOptions()
    {
        var act = () => new KafkaSourceNode<string>(Read with { GroupId = "" });

        act.Should().Throw<ArgumentException>().WithParameterName("GroupId");
    }

    [Fact]
    public void SinkNode_ValidatesItsOptions()
    {
        var act = () => new KafkaSinkNode<string>(Write with { TransactionalId = "tx-1", FailedMessages = FailedMessageAction.Requeue });

        act.Should().Throw<ArgumentException>().WithParameterName("FailedMessages");
    }

    [Fact]
    public void NodeConstructors_RejectNullOptions()
    {
        ((Action)(() => _ = new KafkaSourceNode<string>(null!))).Should().Throw<ArgumentNullException>();
        ((Action)(() => _ = new KafkaSinkNode<string>(null!))).Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public async Task ConnectorSource_AppliesConfigure_ThenValidates()
    {
        var configured = new List<KafkaReadOptions>();

        await using var source = KafkaConnector.Source<string>("localhost:9092", "orders", "billing", o =>
        {
            configured.Add(o);
            return o with { AutoOffsetReset = AutoOffsetReset.Earliest };
        });

        var options = configured.Should().ContainSingle().Subject;
        (options.BootstrapServers, options.Topic, options.GroupId).Should().Be(("localhost:9092", "orders", "billing"));

        var invalid = () => KafkaConnector.Source<string>("localhost:9092", "orders", "billing", o => o with { PollTimeout = TimeSpan.Zero });
        invalid.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("PollTimeout");
    }

    [Fact]
    public async Task ConnectorSink_AppliesConfigure_ThenValidates()
    {
        KafkaWriteOptions<string>? configured = null;

        await using var sink = KafkaConnector.Sink<string>("localhost:9092", "invoices", o =>
        {
            configured = o;
            return o with { BatchSize = 10 };
        });

        configured.Should().NotBeNull();
        (configured!.BootstrapServers, configured.Topic).Should().Be(("localhost:9092", "invoices"));

        var invalid = () => KafkaConnector.Sink<string>("localhost:9092", "invoices", o => o with { BatchSize = 0 });
        invalid.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("BatchSize");
    }
}
