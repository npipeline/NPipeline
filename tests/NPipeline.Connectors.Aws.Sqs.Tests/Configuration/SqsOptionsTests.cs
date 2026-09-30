using Amazon.Runtime;
using Amazon.SQS.Model;
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Messaging;
using static NPipeline.Connectors.Aws.Sqs.Tests.SqsTestSupport;

namespace NPipeline.Connectors.Aws.Sqs.Tests.Configuration;

public sealed class SqsOptionsTests
{
    private static readonly SqsReadOptions Read = new() { QueueUrl = QueueUrl };
    private static readonly SqsWriteOptions<Order> Write = new() { QueueUrl = QueueUrl };

    private static readonly Dictionary<int, (string Parameter, SqsReadOptions Options)> InvalidReadOptionsCases = new()
    {
        [0] = (nameof(SqsReadOptions.MaxMessages), Read with { MaxMessages = 0 }),
        [1] = (nameof(SqsReadOptions.MaxMessages), Read with { MaxMessages = 11 }),
        [2] = (nameof(SqsReadOptions.WaitTime), Read with { WaitTime = TimeSpan.FromSeconds(-1) }),
        [3] = (nameof(SqsReadOptions.WaitTime), Read with { WaitTime = TimeSpan.FromSeconds(21) }),
        [4] = (nameof(SqsReadOptions.VisibilityTimeout), Read with { VisibilityTimeout = TimeSpan.FromSeconds(-1) }),
        [5] = (nameof(SqsReadOptions.VisibilityTimeout), Read with { VisibilityTimeout = TimeSpan.FromHours(12) + TimeSpan.FromSeconds(1) }),
        [6] = (nameof(SqsReadOptions.RawExcerptLength), Read with { RawExcerptLength = -1 }),
        [7] = (nameof(SqsReadOptions.DeleteLinger), Read with { DeleteLinger = TimeSpan.FromMilliseconds(-1) }),
        [8] = (nameof(SqsReadOptions.SettleTimeout), Read with { SettleTimeout = TimeSpan.FromMilliseconds(-1) }),
        [9] = (nameof(SqsReadOptions.MaxErrorRetry), Read with { MaxErrorRetry = -1 }),
    };

    public static TheoryData<int> InvalidReadOptions => [.. InvalidReadOptionsCases.Keys];

    private static readonly Dictionary<int, (string Parameter, SqsWriteOptions<Order> Options)> InvalidWriteOptionsCases = new()
    {
        [0] = (nameof(SqsWriteOptions<Order>.BatchSize), Write with { BatchSize = 0 }),
        [1] = (nameof(SqsWriteOptions<Order>.BatchSize), Write with { BatchSize = 11 }),
        [2] = (nameof(SqsWriteOptions<Order>.BatchLinger), Write with { BatchLinger = TimeSpan.FromMilliseconds(-2) }),
        [3] = (nameof(SqsWriteOptions<Order>.Delay), Write with { Delay = TimeSpan.FromSeconds(-1) }),
        [4] = (nameof(SqsWriteOptions<Order>.Delay), Write with { Delay = TimeSpan.FromMinutes(15) + TimeSpan.FromSeconds(1) }),
        [5] = (nameof(SqsWriteOptions<Order>.MaxErrorRetry), Write with { MaxErrorRetry = -1 }),
    };

    public static TheoryData<int> InvalidWriteOptions => [.. InvalidWriteOptionsCases.Keys];

    private static readonly Dictionary<int, SqsReadOptions> ValidReadOptionsCases = new()
    {
        [0] = Read,
        [1] = Read with { MaxMessages = 1, WaitTime = TimeSpan.Zero },
        [2] = Read with { MaxMessages = 10, WaitTime = TimeSpan.FromSeconds(20) },
        [3] = Read with { VisibilityTimeout = TimeSpan.Zero },
        [4] = Read with { VisibilityTimeout = TimeSpan.FromHours(12) },
        [5] = Read with { RawExcerptLength = 0, DeleteLinger = TimeSpan.Zero, SettleTimeout = TimeSpan.Zero },
        [6] = Read with { MaxErrorRetry = 0 },
        [7] = Read with { RetryMode = null, MaxErrorRetry = null },
        [8] = Read with { Credentials = new BasicAWSCredentials("key", "secret"), Region = "ap-southeast-2" },
        [9] = Read with { ProfileName = "dev", ServiceUrl = "http://localhost:4566" },
    };

    public static TheoryData<int> ValidReadOptions => [.. ValidReadOptionsCases.Keys];

    private static readonly Dictionary<int, SqsWriteOptions<Order>> ValidWriteOptionsCases = new()
    {
        [0] = Write,
        [1] = Write with { BatchSize = 1 },
        [2] = Write with { BatchSize = 10 },
        [3] = Write with { BatchLinger = TimeSpan.Zero },
        [4] = Write with { BatchLinger = Timeout.InfiniteTimeSpan },
        [5] = Write with { Delay = TimeSpan.FromMinutes(15) },
        [6] = Write with { MessageGroupId = order => order.Name, DeduplicationId = order => $"{order.Id}" },
    };

    public static TheoryData<int> ValidWriteOptions => [.. ValidWriteOptionsCases.Keys];

    [Fact]
    public void ReadOptions_HaveTheDocumentedDefaults()
    {
        Read.Client.Should().BeNull();
        Read.Region.Should().BeNull();
        Read.ServiceUrl.Should().BeNull();
        Read.ProfileName.Should().BeNull();
        Read.Credentials.Should().BeNull();
        Read.RetryMode.Should().Be(RequestRetryMode.Standard);
        Read.MaxErrorRetry.Should().Be(3);
        Read.Serializer.Should().BeSameAs(JsonMessageSerializer.Default);
        Read.MaxMessages.Should().Be(10);
        Read.WaitTime.Should().Be(TimeSpan.FromSeconds(20));
        Read.VisibilityTimeout.Should().BeNull();
        Read.RowErrorHandler.Should().BeNull();
        Read.RawExcerptLength.Should().Be(256);
        Read.DeleteLinger.Should().Be(TimeSpan.FromMilliseconds(50));
        Read.SettleTimeout.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void WriteOptions_HaveTheDocumentedDefaults()
    {
        Write.Client.Should().BeNull();
        Write.Serializer.Should().BeSameAs(JsonMessageSerializer.Default);
        Write.BatchSize.Should().Be(10);
        Write.BatchLinger.Should().Be(TimeSpan.FromMilliseconds(10));
        Write.Delay.Should().Be(TimeSpan.Zero);
        Write.MessageAttributes.Should().BeEmpty();
        Write.CopyMessageAttributes.Should().BeTrue();
        Write.MessageGroupId.Should().BeNull();
        Write.DeduplicationId.Should().BeNull();
        Write.FailedMessages.Should().Be(FailedMessageAction.Fail);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Validate_RejectsAMissingQueueUrl(string queueUrl)
    {
        var read = () => (Read with { QueueUrl = queueUrl }).Validate();
        var write = () => (Write with { QueueUrl = queueUrl }).Validate();

        read.Should().Throw<ArgumentException>().WithParameterName(nameof(SqsNodeOptions.QueueUrl));
        write.Should().Throw<ArgumentException>().WithParameterName(nameof(SqsNodeOptions.QueueUrl));
    }

    [Fact]
    public void Validate_RejectsANullSerializer()
    {
        var act = () => (Read with { Serializer = null! }).Validate();

        act.Should().Throw<ArgumentNullException>().WithParameterName(nameof(SqsNodeOptions.Serializer));
    }

    [Fact]
    public void Validate_RejectsNullMessageAttributes()
    {
        var act = () => (Write with { MessageAttributes = null! }).Validate();

        act.Should().Throw<ArgumentNullException>().WithParameterName(nameof(SqsWriteOptions<Order>.MessageAttributes));
    }

    [Theory]
    [MemberData(nameof(InvalidReadOptions))]
    public void ReadOptions_Validate_RejectsOutOfRangeSettings(int index)
    {
        var (parameter, options) = InvalidReadOptionsCases[index];
        var act = options.Validate;

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(parameter);
    }

    [Theory]
    [MemberData(nameof(InvalidWriteOptions))]
    public void WriteOptions_Validate_RejectsOutOfRangeSettings(int index)
    {
        var (parameter, options) = InvalidWriteOptionsCases[index];
        var act = options.Validate;

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(parameter);
    }

    [Theory]
    [MemberData(nameof(ValidReadOptions))]
    public void ReadOptions_Validate_AcceptsSettingsInRange(int index)
    {
        var act = ValidReadOptionsCases[index].Validate;

        act.Should().NotThrow();
    }

    [Theory]
    [MemberData(nameof(ValidWriteOptions))]
    public void WriteOptions_Validate_AcceptsSettingsInRange(int index)
    {
        var act = ValidWriteOptionsCases[index].Validate;

        act.Should().NotThrow();
    }

    [Fact]
    public void WriteOptions_MessageAttributes_CanBeSet()
    {
        var attributes = new Dictionary<string, MessageAttributeValue> { ["source"] = new() { DataType = "String", StringValue = "tests" } };

        (Write with { MessageAttributes = attributes }).MessageAttributes.Should().ContainKey("source");
    }

    [Fact]
    public void Connector_AppliesTheConfigurationToTheOptions()
    {
        var source = () => SqsConnector.Source<Order>(QueueUrl, o => o with { MaxMessages = 0 });
        var sink = () => SqsConnector.Sink<Order>(QueueUrl, o => o with { BatchSize = 0 });

        source.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(SqsReadOptions.MaxMessages));
        sink.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(SqsWriteOptions<Order>.BatchSize));
    }
}
