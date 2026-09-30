using FakeItEasy;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Reliability;
using NResilience;

namespace NPipeline.Connectors.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqWriteOptionsTests
{
    private static RabbitMqWriteOptions<string> Valid(string exchange = "ex") =>
        new() { Exchange = exchange, Connection = A.Fake<IRabbitMqConnectionManager>() };

    [Fact]
    public void Validate_Succeeds_With_Valid_Exchange()
    {
        var act = () => Valid("test-exchange").Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_Succeeds_With_Default_Exchange()
    {
        var act = () => Valid("").Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void Default_Values_Are_Correct()
    {
        var options = Valid();

        options.RoutingKey.Should().Be("");
        options.RoutingKeySelector.Should().BeNull();
        options.Mandatory.Should().BeFalse();
        options.PublisherConfirms.Should().BeTrue();
        options.Persistent.Should().BeTrue();
        options.Resilience.Should().BeSameAs(RabbitMqConnectorResilience.Default);
        options.FailedMessages.Should().Be(FailedMessageAction.Fail);
        options.ConfirmTimeout.Should().Be(TimeSpan.FromSeconds(5));
        options.BatchSize.Should().Be(100);
        options.BatchLinger.Should().Be(TimeSpan.FromMilliseconds(10));
        options.CopyMessageProperties.Should().BeTrue();
        options.Serializer.Should().BeSameAs(JsonMessageSerializer.Default);
        options.ContentType.Should().BeNull();
        options.Topology.Should().BeNull();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_Throws_When_ConfirmTimeout_Is_Not_Positive(int seconds)
    {
        var options = Valid() with { ConfirmTimeout = TimeSpan.FromSeconds(seconds) };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(RabbitMqWriteOptions<string>.ConfirmTimeout));
    }

    [Fact]
    public void Validate_Throws_When_BatchSize_Is_Zero()
    {
        var options = Valid() with { BatchSize = 0 };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(RabbitMqWriteOptions<string>.BatchSize));
    }

    [Fact]
    public void Validate_Throws_When_BatchLinger_Is_Negative()
    {
        var options = Valid() with { BatchLinger = TimeSpan.FromSeconds(-1) };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(RabbitMqWriteOptions<string>.BatchLinger));
    }

    [Fact]
    public void Validate_Accepts_Infinite_BatchLinger()
    {
        var options = Valid() with { BatchLinger = Timeout.InfiniteTimeSpan };
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_Throws_When_Resilience_Is_Invalid()
    {
#pragma warning disable NRES003 // The invalid value is the point of the test.
        var options = Valid() with { Resilience = RabbitMqConnectorResilience.Default with { Attempts = 0 } };
#pragma warning restore NRES003
        var act = () => options.Validate();
        act.Should().Throw<ResilienceConfigurationException>();
    }

    [Fact]
    public void Validate_Throws_When_Resilience_Is_Null()
    {
        var options = Valid() with { Resilience = null! };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Validate_Throws_When_Connection_Is_Null()
    {
        var options = Valid() with { Connection = null! };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentNullException>().WithParameterName(nameof(RabbitMqWriteOptions<string>.Connection));
    }

    [Fact]
    public void Sink_Factory_Validates_The_Configured_Options()
    {
        var act = () => RabbitMqConnector.Sink<string>(A.Fake<IRabbitMqConnectionManager>(), "ex", "rk", o => o with { BatchSize = 0 });
        act.Should().Throw<ArgumentOutOfRangeException>();
    }
}
