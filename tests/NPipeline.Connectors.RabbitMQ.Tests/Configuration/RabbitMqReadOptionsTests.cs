using FakeItEasy;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;

namespace NPipeline.Connectors.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqReadOptionsTests
{
    private static RabbitMqReadOptions Valid() => new() { Queue = "q", Connection = A.Fake<IRabbitMqConnectionManager>() };

    [Fact]
    public void Validate_Succeeds_With_Valid_Queue()
    {
        var act = () => Valid().Validate();
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Validate_Throws_When_Queue_Is_Blank(string queue)
    {
        var options = Valid() with { Queue = queue };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(RabbitMqReadOptions.Queue));
    }

    [Fact]
    public void Validate_Throws_When_Connection_Is_Null()
    {
        var options = Valid() with { Connection = null! };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentNullException>().WithParameterName(nameof(RabbitMqReadOptions.Connection));
    }

    [Fact]
    public void Validate_Throws_When_Serializer_Is_Null()
    {
        var options = Valid() with { Serializer = null! };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentNullException>().WithParameterName(nameof(RabbitMqReadOptions.Serializer));
    }

    [Fact]
    public void Default_Values_Are_Correct()
    {
        var options = Valid();

        options.PrefetchCount.Should().Be(100);
        options.ConsumerTag.Should().BeNull();
        options.Exclusive.Should().BeFalse();
        options.Topology.Should().BeNull();
        options.Serializer.Should().BeSameAs(JsonMessageSerializer.Default);
        options.RowErrorHandler.Should().BeNull();
        options.RawExcerptLength.Should().Be(256);
        options.MaxDeliveryAttempts.Should().BeNull();
        options.SettleTimeout.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void Validate_Throws_When_PrefetchCount_Is_Zero()
    {
        var options = Valid() with { PrefetchCount = 0 };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(RabbitMqReadOptions.PrefetchCount));
    }

    [Fact]
    public void Validate_Throws_When_RawExcerptLength_Is_Negative()
    {
        var options = Valid() with { RawExcerptLength = -1 };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(RabbitMqReadOptions.RawExcerptLength));
    }

    [Fact]
    public void Validate_Throws_When_MaxDeliveryAttempts_Is_Zero()
    {
        var options = Valid() with { MaxDeliveryAttempts = 0 };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(RabbitMqReadOptions.MaxDeliveryAttempts));
    }

    [Theory]
    [InlineData(null)]
    [InlineData(1)]
    [InlineData(5)]
    public void Validate_Succeeds_When_MaxDeliveryAttempts_Is_Unset_Or_Positive(int? attempts)
    {
        var options = Valid() with { MaxDeliveryAttempts = attempts };
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_Throws_When_SettleTimeout_Is_Negative()
    {
        var options = Valid() with { SettleTimeout = TimeSpan.FromSeconds(-1) };
        var act = () => options.Validate();
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(RabbitMqReadOptions.SettleTimeout));
    }

    [Fact]
    public void Source_Factory_Validates_The_Configured_Options()
    {
        var act = () => RabbitMqConnector.Source<string>(A.Fake<IRabbitMqConnectionManager>(), "q", o => o with { PrefetchCount = 0 });
        act.Should().Throw<ArgumentOutOfRangeException>();
    }
}
