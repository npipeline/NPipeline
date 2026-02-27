using NPipeline.Connectors.RabbitMQ.Configuration;

namespace NPipeline.Connectors.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqSinkOptionsTests
{
    [Fact]
    public void Validate_Succeeds_With_Valid_ExchangeName()
    {
        var options = new RabbitMqSinkOptions { ExchangeName = "test-exchange" };
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_Succeeds_With_Default_Exchange()
    {
        var options = new RabbitMqSinkOptions { ExchangeName = "" };
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void Default_Values_Are_Correct()
    {
        var options = new RabbitMqSinkOptions { ExchangeName = "ex" };

        options.RoutingKey.Should().Be("");
        options.Mandatory.Should().BeFalse();
        options.EnablePublisherConfirms.Should().BeTrue();
        options.Persistent.Should().BeTrue();
        options.MaxRetries.Should().Be(3);
        options.RetryBaseDelayMs.Should().Be(100);
        options.ContinueOnError.Should().BeFalse();
        options.ConfirmTimeout.Should().Be(TimeSpan.FromSeconds(5));
        options.ShutdownFlushTimeout.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void Validate_Throws_When_ConfirmTimeout_Is_Negative()
    {
        var options = new RabbitMqSinkOptions { ExchangeName = "ex", ConfirmTimeout = TimeSpan.FromSeconds(-1) };
        var act = () => options.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*ConfirmTimeout*");
    }

    [Fact]
    public void Validate_Throws_When_MaxRetries_Is_Negative()
    {
        var options = new RabbitMqSinkOptions { ExchangeName = "ex", MaxRetries = -1 };
        var act = () => options.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*MaxRetries*");
    }
}
