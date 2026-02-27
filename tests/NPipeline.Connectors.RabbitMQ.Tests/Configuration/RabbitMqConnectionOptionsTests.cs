using NPipeline.Connectors.RabbitMQ.Configuration;

namespace NPipeline.Connectors.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqConnectionOptionsTests
{
    [Fact]
    public void Default_Values_Are_Correct()
    {
        var options = new RabbitMqConnectionOptions();

        options.HostName.Should().Be("localhost");
        options.Port.Should().Be(5672);
        options.VirtualHost.Should().Be("/");
        options.UserName.Should().Be("guest");
        options.Password.Should().Be("guest");
        options.AutomaticRecoveryEnabled.Should().BeTrue();
        options.TopologyRecoveryEnabled.Should().BeTrue();
        options.MaxChannelPoolSize.Should().Be(4);
    }

    [Fact]
    public void Validate_Succeeds_With_Default_Values()
    {
        var options = new RabbitMqConnectionOptions();
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_Throws_When_HostName_Is_Empty()
    {
        var options = new RabbitMqConnectionOptions { HostName = "" };
        var act = () => options.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*HostName*");
    }

    [Fact]
    public void Validate_Throws_When_Port_Is_Zero()
    {
        var options = new RabbitMqConnectionOptions { Port = 0 };
        var act = () => options.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*Port*");
    }

    [Fact]
    public void Validate_Throws_When_MaxChannelPoolSize_Is_Zero()
    {
        var options = new RabbitMqConnectionOptions { MaxChannelPoolSize = 0 };
        var act = () => options.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*MaxChannelPoolSize*");
    }

    [Fact]
    public void Validate_Skips_HostName_Check_When_Uri_Is_Set()
    {
        var options = new RabbitMqConnectionOptions
        {
            Uri = new Uri("amqp://user:pass@myhost:5672/vhost"),
            HostName = "", // Should be ignored when Uri is set
        };

        var act = () => options.Validate();
        act.Should().NotThrow();
    }
}
