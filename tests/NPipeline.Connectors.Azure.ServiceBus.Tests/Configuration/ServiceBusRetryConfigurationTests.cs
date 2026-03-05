using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Configuration;

public class ServiceBusRetryConfigurationTests
{
    public class Defaults
    {
        [Fact]
        public void NewRetryConfiguration_HasExpectedDefaults()
        {
            var retry = new ServiceBusRetryConfiguration();

            retry.Mode.Should().Be(ServiceBusRetryMode.Exponential);
            retry.MaxRetries.Should().Be(3);
            retry.Delay.Should().Be(TimeSpan.FromSeconds(1));
            retry.MaxDelay.Should().Be(TimeSpan.FromSeconds(30));
            retry.TryTimeout.Should().Be(TimeSpan.FromMinutes(1));
        }
    }

    public class ToRetryOptions
    {
        [Fact]
        public void ToRetryOptions_MapsExponentialMode()
        {
            var retry = new ServiceBusRetryConfiguration
            {
                Mode = ServiceBusRetryMode.Exponential,
                MaxRetries = 5,
                Delay = TimeSpan.FromSeconds(1),
                MaxDelay = TimeSpan.FromMinutes(2),
                TryTimeout = TimeSpan.FromMinutes(3),
            };

            var options = retry.ToRetryOptions();

            options.Mode.Should().Be(ServiceBusRetryMode.Exponential);
            options.MaxRetries.Should().Be(5);
            options.Delay.Should().Be(TimeSpan.FromSeconds(1));
            options.MaxDelay.Should().Be(TimeSpan.FromMinutes(2));
            options.TryTimeout.Should().Be(TimeSpan.FromMinutes(3));
        }

        [Fact]
        public void ToRetryOptions_MapsFixedMode()
        {
            var retry = new ServiceBusRetryConfiguration
            {
                Mode = ServiceBusRetryMode.Fixed,
                MaxRetries = 2,
                Delay = TimeSpan.FromSeconds(5),
                MaxDelay = TimeSpan.FromSeconds(30),
                TryTimeout = TimeSpan.FromSeconds(60),
            };

            var options = retry.ToRetryOptions();

            options.Mode.Should().Be(ServiceBusRetryMode.Fixed);
            options.MaxRetries.Should().Be(2);
        }

        [Fact]
        public void ToRetryOptions_ReturnsNewInstanceEachCall()
        {
            var retry = new ServiceBusRetryConfiguration();
            var options1 = retry.ToRetryOptions();
            var options2 = retry.ToRetryOptions();

            options1.Should().NotBeSameAs(options2);
        }
    }
}
