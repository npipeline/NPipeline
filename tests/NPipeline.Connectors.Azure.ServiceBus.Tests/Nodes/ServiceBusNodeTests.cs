using Azure.Messaging.ServiceBus;
using FakeItEasy;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Nodes;

public class ServiceBusQueueSourceNodeTests
{
    private static ServiceBusConfiguration CreateValidConfig()
    {
        return new ServiceBusConfiguration
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=abc=",
            QueueName = "test-queue",
        };
    }

    public class Constructor_WithConfiguration
    {
        [Fact]
        public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new ServiceBusQueueSourceNode<TestModel>(null!, A.Fake<ILogger>()));
        }

        [Fact]
        public void Constructor_WithInvalidConfiguration_ThrowsInvalidOperationException()
        {
            var config = new ServiceBusConfiguration(); // Missing connection string and queue

            Assert.Throws<InvalidOperationException>(() =>
                new ServiceBusQueueSourceNode<TestModel>(config));
        }

        [Fact]
        public void Constructor_WithMissingQueueName_ThrowsInvalidOperationException()
        {
            var config = new ServiceBusConfiguration
            {
                ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=abc=",
            };

            Assert.Throws<InvalidOperationException>(() =>
                new ServiceBusQueueSourceNode<TestModel>(config));
        }
    }

    public class Constructor_WithClient
    {
        [Fact]
        public void Constructor_WithNullClient_ThrowsArgumentNullException()
        {
            var config = CreateValidConfig();

            Assert.Throws<ArgumentNullException>(() =>
                new ServiceBusQueueSourceNode<TestModel>(null!, config));
        }

        [Fact]
        public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
        {
            var client = A.Fake<ServiceBusClient>();

            Assert.Throws<ArgumentNullException>(() =>
                new ServiceBusQueueSourceNode<TestModel>(client, null!));
        }
    }

    private class TestModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

public class ServiceBusSubscriptionSourceNodeTests
{
    private static ServiceBusConfiguration CreateValidConfig()
    {
        return new ServiceBusConfiguration
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=abc=",
            TopicName = "test-topic",
            SubscriptionName = "test-subscription",
        };
    }

    public class Constructor
    {
        [Fact]
        public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new ServiceBusSubscriptionSourceNode<TestModel>(null!, A.Fake<ILogger>()));
        }

        [Fact]
        public void Constructor_WithMissingSubscriptionName_ThrowsInvalidOperationException()
        {
            var config = new ServiceBusConfiguration
            {
                ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=abc=",
                TopicName = "test-topic",
            };

            Assert.Throws<InvalidOperationException>(() =>
                new ServiceBusSubscriptionSourceNode<TestModel>(config));
        }

        [Fact]
        public void Constructor_WithNullClientAndValidConfig_ThrowsArgumentNullException()
        {
            var config = CreateValidConfig();

            Assert.Throws<ArgumentNullException>(() =>
                new ServiceBusSubscriptionSourceNode<TestModel>(null!, config));
        }
    }

    private class TestModel
    {
        public int Id { get; set; }
    }
}

public class ServiceBusQueueSinkNodeTests
{
    private static ServiceBusConfiguration CreateValidConfig()
    {
        return new ServiceBusConfiguration
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=abc=",
            QueueName = "test-queue",
        };
    }

    public class Constructor_WithConfiguration
    {
        [Fact]
        public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new ServiceBusQueueSinkNode<TestModel>(null!, A.Fake<ILogger>()));
        }

        [Fact]
        public void Constructor_WithInvalidConfiguration_ThrowsInvalidOperationException()
        {
            var config = new ServiceBusConfiguration(); // No connection or queue

            Assert.Throws<InvalidOperationException>(() =>
                new ServiceBusQueueSinkNode<TestModel>(config));
        }
    }

    public class Constructor_WithSender
    {
        [Fact]
        public void Constructor_WithNullSender_ThrowsArgumentNullException()
        {
            var config = CreateValidConfig();

            Assert.Throws<ArgumentNullException>(() =>
                new ServiceBusQueueSinkNode<TestModel>(null!, config));
        }

        [Fact]
        public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
        {
            var sender = A.Fake<ServiceBusSender>();

            Assert.Throws<ArgumentNullException>(() =>
                new ServiceBusQueueSinkNode<TestModel>(sender, null!));
        }

        [Fact]
        public void Constructor_WithValidSenderAndConfig_DoesNotThrow()
        {
            var sender = A.Fake<ServiceBusSender>();
            var config = CreateValidConfig();

            var exception = Record.Exception(() => new ServiceBusQueueSinkNode<TestModel>(sender, config));
            exception.Should().BeNull();
        }

        [Fact]
        public void Constructor_WithSenderAndInvalidConfig_ThrowsInvalidOperationException()
        {
            var sender = A.Fake<ServiceBusSender>();
            var config = new ServiceBusConfiguration(); // Missing queue name

            Assert.Throws<InvalidOperationException>(() =>
                new ServiceBusQueueSinkNode<TestModel>(sender, config));
        }
    }

    private class TestModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

public class ServiceBusTopicSinkNodeTests
{
    private static ServiceBusConfiguration CreateValidConfig()
    {
        return new ServiceBusConfiguration
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=abc=",
            TopicName = "test-topic",
        };
    }

    public class Constructor_WithSender
    {
        [Fact]
        public void Constructor_WithNullSender_ThrowsArgumentNullException()
        {
            var config = CreateValidConfig();

            Assert.Throws<ArgumentNullException>(() =>
                new ServiceBusTopicSinkNode<TestModel>(null!, config));
        }

        [Fact]
        public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
        {
            var sender = A.Fake<ServiceBusSender>();

            Assert.Throws<ArgumentNullException>(() =>
                new ServiceBusTopicSinkNode<TestModel>(sender, null!));
        }

        [Fact]
        public void Constructor_WithValidSenderAndConfig_DoesNotThrow()
        {
            var sender = A.Fake<ServiceBusSender>();
            var config = CreateValidConfig();

            var exception = Record.Exception(() => new ServiceBusTopicSinkNode<TestModel>(sender, config));
            exception.Should().BeNull();
        }
    }

    private class TestModel
    {
        public int Id { get; set; }
    }
}
