using Azure.Core;
using FakeItEasy;
using NPipeline.Connectors.Azure.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Configuration;

public class ServiceBusConfigurationTests
{
    private static ServiceBusConfiguration CreateValidQueueSourceConfig()
    {
        return new ServiceBusConfiguration
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123=",
            QueueName = "test-queue",
        };
    }

    private static ServiceBusConfiguration CreateValidQueueSinkConfig()
    {
        return new ServiceBusConfiguration
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123=",
            QueueName = "test-queue",
        };
    }

    private static ServiceBusConfiguration CreateValidTopicSinkConfig()
    {
        return new ServiceBusConfiguration
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123=",
            TopicName = "test-topic",
        };
    }

    private static ServiceBusConfiguration CreateValidSubscriptionSourceConfig()
    {
        return new ServiceBusConfiguration
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123=",
            TopicName = "test-topic",
            SubscriptionName = "test-subscription",
        };
    }

    public class Defaults
    {
        [Fact]
        public void NewConfiguration_HasExpectedDefaults()
        {
            var config = new ServiceBusConfiguration();

            config.AuthenticationMode.Should().Be(AzureAuthenticationMode.ConnectionString);
            config.MaxConcurrentCalls.Should().Be(1);
            config.MaxAutoLockRenewalDuration.Should().Be(TimeSpan.FromMinutes(5));
            config.EnableAutoComplete.Should().BeFalse();
            config.EnableBatchSending.Should().BeTrue();
            config.BatchSize.Should().Be(100);
            config.MaxConcurrentSessions.Should().Be(8);
            config.SessionMaxConcurrentCallsPerSession.Should().Be(1);
            config.SessionIdleTimeout.Should().Be(TimeSpan.FromMinutes(1));
            config.AcknowledgmentStrategy.Should().Be(AcknowledgmentStrategy.AutoOnSinkSuccess);
            config.ContinueOnError.Should().BeTrue();
            config.DeadLetterOnDeserializationError.Should().BeTrue();
            config.EnableSessions.Should().BeFalse();
            config.PrefetchCount.Should().Be(0);
            config.InternalBufferCapacity.Should().Be(0);
        }
    }

    public class ValidateSource_ConnectionString
    {
        [Fact]
        public void ValidateSource_WithConnectionStringAndQueueName_DoesNotThrow()
        {
            var config = CreateValidQueueSourceConfig();
            config.Invoking(c => c.ValidateSource()).Should().NotThrow();
        }

        [Fact]
        public void ValidateSource_WithConnectionStringAndTopicAndSubscription_DoesNotThrow()
        {
            var config = CreateValidSubscriptionSourceConfig();
            config.Invoking(c => c.ValidateSource()).Should().NotThrow();
        }

        [Fact]
        public void ValidateSource_WhenConnectionStringIsEmpty_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.ConnectionString = string.Empty;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("ConnectionString must be specified");
        }

        [Fact]
        public void ValidateSource_WhenConnectionStringIsWhitespace_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.ConnectionString = "   ";

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("ConnectionString must be specified");
        }
    }

    public class ValidateSource_AzureAdCredential
    {
        [Fact]
        public void ValidateSource_WithAzureAdAndNamespaceAndCredential_DoesNotThrow()
        {
            var config = new ServiceBusConfiguration
            {
                AuthenticationMode = AzureAuthenticationMode.AzureAdCredential,
                FullyQualifiedNamespace = "test.servicebus.windows.net",
                Credential = A.Fake<TokenCredential>(),
                QueueName = "test-queue",
            };

            config.Invoking(c => c.ValidateSource()).Should().NotThrow();
        }

        [Fact]
        public void ValidateSource_WithAzureAdAndMissingNamespace_ThrowsInvalidOperationException()
        {
            var config = new ServiceBusConfiguration
            {
                AuthenticationMode = AzureAuthenticationMode.AzureAdCredential,
                FullyQualifiedNamespace = null,
                Credential = A.Fake<TokenCredential>(),
                QueueName = "test-queue",
            };

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("FullyQualifiedNamespace must be specified");
        }

        [Fact]
        public void ValidateSource_WithAzureAdAndMissingCredential_DoesNotThrow()
        {
            var config = new ServiceBusConfiguration
            {
                AuthenticationMode = AzureAuthenticationMode.AzureAdCredential,
                FullyQualifiedNamespace = "test.servicebus.windows.net",
                Credential = null,
                QueueName = "test-queue",
            };

            config.Invoking(c => c.ValidateSource()).Should().NotThrow();
        }
    }

    public class ValidateSource_EndpointWithKey
    {
        [Fact]
        public void ValidateSource_WithEndpointWithKeyAndRequiredFields_DoesNotThrow()
        {
            var config = new ServiceBusConfiguration
            {
                AuthenticationMode = AzureAuthenticationMode.EndpointWithKey,
                FullyQualifiedNamespace = "test.servicebus.windows.net",
                SharedAccessKeyName = "RootManageSharedAccessKey",
                SharedAccessKey = "abc123=",
                QueueName = "test-queue",
            };

            config.Invoking(c => c.ValidateSource()).Should().NotThrow();
        }

        [Fact]
        public void ValidateSource_WithEndpointWithKeyAndMissingKeyName_ThrowsInvalidOperationException()
        {
            var config = new ServiceBusConfiguration
            {
                AuthenticationMode = AzureAuthenticationMode.EndpointWithKey,
                FullyQualifiedNamespace = "test.servicebus.windows.net",
                SharedAccessKey = "abc123=",
                QueueName = "test-queue",
            };

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("SharedAccessKeyName must be specified");
        }

        [Fact]
        public void ValidateSource_WithEndpointWithKeyAndMissingKey_ThrowsInvalidOperationException()
        {
            var config = new ServiceBusConfiguration
            {
                AuthenticationMode = AzureAuthenticationMode.EndpointWithKey,
                FullyQualifiedNamespace = "test.servicebus.windows.net",
                SharedAccessKeyName = "RootManageSharedAccessKey",
                QueueName = "test-queue",
            };

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("SharedAccessKey must be specified");
        }
    }

    public class ValidateSource_NamedConnection
    {
        [Fact]
        public void ValidateSource_WithNamedConnectionAndQueueName_SkipsConnectionValidation()
        {
            var config = new ServiceBusConfiguration
            {
                NamedConnection = "MyConnection",
                QueueName = "test-queue",
            };

            // No connection string needed when NamedConnection is provided
            config.Invoking(c => c.ValidateSource()).Should().NotThrow();
        }
    }

    public class ValidateSource_Entity
    {
        [Fact]
        public void ValidateSource_WhenNoQueueOrTopic_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.QueueName = null;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("QueueName or (TopicName + SubscriptionName)");
        }

        [Fact]
        public void ValidateSource_WithTopicButNoSubscription_ThrowsInvalidOperationException()
        {
            var config = new ServiceBusConfiguration
            {
                ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=abc=",
                TopicName = "test-topic",
                SubscriptionName = null,
            };

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("QueueName or (TopicName + SubscriptionName)");
        }
    }

    public class ValidateSource_Concurrency
    {
        [Fact]
        public void ValidateSource_WhenMaxConcurrentCallsIsZero_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.MaxConcurrentCalls = 0;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("MaxConcurrentCalls must be at least 1");
        }

        [Fact]
        public void ValidateSource_WhenMaxConcurrentCallsIsNegative_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.MaxConcurrentCalls = -1;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("MaxConcurrentCalls must be at least 1");
        }
    }

    public class ValidateSource_Session
    {
        [Fact]
        public void ValidateSource_WithValidSessionConfig_DoesNotThrow()
        {
            var config = CreateValidQueueSourceConfig();
            config.EnableSessions = true;
            config.MaxConcurrentSessions = 5;
            config.SessionMaxConcurrentCallsPerSession = 2;

            config.Invoking(c => c.ValidateSource()).Should().NotThrow();
        }

        [Fact]
        public void ValidateSource_WithZeroMaxConcurrentSessions_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.EnableSessions = true;
            config.MaxConcurrentSessions = 0;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("MaxConcurrentSessions must be at least 1");
        }

        [Fact]
        public void ValidateSource_WithZeroCallsPerSession_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.EnableSessions = true;
            config.SessionMaxConcurrentCallsPerSession = 0;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("SessionMaxConcurrentCallsPerSession must be at least 1");
        }
    }

    public class ValidateSource_BufferCapacity
    {
        [Fact]
        public void ValidateSource_WhenInternalBufferCapacityIsNegative_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.InternalBufferCapacity = -1;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("InternalBufferCapacity must be non-negative");
        }

        [Fact]
        public void ValidateSource_WhenInternalBufferCapacityIsZero_DoesNotThrow()
        {
            var config = CreateValidQueueSourceConfig();
            config.InternalBufferCapacity = 0;
            config.Invoking(c => c.ValidateSource()).Should().NotThrow();
        }
    }

    public class ValidateSink
    {
        [Fact]
        public void ValidateSink_WithQueueName_DoesNotThrow()
        {
            var config = CreateValidQueueSinkConfig();
            config.Invoking(c => c.ValidateSink()).Should().NotThrow();
        }

        [Fact]
        public void ValidateSink_WithTopicName_DoesNotThrow()
        {
            var config = CreateValidTopicSinkConfig();
            config.Invoking(c => c.ValidateSink()).Should().NotThrow();
        }

        [Fact]
        public void ValidateSink_WithNoQueueOrTopicName_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSinkConfig();
            config.QueueName = null;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSink());
            ex.Message.Should().Contain("QueueName or TopicName must be specified");
        }

        [Fact]
        public void ValidateSink_WhenBatchSizeIsZero_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSinkConfig();
            config.BatchSize = 0;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSink());
            ex.Message.Should().Contain("BatchSize must be between 1 and 100");
        }

        [Fact]
        public void ValidateSink_WhenBatchSizeExceeds100_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSinkConfig();
            config.BatchSize = 101;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSink());
            ex.Message.Should().Contain("BatchSize must be between 1 and 100");
        }

        [Fact]
        public void ValidateSink_WhenBatchSizeIs100_DoesNotThrow()
        {
            var config = CreateValidQueueSinkConfig();
            config.BatchSize = 100;
            config.Invoking(c => c.ValidateSink()).Should().NotThrow();
        }
    }

    public class ValidateRetry
    {
        [Fact]
        public void ValidateSource_WhenRetryMaxRetriesIsNegative_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.Retry.MaxRetries = -1;

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("Retry.MaxRetries must be non-negative");
        }

        [Fact]
        public void ValidateSource_WhenRetryDelayIsNegative_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.Retry.Delay = TimeSpan.FromSeconds(-1);

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("Retry.Delay must be non-negative");
        }

        [Fact]
        public void ValidateSource_WhenRetryMaxDelayIsLessThanDelay_ThrowsInvalidOperationException()
        {
            var config = CreateValidQueueSourceConfig();
            config.Retry.Delay = TimeSpan.FromSeconds(10);
            config.Retry.MaxDelay = TimeSpan.FromSeconds(5);

            var ex = Assert.Throws<InvalidOperationException>(() => config.ValidateSource());
            ex.Message.Should().Contain("Retry.MaxDelay must be greater than or equal to Retry.Delay");
        }
    }
}
