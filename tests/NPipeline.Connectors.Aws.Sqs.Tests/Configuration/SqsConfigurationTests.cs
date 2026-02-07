using NPipeline.Connectors.AwsSqs.Configuration;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.AwsSqs.Tests.Configuration;

public class SqsConfigurationTests
{
    private static SqsConfiguration CreateValidConfiguration()
    {
        return new SqsConfiguration
        {
            SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/source-queue",
            SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/sink-queue",
        };
    }

    public class ConstructorAndDefaults
    {
        [Fact]
        public void Constructor_InitializesWithDefaultValues()
        {
            // Arrange & Act
            var config = new SqsConfiguration();

            // Assert
            config.Region.Should().Be("us-east-1");
            config.MaxNumberOfMessages.Should().Be(10);
            config.WaitTimeSeconds.Should().Be(20);
            config.VisibilityTimeout.Should().Be(30);
            config.PollingIntervalMs.Should().Be(1000);
            config.BatchSize.Should().Be(10);
            config.DelaySeconds.Should().Be(0);
            config.PropertyNamingPolicy.Should().Be(JsonPropertyNamingPolicy.CamelCase);
            config.PropertyNameCaseInsensitive.Should().BeTrue();
            config.MaxRetries.Should().Be(3);
            config.RetryBaseDelayMs.Should().Be(1000);
            config.ContinueOnError.Should().BeTrue();
            config.AcknowledgmentStrategy.Should().Be(AcknowledgmentStrategy.AutoOnSinkSuccess);
            config.AcknowledgmentDelayMs.Should().Be(5000);
            config.MaxConnectionPoolSize.Should().Be(10);
            config.MaxDegreeOfParallelism.Should().Be(1);
            config.EnableParallelProcessing.Should().BeFalse();
        }

        [Fact]
        public void Constructor_AllowsSettingAllProperties()
        {
            // Arrange & Act
            var config = new SqsConfiguration
            {
                AccessKeyId = "test-access-key",
                SecretAccessKey = "test-secret-key",
                Region = "us-west-2",
                ProfileName = "test-profile",
                SourceQueueUrl = "https://sqs.us-west-2.amazonaws.com/123456789012/source-queue",
                SinkQueueUrl = "https://sqs.us-west-2.amazonaws.com/123456789012/sink-queue",
                MaxNumberOfMessages = 5,
                WaitTimeSeconds = 10,
                VisibilityTimeout = 60,
                PollingIntervalMs = 500,
                BatchSize = 5,
                DelaySeconds = 30,
                PropertyNamingPolicy = JsonPropertyNamingPolicy.SnakeCase,
                PropertyNameCaseInsensitive = false,
                MaxRetries = 5,
                RetryBaseDelayMs = 2000,
                ContinueOnError = false,
                AcknowledgmentStrategy = AcknowledgmentStrategy.Manual,
                AcknowledgmentDelayMs = 10000,
                MaxConnectionPoolSize = 20,
                MaxDegreeOfParallelism = 5,
                EnableParallelProcessing = true,
            };

            // Assert
            config.AccessKeyId.Should().Be("test-access-key");
            config.SecretAccessKey.Should().Be("test-secret-key");
            config.Region.Should().Be("us-west-2");
            config.ProfileName.Should().Be("test-profile");
            config.SourceQueueUrl.Should().Be("https://sqs.us-west-2.amazonaws.com/123456789012/source-queue");
            config.SinkQueueUrl.Should().Be("https://sqs.us-west-2.amazonaws.com/123456789012/sink-queue");
            config.MaxNumberOfMessages.Should().Be(5);
            config.WaitTimeSeconds.Should().Be(10);
            config.VisibilityTimeout.Should().Be(60);
            config.PollingIntervalMs.Should().Be(500);
            config.BatchSize.Should().Be(5);
            config.DelaySeconds.Should().Be(30);
            config.PropertyNamingPolicy.Should().Be(JsonPropertyNamingPolicy.SnakeCase);
            config.PropertyNameCaseInsensitive.Should().BeFalse();
            config.MaxRetries.Should().Be(5);
            config.RetryBaseDelayMs.Should().Be(2000);
            config.ContinueOnError.Should().BeFalse();
            config.AcknowledgmentStrategy.Should().Be(AcknowledgmentStrategy.Manual);
            config.AcknowledgmentDelayMs.Should().Be(10000);
            config.MaxConnectionPoolSize.Should().Be(20);
            config.MaxDegreeOfParallelism.Should().Be(5);
            config.EnableParallelProcessing.Should().BeTrue();
        }
    }

    public class Validation_QueueUrls
    {
        [Fact]
        public void Validate_WhenSourceQueueUrlIsEmpty_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = new SqsConfiguration
            {
                SourceQueueUrl = string.Empty,
                SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/sink-queue",
            };

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("SourceQueueUrl must be specified");
        }

        [Fact]
        public void Validate_WhenSourceQueueUrlIsWhitespace_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = new SqsConfiguration
            {
                SourceQueueUrl = "   ",
                SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/sink-queue",
            };

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("SourceQueueUrl must be specified");
        }

        [Fact]
        public void Validate_WhenSinkQueueUrlIsEmpty_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = new SqsConfiguration
            {
                SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/source-queue",
                SinkQueueUrl = string.Empty,
            };

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("SinkQueueUrl must be specified");
        }

        [Fact]
        public void Validate_WhenSinkQueueUrlIsWhitespace_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = new SqsConfiguration
            {
                SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/source-queue",
                SinkQueueUrl = "   ",
            };

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("SinkQueueUrl must be specified");
        }

        [Fact]
        public void Validate_WhenBothQueueUrlsAreValid_DoesNotThrow()
        {
            // Arrange
            var config = new SqsConfiguration
            {
                SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/source-queue",
                SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/sink-queue",
            };

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }
    }

    public class Validation_NumericRanges
    {
        [Fact]
        public void Validate_WhenMaxNumberOfMessagesIsZero_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.MaxNumberOfMessages = 0;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("MaxNumberOfMessages must be between 1 and 10");
        }

        [Fact]
        public void Validate_WhenMaxNumberOfMessagesIsNegative_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.MaxNumberOfMessages = -1;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("MaxNumberOfMessages must be between 1 and 10");
        }

        [Fact]
        public void Validate_WhenMaxNumberOfMessagesIsGreaterThan10_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.MaxNumberOfMessages = 11;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("MaxNumberOfMessages must be between 1 and 10");
        }

        [Fact]
        public void Validate_WhenMaxNumberOfMessagesIsWithinRange_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.MaxNumberOfMessages = 5;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenWaitTimeSecondsIsNegative_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.WaitTimeSeconds = -1;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("WaitTimeSeconds must be between 0 and 20");
        }

        [Fact]
        public void Validate_WhenWaitTimeSecondsIsGreaterThan20_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.WaitTimeSeconds = 21;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("WaitTimeSeconds must be between 0 and 20");
        }

        [Fact]
        public void Validate_WhenWaitTimeSecondsIsWithinRange_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.WaitTimeSeconds = 15;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenVisibilityTimeoutIsNegative_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.VisibilityTimeout = -1;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("VisibilityTimeout must be between 0 and 43200");
        }

        [Fact]
        public void Validate_WhenVisibilityTimeoutIsGreaterThan43200_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.VisibilityTimeout = 43201;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("VisibilityTimeout must be between 0 and 43200");
        }

        [Fact]
        public void Validate_WhenVisibilityTimeoutIsWithinRange_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.VisibilityTimeout = 300;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenBatchSizeIsZero_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.BatchSize = 0;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("BatchSize must be between 1 and 10");
        }

        [Fact]
        public void Validate_WhenBatchSizeIsGreaterThan10_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.BatchSize = 11;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("BatchSize must be between 1 and 10");
        }

        [Fact]
        public void Validate_WhenBatchSizeIsWithinRange_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.BatchSize = 5;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenDelaySecondsIsNegative_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.DelaySeconds = -1;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("DelaySeconds must be between 0 and 900");
        }

        [Fact]
        public void Validate_WhenDelaySecondsIsGreaterThan900_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.DelaySeconds = 901;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("DelaySeconds must be between 0 and 900");
        }

        [Fact]
        public void Validate_WhenDelaySecondsIsWithinRange_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.DelaySeconds = 60;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenMaxConnectionPoolSizeIsZero_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.MaxConnectionPoolSize = 0;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("MaxConnectionPoolSize must be at least 1");
        }

        [Fact]
        public void Validate_WhenMaxConnectionPoolSizeIsPositive_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.MaxConnectionPoolSize = 5;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenMaxDegreeOfParallelismIsZero_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.MaxDegreeOfParallelism = 0;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("MaxDegreeOfParallelism must be at least 1");
        }

        [Fact]
        public void Validate_WhenMaxDegreeOfParallelismIsPositive_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.MaxDegreeOfParallelism = 3;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }
    }

    public class Validation_AcknowledgmentConfiguration
    {
        [Fact]
        public void Validate_WhenDelayedStrategyWithNegativeDelay_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AcknowledgmentStrategy = AcknowledgmentStrategy.Delayed;
            config.AcknowledgmentDelayMs = -1;

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
            exception.Message.Should().Contain("AcknowledgmentDelayMs must be non-negative");
        }

        [Fact]
        public void Validate_WhenDelayedStrategyWithZeroDelay_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AcknowledgmentStrategy = AcknowledgmentStrategy.Delayed;
            config.AcknowledgmentDelayMs = 0;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenDelayedStrategyWithPositiveDelay_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AcknowledgmentStrategy = AcknowledgmentStrategy.Delayed;
            config.AcknowledgmentDelayMs = 10000;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenAutoOnSinkSuccessStrategyWithNegativeDelay_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;
            config.AcknowledgmentDelayMs = -1;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenManualStrategyWithNegativeDelay_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AcknowledgmentStrategy = AcknowledgmentStrategy.Manual;
            config.AcknowledgmentDelayMs = -1;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenNoneStrategyWithNegativeDelay_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AcknowledgmentStrategy = AcknowledgmentStrategy.None;
            config.AcknowledgmentDelayMs = -1;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenBatchAcknowledgmentOptionsAreValid_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();

            config.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                BatchSize = 5,
                FlushTimeoutMs = 500,
                EnableAutomaticBatching = true,
                MaxConcurrentBatches = 2,
            };

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenBatchAcknowledgmentOptionsAreInvalid_ThrowsInvalidOperationException()
        {
            // Arrange
            var config = CreateValidConfiguration();

            config.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                BatchSize = 15, // Invalid: > 10
            };

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() => config.Validate());
        }

        [Fact]
        public void Validate_WhenBatchAcknowledgmentIsNull_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.BatchAcknowledgment = null;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }
    }

    public class Validation_Credentials
    {
        [Fact]
        public void Validate_WhenAccessKeyIdAndSecretAccessKeyAreProvided_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AccessKeyId = "test-access-key";
            config.SecretAccessKey = "test-secret-key";

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenProfileNameIsProvided_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.ProfileName = "test-profile";

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WhenNoCredentialsProvided_DoesNotThrow_AllowsDefaultCredentialChain()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AccessKeyId = null;
            config.SecretAccessKey = null;
            config.ProfileName = null;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }
    }

    public class Validation_AllStrategies
    {
        [Fact]
        public void Validate_WithAutoOnSinkSuccessStrategy_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WithManualStrategy_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AcknowledgmentStrategy = AcknowledgmentStrategy.Manual;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WithDelayedStrategy_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AcknowledgmentStrategy = AcknowledgmentStrategy.Delayed;
            config.AcknowledgmentDelayMs = 5000;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WithNoneStrategy_DoesNotThrow()
        {
            // Arrange
            var config = CreateValidConfiguration();
            config.AcknowledgmentStrategy = AcknowledgmentStrategy.None;

            // Act & Assert
            config.Invoking(c => c.Validate()).Should().NotThrow();
        }
    }
}
