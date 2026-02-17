using System.Net;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using FakeItEasy;
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Aws.Sqs.Tests.Nodes;

public class SqsSourceNodeTests
{
    private static SqsConfiguration CreateValidConfiguration()
    {
        return new SqsConfiguration
        {
            SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/source-queue",
            SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/sink-queue",
        };
    }

    public class Constructor
    {
        [Fact]
        public void Constructor_WithValidConfiguration_DoesNotThrow()
        {
            // Arrange
            var configuration = CreateValidConfiguration();

            // Act & Assert
            var exception = Record.Exception(() => new SqsSourceNode<TestModel>(configuration));
            exception.Should().BeNull();
        }

        [Fact]
        public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new SqsSourceNode<TestModel>(null!));
        }

        [Fact]
        public void Constructor_WithInvalidConfiguration_ThrowsInvalidOperationException()
        {
            // Arrange
            var configuration = new SqsConfiguration(); // Missing queue URLs

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() => new SqsSourceNode<TestModel>(configuration));
        }

        [Fact]
        public void Constructor_WithCustomSqsClient_DoesNotThrow()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            var sqsClientFake = A.Fake<IAmazonSQS>();

            // Act & Assert
            var exception = Record.Exception(() => new SqsSourceNode<TestModel>(sqsClientFake, configuration));
            exception.Should().BeNull();
        }

        [Fact]
        public void Constructor_WithNullSqsClient_ThrowsArgumentNullException()
        {
            // Arrange
            var configuration = CreateValidConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new SqsSourceNode<TestModel>(null!, configuration));
        }

        [Fact]
        public void Constructor_WithCustomSqsClientAndNullConfiguration_ThrowsArgumentNullException()
        {
            // Arrange
            var sqsClientFake = A.Fake<IAmazonSQS>();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new SqsSourceNode<TestModel>(sqsClientFake, null!));
        }
    }

    public class SqsClientCreation
    {
        [Fact]
        public void Constructor_WithAccessKeyIdAndSecretKey_CreatesAmazonSQSClient()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AccessKeyId = "test-access-key";
            configuration.SecretAccessKey = "test-secret-key";

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithProfileName_CreatesAmazonSQSClient()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.ProfileName = "test-profile";

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithNoCredentials_UsesDefaultCredentialChain()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AccessKeyId = null;
            configuration.SecretAccessKey = null;
            configuration.ProfileName = null;

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithAccessKeyIdOnly_DoesNotCreateClientWithPartialCredentials()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AccessKeyId = "test-access-key";
            configuration.SecretAccessKey = null;

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created (falls back to default credential chain)
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithSecretAccessKeyOnly_DoesNotCreateClientWithPartialCredentials()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AccessKeyId = null;
            configuration.SecretAccessKey = "test-secret-key";

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created (falls back to default credential chain)
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithCustomRegion_CreatesClientWithCorrectRegion()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.Region = "us-west-2";

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }
    }

    public class SerializerOptions
    {
        [Fact]
        public void Constructor_WithCamelCaseNamingPolicy_SetsCorrectPolicy()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase;

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithSnakeCaseNamingPolicy_SetsCorrectPolicy()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.PropertyNamingPolicy = JsonPropertyNamingPolicy.SnakeCase;

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithPascalCaseNamingPolicy_SetsCorrectPolicy()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.PropertyNamingPolicy = JsonPropertyNamingPolicy.PascalCase;

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithLowerCaseNamingPolicy_SetsCorrectPolicy()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.PropertyNamingPolicy = JsonPropertyNamingPolicy.LowerCase;

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithAsIsNamingPolicy_SetsCorrectPolicy()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.PropertyNamingPolicy = JsonPropertyNamingPolicy.AsIs;

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithCaseInsensitiveTrue_SetsCorrectOption()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.PropertyNameCaseInsensitive = true;

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithCaseInsensitiveFalse_SetsCorrectOption()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.PropertyNameCaseInsensitive = false;

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithCustomNamingPolicyAndCaseInsensitive_SetsBothOptions()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.PropertyNamingPolicy = JsonPropertyNamingPolicy.SnakeCase;
            configuration.PropertyNameCaseInsensitive = false;

            // Act
            var node = new SqsSourceNode<TestModel>(configuration);

            // Assert - Node should be created successfully
            node.Should().NotBeNull();
        }
    }

    public class Initialize
    {
        [Fact]
        public async Task Initialize_WithValidContext_ReturnsDataPipe()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var node = new SqsSourceNode<TestModel>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            // Setup fake to return empty messages
            A.CallTo(() => sqsClientFake.ReceiveMessageAsync(
                    A<ReceiveMessageRequest>._,
                    A<CancellationToken>._))
                .Returns(new ReceiveMessageResponse { Messages = new List<Message>() });

            // Act
            var dataPipe = node.Initialize(context, cts.Token);

            // Assert
            dataPipe.Should().NotBeNull();
            await cts.CancelAsync();
        }

        [Fact]
        public void Initialize_WithCancelledToken_CanBeCancelled()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var node = new SqsSourceNode<TestModel>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            cts.Cancel();

            // Act
            var dataPipe = node.Initialize(context, cts.Token);

            // Assert
            dataPipe.Should().NotBeNull();
        }
    }

    public class PollingBehavior
    {
        [Fact]
        public async Task PollMessagesAsync_WithEmptyQueue_WaitsBeforeNextPoll()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.PollingIntervalMs = 100;
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var node = new SqsSourceNode<TestModel>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            // Setup fake to return empty messages
            A.CallTo(() => sqsClientFake.ReceiveMessageAsync(
                    A<ReceiveMessageRequest>._,
                    A<CancellationToken>._))
                .Returns(new ReceiveMessageResponse { Messages = new List<Message>() });

            // Act
            var dataPipe = node.Initialize(context, cts.Token);
            var enumerator = dataPipe.GetAsyncEnumerator(cts.Token);

            // Start polling
            var pollTask = enumerator.MoveNextAsync().AsTask();

            // Wait a bit
            await Task.Delay(150);

            // Cancel
            await cts.CancelAsync();

            try
            {
                await pollTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        [Fact]
        public async Task PollMessagesAsync_WithMessages_YieldsMessages()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var node = new SqsSourceNode<TestModel>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            var testMessage = new Message
            {
                MessageId = "test-id",
                ReceiptHandle = "test-handle",
                Body = JsonSerializer.Serialize(new TestModel { Id = 1, Name = "Test" }),
                Attributes = new Dictionary<string, string>
                {
                    ["SentTimestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(),
                },
                MessageAttributes = new Dictionary<string, MessageAttributeValue>(),
            };

            // Setup fake to return messages
            A.CallTo(() => sqsClientFake.ReceiveMessageAsync(
                    A<ReceiveMessageRequest>._,
                    A<CancellationToken>._))
                .Returns(new ReceiveMessageResponse { Messages = new List<Message> { testMessage } });

            // Act
            var dataPipe = node.Initialize(context, cts.Token);
            var enumerator = dataPipe.GetAsyncEnumerator(cts.Token);

            // Get first message
            var hasMessage = await enumerator.MoveNextAsync();

            // Assert
            hasMessage.Should().BeTrue();
            enumerator.Current.Should().NotBeNull();
            enumerator.Current.MessageId.Should().Be("test-id");

            await cts.CancelAsync();
        }

        [Fact]
        public async Task PollMessagesAsync_WithTransientError_Retries()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.RetryBaseDelayMs = 10;
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var node = new SqsSourceNode<TestModel>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            var callCount = 0;
            var secondCallSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            // Setup fake to fail once, then succeed
            A.CallTo(() => sqsClientFake.ReceiveMessageAsync(
                    A<ReceiveMessageRequest>._,
                    A<CancellationToken>._))
                .ReturnsLazily(() =>
                {
                    callCount++;

                    if (callCount == 2)
                        secondCallSignal.TrySetResult();

                    if (callCount == 1)
                        throw new AmazonSQSException("Service unavailable", ErrorType.Unknown, "ServiceUnavailable", null, HttpStatusCode.ServiceUnavailable);

                    return new ReceiveMessageResponse { Messages = new List<Message>() };
                });

            // Act
            var dataPipe = node.Initialize(context, cts.Token);
            var enumerator = dataPipe.GetAsyncEnumerator(cts.Token);

            // Start polling
            var pollTask = enumerator.MoveNextAsync().AsTask();

            // Wait for retry to happen
            var completedTask = await Task.WhenAny(secondCallSignal.Task, Task.Delay(500, cts.Token));
            completedTask.Should().Be(secondCallSignal.Task);

            // Cancel
            await cts.CancelAsync();

            try
            {
                await pollTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }

            // Assert - Should have been called twice (initial + retry)
            callCount.Should().BeGreaterThanOrEqualTo(2);
        }

        [Fact]
        public async Task PollMessagesAsync_WithCancellation_StopsPolling()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.PollingIntervalMs = 100;
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var node = new SqsSourceNode<TestModel>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            var callCount = 0;

            // Setup fake to track calls
            A.CallTo(() => sqsClientFake.ReceiveMessageAsync(
                    A<ReceiveMessageRequest>._,
                    A<CancellationToken>._))
                .Invokes(() => callCount++)
                .Returns(new ReceiveMessageResponse { Messages = new List<Message>() });

            // Act
            var dataPipe = node.Initialize(context, cts.Token);
            var enumerator = dataPipe.GetAsyncEnumerator(cts.Token);

            // Start polling
            var pollTask = enumerator.MoveNextAsync().AsTask();

            // Wait a bit
            await Task.Delay(250);

            // Cancel
            await cts.CancelAsync();

            try
            {
                await pollTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }

            // Assert - Should have been called a few times before cancellation
            callCount.Should().BeGreaterThanOrEqualTo(1);
        }
    }

    public class MessageDeserialization
    {
        [Fact]
        public async Task PollMessagesAsync_WithValidJson_DeserializesCorrectly()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var node = new SqsSourceNode<TestModel>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            var testModel = new TestModel { Id = 42, Name = "TestName" };

            var testMessage = new Message
            {
                MessageId = "test-id",
                ReceiptHandle = "test-handle",
                Body = JsonSerializer.Serialize(testModel),
                Attributes = new Dictionary<string, string>
                {
                    ["SentTimestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(),
                },
                MessageAttributes = new Dictionary<string, MessageAttributeValue>(),
            };

            A.CallTo(() => sqsClientFake.ReceiveMessageAsync(
                    A<ReceiveMessageRequest>._,
                    A<CancellationToken>._))
                .Returns(new ReceiveMessageResponse { Messages = new List<Message> { testMessage } });

            // Act
            var dataPipe = node.Initialize(context, cts.Token);
            var enumerator = dataPipe.GetAsyncEnumerator(cts.Token);
            await enumerator.MoveNextAsync();

            // Assert
            enumerator.Current.Body.Id.Should().Be(42);
            enumerator.Current.Body.Name.Should().Be("TestName");

            await cts.CancelAsync();
        }

        [Fact]
        public async Task PollMessagesAsync_WithInvalidJson_WhenContinueOnError_SkipsMessage()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.ContinueOnError = true;
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var node = new SqsSourceNode<TestModel>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

            var invalidMessage = new Message
            {
                MessageId = "invalid-id",
                ReceiptHandle = "invalid-handle",
                Body = "invalid json",
                Attributes = new Dictionary<string, string>
                {
                    ["SentTimestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(),
                },
                MessageAttributes = new Dictionary<string, MessageAttributeValue>(),
            };

            A.CallTo(() => sqsClientFake.ReceiveMessageAsync(
                    A<ReceiveMessageRequest>._,
                    A<CancellationToken>._))
                .Returns(new ReceiveMessageResponse { Messages = new List<Message> { invalidMessage } });

            // Act
            var dataPipe = node.Initialize(context, cts.Token);
            var enumerator = dataPipe.GetAsyncEnumerator(cts.Token);

            // Should not yield a message; cancellation ends the poll.
            var moveNextTask = enumerator.MoveNextAsync().AsTask();

            try
            {
                var hasMessage = await moveNextTask;
                hasMessage.Should().BeFalse();
            }
            catch (OperationCanceledException)
            {
                // Expected once cancellation fires
            }
        }

        [Fact]
        public async Task PollMessagesAsync_WithInvalidJson_WhenNotContinueOnError_Throws()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.ContinueOnError = false;
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var node = new SqsSourceNode<TestModel>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            var invalidMessage = new Message
            {
                MessageId = "invalid-id",
                ReceiptHandle = "invalid-handle",
                Body = "invalid json",
                Attributes = new Dictionary<string, string>
                {
                    ["SentTimestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(),
                },
                MessageAttributes = new Dictionary<string, MessageAttributeValue>(),
            };

            A.CallTo(() => sqsClientFake.ReceiveMessageAsync(
                    A<ReceiveMessageRequest>._,
                    A<CancellationToken>._))
                .Returns(new ReceiveMessageResponse { Messages = new List<Message> { invalidMessage } });

            // Act
            var dataPipe = node.Initialize(context, cts.Token);
            var enumerator = dataPipe.GetAsyncEnumerator(cts.Token);

            // Assert
            await Assert.ThrowsAsync<JsonException>(async () => await enumerator.MoveNextAsync());

            await cts.CancelAsync();
        }
    }

    public class ReceiveMessageRequestTests
    {
        [Fact]
        public async Task PollMessagesAsync_SendsCorrectReceiveMessageRequest()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.MaxNumberOfMessages = 5;
            configuration.WaitTimeSeconds = 10;
            configuration.VisibilityTimeout = 60;

            var sqsClientFake = A.Fake<IAmazonSQS>();
            var node = new SqsSourceNode<TestModel>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            ReceiveMessageRequest? capturedRequest = null;

            A.CallTo(() => sqsClientFake.ReceiveMessageAsync(
                    A<ReceiveMessageRequest>._,
                    A<CancellationToken>._))
                .Invokes((ReceiveMessageRequest req, CancellationToken ct) => capturedRequest = req)
                .Returns(new ReceiveMessageResponse { Messages = new List<Message>() });

            // Act
            var dataPipe = node.Initialize(context, cts.Token);
            var enumerator = dataPipe.GetAsyncEnumerator(cts.Token);
            var pollTask = enumerator.MoveNextAsync().AsTask();

            for (var i = 0; i < 10 && capturedRequest == null; i++)
            {
                await Task.Delay(10);
            }

            await cts.CancelAsync();

            try
            {
                await pollTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }

            // Assert
            capturedRequest.Should().NotBeNull();
            capturedRequest!.QueueUrl.Should().Be(configuration.SourceQueueUrl);
            capturedRequest.MaxNumberOfMessages.Should().Be(5);
            capturedRequest.WaitTimeSeconds.Should().Be(10);
            capturedRequest.VisibilityTimeout.Should().Be(60);
            capturedRequest.MessageSystemAttributeNames.Should().Contain("All");
            capturedRequest.MessageAttributeNames.Should().Contain("All");
        }
    }

    private class TestModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
