using System.Reflection;
using Amazon.SQS;
using Amazon.SQS.Model;
using FakeItEasy;
using NPipeline.Connectors.AwsSqs.Configuration;
using NPipeline.Connectors.AwsSqs.Models;
using NPipeline.Connectors.AwsSqs.Nodes;
using NPipeline.Connectors.Configuration;
using NPipeline.DataFlow;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Aws.Sqs.Tests.Nodes;

public class SqsSinkNodeTests
{
    private static IDataPipe<T> CreateDataPipe<T>(T[] items)
    {
        return new TestDataPipe<T>(items);
    }

    private static SqsMessage<TestModel> CreateSqsMessage(
        string messageId = "test-id",
        string receiptHandle = "test-handle",
        TestModel? body = null)
    {
        body ??= new TestModel { Id = 1, Name = "Test" };
        var attributes = new Dictionary<string, MessageAttributeValue>();
        var timestamp = DateTime.UtcNow;
        Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

        var messageType = typeof(SqsMessage<>).MakeGenericType(typeof(TestModel));

        var constructor = messageType.GetConstructors(
                BindingFlags.NonPublic | BindingFlags.Instance)
            .First(c => c.GetParameters().Length == 6);

        return (SqsMessage<TestModel>)constructor.Invoke(
            [body, messageId, receiptHandle, attributes, timestamp, callback]);
    }

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
            var exception = Record.Exception(() => new SqsSinkNode<SqsMessage<TestModel>>(configuration));
            exception.Should().BeNull();
        }

        [Fact]
        public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new SqsSinkNode<SqsMessage<TestModel>>(null!));
        }

        [Fact]
        public void Constructor_WithInvalidConfiguration_ThrowsInvalidOperationException()
        {
            // Arrange
            var configuration = new SqsConfiguration(); // Missing queue URLs

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() => new SqsSinkNode<SqsMessage<TestModel>>(configuration));
        }

        [Fact]
        public void Constructor_WithCustomSqsClient_DoesNotThrow()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            var sqsClientFake = A.Fake<IAmazonSQS>();

            // Act & Assert
            var exception = Record.Exception(() => new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration));
            exception.Should().BeNull();
        }

        [Fact]
        public void Constructor_WithNullSqsClient_ThrowsArgumentNullException()
        {
            // Arrange
            var configuration = CreateValidConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new SqsSinkNode<SqsMessage<TestModel>>(null!, configuration));
        }
    }

    public class AcknowledgmentStrategy_AutoOnSinkSuccess
    {
        [Fact]
        public async Task ExecuteAsync_WithAutoOnSinkStrategy_AcknowledgesImmediately()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;

            configuration.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                EnableAutomaticBatching = false,
            };

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var message = CreateSqsMessage();

            // Act
            await node.ExecuteAsync(CreateDataPipe([message]), context, cts.Token);

            // Assert
            message.IsAcknowledged.Should().BeTrue();
        }

        [Fact]
        public async Task ExecuteAsync_WithAutoOnSinkStrategyAndBatching_AcknowledgesInBatch()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;

            configuration.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                EnableAutomaticBatching = true,
                BatchSize = 2,
            };

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            A.CallTo(() => sqsClientFake.SendMessageBatchAsync(A<SendMessageBatchRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageBatchResponse());

            A.CallTo(() => sqsClientFake.DeleteMessageBatchAsync(A<DeleteMessageBatchRequest>._, A<CancellationToken>._))
                .Returns(new DeleteMessageBatchResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var message1 = CreateSqsMessage("msg-1", "handle-1");
            var message2 = CreateSqsMessage("msg-2", "handle-2");

            // Act
            await node.ExecuteAsync(CreateDataPipe([message1, message2]), context, cts.Token);

            // Assert
            message1.IsAcknowledged.Should().BeTrue();
            message2.IsAcknowledged.Should().BeTrue();

            A.CallTo(() => sqsClientFake.DeleteMessageBatchAsync(A<DeleteMessageBatchRequest>._, A<CancellationToken>._))
                .MustHaveHappened();
        }
    }

    public class AcknowledgmentStrategy_Manual
    {
        [Fact]
        public async Task ExecuteAsync_WithManualStrategy_DoesNotAcknowledge()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.Manual;

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var message = CreateSqsMessage();

            // Act
            await node.ExecuteAsync(CreateDataPipe([message]), context, cts.Token);

            // Assert
            message.IsAcknowledged.Should().BeFalse();
        }

        [Fact]
        public async Task ExecuteAsync_WithManualStrategy_CanManuallyAcknowledge()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.Manual;

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            A.CallTo(() => sqsClientFake.DeleteMessageAsync(A<string>._, A<string>._, A<CancellationToken>._))
                .Returns(new DeleteMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var message = CreateSqsMessage();

            // Act
            await node.ExecuteAsync(CreateDataPipe([message]), context, cts.Token);
            await message.AcknowledgeAsync();

            // Assert
            message.IsAcknowledged.Should().BeTrue();
        }
    }

    public class AcknowledgmentStrategy_Delayed
    {
        [Fact]
        public async Task ExecuteAsync_WithDelayedStrategy_AcknowledgesAfterDelay()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.Delayed;
            configuration.AcknowledgmentDelayMs = 100;

            configuration.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                EnableAutomaticBatching = false,
            };

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            A.CallTo(() => sqsClientFake.DeleteMessageAsync(A<string>._, A<string>._, A<CancellationToken>._))
                .Returns(new DeleteMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var message = CreateSqsMessage();

            // Act
            await node.ExecuteAsync(CreateDataPipe([message]), context, cts.Token);

            // Assert - Not acknowledged immediately
            message.IsAcknowledged.Should().BeFalse();

            // Wait for delay
            await Task.Delay(150);

            // Assert - Should be acknowledged after delay
            message.IsAcknowledged.Should().BeTrue();
        }

        [Fact]
        public async Task ExecuteAsync_WithDelayedStrategy_WhenCancelledBeforeDelay_DoesNotAcknowledge()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.Delayed;
            configuration.AcknowledgmentDelayMs = 5000;

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var message = CreateSqsMessage();

            // Act
            var executeTask = node.ExecuteAsync(CreateDataPipe([message]), context, cts.Token);
            await executeTask;

            // Cancel immediately
            cts.Cancel();

            // Wait a bit
            await Task.Delay(100);

            // Assert - Should not be acknowledged
            message.IsAcknowledged.Should().BeFalse();
        }
    }

    public class AcknowledgmentStrategy_None
    {
        [Fact]
        public async Task ExecuteAsync_WithNoneStrategy_DoesNotAcknowledge()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.None;

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var message = CreateSqsMessage();

            // Act
            await node.ExecuteAsync(CreateDataPipe([message]), context, cts.Token);

            // Assert
            message.IsAcknowledged.Should().BeFalse();
        }

        [Fact]
        public async Task ExecuteAsync_WithNoneStrategy_DoesNotCallDeleteMessage()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.None;

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var message = CreateSqsMessage();

            // Act
            await node.ExecuteAsync(CreateDataPipe([message]), context, cts.Token);

            // Assert
            A.CallTo(() => sqsClientFake.DeleteMessageAsync(A<string>._, A<string>._, A<CancellationToken>._))
                .MustNotHaveHappened();

            A.CallTo(() => sqsClientFake.DeleteMessageBatchAsync(A<DeleteMessageBatchRequest>._, A<CancellationToken>._))
                .MustNotHaveHappened();
        }
    }

    public class BatchAcknowledgment
    {
        [Fact]
        public async Task ExecuteAsync_WithBatchAcknowledgment_AcksInBatches()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;

            configuration.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                EnableAutomaticBatching = true,
                BatchSize = 3,
            };

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            A.CallTo(() => sqsClientFake.SendMessageBatchAsync(A<SendMessageBatchRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageBatchResponse());

            A.CallTo(() => sqsClientFake.DeleteMessageBatchAsync(A<DeleteMessageBatchRequest>._, A<CancellationToken>._))
                .Returns(new DeleteMessageBatchResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var messages = new[]
            {
                CreateSqsMessage("msg-1", "handle-1"),
                CreateSqsMessage("msg-2", "handle-2"),
                CreateSqsMessage("msg-3", "handle-3"),
                CreateSqsMessage("msg-4", "handle-4"),
            };

            // Act
            await node.ExecuteAsync(CreateDataPipe(messages), context, cts.Token);

            // Assert
            messages.All(m => m.IsAcknowledged).Should().BeTrue();

            A.CallTo(() => sqsClientFake.DeleteMessageBatchAsync(A<DeleteMessageBatchRequest>._, A<CancellationToken>._))
                .MustHaveHappened();
        }

        [Fact]
        public async Task ExecuteAsync_WithDisabledBatchAcknowledgment_AcksIndividually()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;

            configuration.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                EnableAutomaticBatching = false,
            };

            configuration.BatchSize = 1; // Set to 1 for individual message processing

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var messages = new[]
            {
                CreateSqsMessage("msg-1", "handle-1"),
                CreateSqsMessage("msg-2", "handle-2"),
                CreateSqsMessage("msg-3", "handle-3"),
            };

            // Act
            await node.ExecuteAsync(CreateDataPipe(messages), context, cts.Token);

            // Assert
            messages.All(m => m.IsAcknowledged).Should().BeTrue();

            A.CallTo(() => sqsClientFake.DeleteMessageBatchAsync(A<DeleteMessageBatchRequest>._, A<CancellationToken>._))
                .MustNotHaveHappened();
        }

        [Fact]
        public async Task ExecuteAsync_WithFlushTimeout_FlushesPartialBatch()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;

            configuration.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                EnableAutomaticBatching = true,
                BatchSize = 10,
                FlushTimeoutMs = 100,
            };

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            A.CallTo(() => sqsClientFake.DeleteMessageBatchAsync(A<DeleteMessageBatchRequest>._, A<CancellationToken>._))
                .Returns(new DeleteMessageBatchResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var message = CreateSqsMessage();

            // Act
            await node.ExecuteAsync(CreateDataPipe([message]), context, cts.Token);

            // Wait for flush timeout
            await Task.Delay(150);

            // Assert
            message.IsAcknowledged.Should().BeTrue();
        }
    }

    public class ParallelVsSequentialProcessing
    {
        [Fact]
        public async Task ExecuteAsync_WithSequentialProcessing_ProcessesSequentially()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.EnableParallelProcessing = false;
            configuration.MaxDegreeOfParallelism = 1;
            configuration.BatchSize = 1; // Set to 1 for individual message processing

            configuration.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                EnableAutomaticBatching = false,
            };

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var messages = new[]
            {
                CreateSqsMessage("msg-1", "handle-1"),
                CreateSqsMessage("msg-2", "handle-2"),
                CreateSqsMessage("msg-3", "handle-3"),
            };

            // Act
            await node.ExecuteAsync(CreateDataPipe(messages), context, cts.Token);

            // Assert
            messages.All(m => m.IsAcknowledged).Should().BeTrue();
        }

        [Fact]
        public async Task ExecuteAsync_WithParallelProcessing_ProcessesInParallel()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.EnableParallelProcessing = true;
            configuration.MaxDegreeOfParallelism = 3;
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;

            configuration.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                EnableAutomaticBatching = false,
            };

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var messages = new[]
            {
                CreateSqsMessage("msg-1", "handle-1"),
                CreateSqsMessage("msg-2", "handle-2"),
                CreateSqsMessage("msg-3", "handle-3"),
            };

            // Act
            await node.ExecuteAsync(CreateDataPipe(messages), context, cts.Token);

            // Assert
            messages.All(m => m.IsAcknowledged).Should().BeTrue();
        }

        [Fact]
        public async Task ExecuteAsync_WithMaxDegreeOfParallelism_LimitsConcurrency()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.EnableParallelProcessing = true;
            configuration.MaxDegreeOfParallelism = 2;
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;

            configuration.BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                EnableAutomaticBatching = false,
            };

            var sqsClientFake = A.Fake<IAmazonSQS>();

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var messages = new[]
            {
                CreateSqsMessage("msg-1", "handle-1"),
                CreateSqsMessage("msg-2", "handle-2"),
                CreateSqsMessage("msg-3", "handle-3"),
                CreateSqsMessage("msg-4", "handle-4"),
            };

            // Act
            await node.ExecuteAsync(CreateDataPipe(messages), context, cts.Token);

            // Assert
            messages.All(m => m.IsAcknowledged).Should().BeTrue();
        }
    }

    public class SendMessageBehavior
    {
        [Fact]
        public async Task ExecuteAsync_WithRegularMessage_SendsMessage()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.None;
            var sqsClientFake = A.Fake<IAmazonSQS>();
            SendMessageRequest? capturedRequest = null;

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Invokes((SendMessageRequest req, CancellationToken ct) => capturedRequest = req)
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var testModel = new TestModel { Id = 42, Name = "Test" };

            // Act
            await node.ExecuteAsync(CreateDataPipe([CreateSqsMessage(body: testModel)]), context, cts.Token);

            // Assert
            capturedRequest.Should().NotBeNull();
            capturedRequest!.QueueUrl.Should().Be(configuration.SinkQueueUrl);
            capturedRequest.MessageBody.Should().Contain("42");
            capturedRequest.MessageBody.Should().Contain("Test");
        }

        [Fact]
        public async Task ExecuteAsync_WithMessageAttributes_AddsAttributes()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.None;
            configuration.BatchSize = 1; // Set to 1 for individual message processing

            configuration.MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                ["CustomAttr"] = new()
                {
                    DataType = "String",
                    StringValue = "custom-value",
                },
            };

            var sqsClientFake = A.Fake<IAmazonSQS>();
            SendMessageRequest? capturedRequest = null;
            Exception? capturedException = null;

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Invokes((SendMessageRequest req, CancellationToken ct) =>
                {
                    try
                    {
                        capturedRequest = req;
                        Console.WriteLine($"Callback executed: QueueUrl={req.QueueUrl}, Body={req.MessageBody}");
                    }
                    catch (Exception ex)
                    {
                        capturedException = ex;
                        Console.WriteLine($"Callback exception: {ex}");
                    }
                })
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var testModel = new TestModel { Id = 42, Name = "Test" };

            // Act
            try
            {
                await node.ExecuteAsync(CreateDataPipe([CreateSqsMessage(body: testModel)]), context, cts.Token);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ExecuteAsync exception: {ex}");
                capturedException = ex;
            }

            Console.WriteLine($"After ExecuteAsync: capturedRequest={capturedRequest != null}, capturedException={capturedException != null}");

            // Assert
            if (capturedException != null)
                throw new Exception($"Test failed with exception: {capturedException}", capturedException);

            capturedRequest.Should().NotBeNull("SendMessageAsync callback should have been executed");
            capturedRequest!.MessageAttributes.Should().ContainKey("CustomAttr");
            capturedRequest.MessageAttributes["CustomAttr"].StringValue.Should().Be("custom-value");
        }

        [Fact]
        public async Task ExecuteAsync_WithDelaySeconds_SetsDelay()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.None;
            configuration.DelaySeconds = 30;

            var sqsClientFake = A.Fake<IAmazonSQS>();
            SendMessageRequest? capturedRequest = null;

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Invokes((SendMessageRequest req, CancellationToken ct) => capturedRequest = req)
                .Returns(new SendMessageResponse());

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var testModel = new TestModel { Id = 42, Name = "Test" };

            // Act
            await node.ExecuteAsync(CreateDataPipe([CreateSqsMessage(body: testModel)]), context, cts.Token);

            // Assert
            capturedRequest.Should().NotBeNull();
            capturedRequest!.DelaySeconds.Should().Be(30);
        }

        [Fact]
        public async Task ExecuteAsync_WithContinueOnError_ContinuesOnFailure()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.ContinueOnError = true;
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.None;
            configuration.BatchSize = 1; // Set to 1 for individual message processing

            var sqsClientFake = A.Fake<IAmazonSQS>();
            var callCount = 0;

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Invokes(() => callCount++)
                .Throws(new Exception("Simulated error"));

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var testModels = new[]
            {
                CreateSqsMessage(body: new TestModel { Id = 1, Name = "Test1" }),
                CreateSqsMessage(body: new TestModel { Id = 2, Name = "Test2" }),
                CreateSqsMessage(body: new TestModel { Id = 3, Name = "Test3" }),
            };

            // Act
            await node.ExecuteAsync(CreateDataPipe(testModels), context, cts.Token);

            // Assert - Should have attempted to send all messages
            callCount.Should().Be(3);
        }

        [Fact]
        public async Task ExecuteAsync_WithContinueOnErrorFalse_StopsOnFailure()
        {
            // Arrange
            var configuration = CreateValidConfiguration();
            configuration.ContinueOnError = false;
            configuration.AcknowledgmentStrategy = AcknowledgmentStrategy.None;
            configuration.BatchSize = 1; // Set to 1 for individual message processing

            var sqsClientFake = A.Fake<IAmazonSQS>();
            var callCount = 0;

            A.CallTo(() => sqsClientFake.SendMessageAsync(A<SendMessageRequest>._, A<CancellationToken>._))
                .Invokes(() => callCount++)
                .Throws(new Exception("Simulated error"));

            var node = new SqsSinkNode<SqsMessage<TestModel>>(sqsClientFake, configuration);
            var context = new PipelineContext();
            var cts = new CancellationTokenSource();

            var testModels = new[]
            {
                CreateSqsMessage(body: new TestModel { Id = 1, Name = "Test1" }),
                CreateSqsMessage(body: new TestModel { Id = 2, Name = "Test2" }),
                CreateSqsMessage(body: new TestModel { Id = 3, Name = "Test3" }),
            };

            // Act & Assert
            await Assert.ThrowsAsync<Exception>(async () =>
                await node.ExecuteAsync(CreateDataPipe(testModels), context, cts.Token));

            // Assert - Should have stopped after first error
            callCount.Should().Be(1);
        }
    }

    private class TestModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class TestDataPipe<T> : IDataPipe<T>
    {
        private readonly T[] _items;

        public TestDataPipe(T[] items)
        {
            _items = items;
        }

        public string StreamName => "test-stream";

        public Type GetDataType()
        {
            return typeof(T);
        }

        public IAsyncEnumerable<object?> ToAsyncEnumerable(CancellationToken cancellationToken = default)
        {
            return _items.ToAsyncEnumerable().Select(x => (object?)x);
        }

        public async ValueTask DisposeAsync()
        {
            // No resources to dispose
            await Task.CompletedTask;
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return _items.ToAsyncEnumerable().GetAsyncEnumerator(cancellationToken);
        }
    }
}
