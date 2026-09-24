using System.Net;
using System.Net.Sockets;
using System.Text;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using FakeItEasy;
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Internal;
using NPipeline.Connectors.Aws.Sqs.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Aws.Sqs.Tests.Reliability.Behavior;

/// <summary>
///     The SQS connector leaves retries to the AWS SDK (S1 in <c>plans/resilience-improvements.md</c>): the client it
///     builds carries the retry settings, and the nodes don't retry on top of it.
/// </summary>
public sealed class SqsRetryBehaviorTests
{
    private const string QueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/source-queue";

    [Fact]
    public void Defaults_UseTheSdkStandardRetryWithTheOldAttemptCount()
    {
        var configuration = new SqsConfiguration { SourceQueueUrl = QueueUrl };

        // MaxRetries = 3 used to make four receive calls; MaxErrorRetry = 3 makes four attempts per call.
        configuration.RetryMode.Should().Be(RequestRetryMode.Standard);
        configuration.MaxErrorRetry.Should().Be(3);

        var clientConfig = SqsClientFactory.CreateClientConfig(configuration);
        clientConfig.RetryMode.Should().Be(RequestRetryMode.Standard);
        clientConfig.MaxErrorRetry.Should().Be(3);
    }

    [Fact]
    public void ClientConfig_AppliesTheConfiguredRetrySettings()
    {
        var configuration = new SqsConfiguration
        {
            SourceQueueUrl = QueueUrl,
            RetryMode = RequestRetryMode.Adaptive,
            MaxErrorRetry = 7,
        };

        var clientConfig = SqsClientFactory.CreateClientConfig(configuration);

        clientConfig.RetryMode.Should().Be(RequestRetryMode.Adaptive);
        clientConfig.MaxErrorRetry.Should().Be(7);
    }

    [Fact]
    public void ClientConfig_WithNullSettings_LeavesThemToTheSdk()
    {
        var configuration = new SqsConfiguration
        {
            SourceQueueUrl = QueueUrl,
            RetryMode = null,
            MaxErrorRetry = null,
        };

        var clientConfig = SqsClientFactory.CreateClientConfig(configuration);

        clientConfig.IsMaxErrorRetrySet.Should().BeFalse();
    }

    [Fact]
    public void Validate_RejectsANegativeMaxErrorRetry()
    {
        var configuration = new SqsConfiguration { SourceQueueUrl = QueueUrl, MaxErrorRetry = -1 };

        var act = configuration.ValidateSource;

        act.Should().Throw<InvalidOperationException>().WithMessage("*MaxErrorRetry*");
    }

    [Theory]
    [InlineData(3, 4)]
    [InlineData(0, 1)]
    public async Task Source_WithPersistentServerError_MakesOnlyTheSdkAttempts(int maxErrorRetry, int expectedRequests)
    {
        using var server = new FailingServer();

        var configuration = new SqsConfiguration
        {
            SourceQueueUrl = QueueUrl,
            MaxErrorRetry = maxErrorRetry,
            PollingIntervalMs = 0,
        };

        // The client the connector would build, pointed at a local server that always answers 503.
        var clientConfig = SqsClientFactory.CreateClientConfig(configuration);
        clientConfig.ServiceURL = server.Url;
        using var client = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), clientConfig);

        var node = new SqsSourceNode<string>(client, configuration);

        var act = async () =>
        {
            await foreach (var _ in node.OpenStream(new PipelineContext(), CancellationToken.None))
            {
            }
        };

        _ = await act.Should().ThrowAsync<AmazonServiceException>();

        // Before, a connector-level loop retried each exhausted SDK call three more times, multiplying the attempts.
        server.Requests.Should().Be(expectedRequests);
    }

    [Fact]
    public async Task Source_WithACallerSuppliedClient_DoesNotAddRetries()
    {
        // The connector can't reconfigure a client it didn't build; it just doesn't retry on top of it.
        var client = A.Fake<IAmazonSQS>();
        var calls = 0;

        A.CallTo(() => client.ReceiveMessageAsync(A<ReceiveMessageRequest>._, A<CancellationToken>._))
            .ReturnsLazily(ReceiveMessageResponse () =>
            {
                calls++;
                throw new AmazonSQSException("Throttled", ErrorType.Sender, "ThrottlingException", null, HttpStatusCode.TooManyRequests);
            });

        var node = new SqsSourceNode<string>(client, new SqsConfiguration { SourceQueueUrl = QueueUrl });

        var act = async () =>
        {
            await foreach (var _ in node.OpenStream(new PipelineContext(), CancellationToken.None))
            {
            }
        };

        _ = await act.Should().ThrowAsync<AmazonSQSException>();
        calls.Should().Be(1);
    }

    /// <summary>A local HTTP server that answers every request with 503 and counts them.</summary>
    private sealed class FailingServer : IDisposable
    {
        private readonly HttpListener _listener = new();
        private int _requests;

        public FailingServer()
        {
            int port;

            using (var socket = new TcpListener(IPAddress.Loopback, 0))
            {
                socket.Start();
                port = ((IPEndPoint)socket.LocalEndpoint).Port;
            }

            Url = $"http://127.0.0.1:{port}/";
            _listener.Prefixes.Add(Url);
            _listener.Start();
            _ = Task.Run(ServeAsync);
        }

        public string Url { get; }

        public int Requests => Volatile.Read(ref _requests);

        public void Dispose()
        {
            _listener.Close();
        }

        private async Task ServeAsync()
        {
            while (_listener.IsListening)
            {
                HttpListenerContext context;

                try
                {
                    context = await _listener.GetContextAsync();
                }
                catch (Exception) when (!_listener.IsListening)
                {
                    return;
                }

                _ = Interlocked.Increment(ref _requests);
                await context.Request.InputStream.CopyToAsync(Stream.Null);

                context.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                context.Response.ContentType = "application/x-amz-json-1.0";
                var body = Encoding.UTF8.GetBytes("""{"__type":"com.amazonaws.sqs#ServiceUnavailable","message":"unavailable"}""");
                await context.Response.OutputStream.WriteAsync(body);
                context.Response.Close();
            }
        }
    }
}
