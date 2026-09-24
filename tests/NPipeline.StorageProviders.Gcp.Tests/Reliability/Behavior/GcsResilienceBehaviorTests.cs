using System.Net;
using System.Net.Sockets;
using System.Text;
using AwesomeAssertions;
using FakeItEasy;
using Google;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Download;
using Google.Apis.Upload;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Gcp.Reliability;
using NPipeline.StorageProviders.Models;
using NResilience;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace NPipeline.StorageProviders.Gcp.Tests.Reliability.Behavior;

public sealed class GcsResilienceBehaviorTests
{
    private static readonly StorageUri Uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

    // The default preset with no delay between attempts, so tests of transient failures stay fast.
    private static readonly Resilience FastRetry = GcsStorageResilience.Default with { Backoff = Backoff.None };

    private static GoogleApiException ApiError(HttpStatusCode status) => new("storage", $"status {(int)status}") { HttpStatusCode = status };

    private static (GcsStorageProvider Provider, StorageClient Client) CreateProvider(Resilience resilience)
    {
        var options = new GcsStorageProviderOptions { Resilience = resilience };

        var factory = A.Fake<GcsClientFactory>(c => c
            .WithArgumentsForConstructor([options])
            .CallsBaseMethods());

        var client = A.Fake<StorageClient>();

        A.CallTo(() => factory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(client));

        return (new GcsStorageProvider(factory, options), client);
    }

    [Fact]
    public void Default_ReproducesTheSdkDefaultRetry()
    {
        var preset = GcsStorageResilience.Default;

        // The Google SDK retried idempotent requests with three tries, one second doubling to 32 seconds.
        preset.Attempts.Should().Be(3);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(1));
        preset.Backoff.ThrottledBase.Should().Be(TimeSpan.FromSeconds(1));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(32));
        preset.Backoff.Factor.Should().Be(2.0);
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse();
        preset.Classifier.Should().BeSameAs(GcsStorageResilience.Classifier);
        preset.Validate();
    }

    [Theory]
    [InlineData(HttpStatusCode.RequestTimeout)]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.BadGateway)]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.GatewayTimeout)]
    public void Classifier_TreatsTimeoutsAndServerErrorsAsTransient(HttpStatusCode status)
    {
        GcsStorageResilience.Classifier.ClassifyException(ApiError(status)).Kind.Should().Be(VerdictKind.Transient);
    }

    [Theory]
    [InlineData(HttpStatusCode.BadRequest)]
    [InlineData(HttpStatusCode.Unauthorized)]
    [InlineData(HttpStatusCode.Forbidden)]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.Conflict)]
    [InlineData(HttpStatusCode.PreconditionFailed)]
    public void Classifier_TreatsClientErrorsAsPermanent(HttpStatusCode status)
    {
        GcsStorageResilience.Classifier.ClassifyException(ApiError(status)).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public void Classifier_TreatsRateLimitAsThrottledAndCarriesRetryAfter()
    {
        var plain = GcsStorageResilience.Classifier.ClassifyException(ApiError(HttpStatusCode.TooManyRequests));
        plain.Kind.Should().Be(VerdictKind.Throttled);
        plain.RetryAfter.Should().BeNull();

        var withHeader = ApiError(HttpStatusCode.TooManyRequests);
        withHeader.Data[GcsRetryAfter.DataKey] = TimeSpan.FromSeconds(7);

        var verdict = GcsStorageResilience.Classifier.ClassifyException(withHeader);
        verdict.Kind.Should().Be(VerdictKind.Throttled);
        verdict.RetryAfter.Should().Be(TimeSpan.FromSeconds(7));
    }

    [Fact]
    public void Classifier_TreatsNetworkFailuresAsTransient()
    {
        var classifier = GcsStorageResilience.Classifier;

        classifier.ClassifyException(new HttpRequestException("connection reset")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new IOException("broken pipe")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new SocketException()).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new TaskCanceledException("client timeout", new TimeoutException())).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("bug")).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public async Task ExistsAsync_WithPersistentServerError_MakesThreeAttemptsAndMarksTheFailureAsRetried()
    {
        var (provider, client) = CreateProvider(FastRetry);
        var attempts = 0;

        A.CallTo(() => client.GetObjectAsync(A<string>._, A<string>._, A<GetObjectOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                attempts++;
                return Task.FromException<Object>(ApiError(HttpStatusCode.ServiceUnavailable));
            });

        var exception = await Assert.ThrowsAsync<GcsStorageException>(() => provider.ExistsAsync(Uri));

        attempts.Should().Be(3);

        // The translated exception keeps NResilience's marker, so a pipeline-level retry does not multiply attempts.
        exception.Data.Contains("NResilience.Attempts").Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_TurnsOffTheSdkRetryOnEachRequest()
    {
        var (provider, client) = CreateProvider(FastRetry);
        GetObjectOptions? seen = null;

        A.CallTo(() => client.GetObjectAsync(A<string>._, A<string>._, A<GetObjectOptions>._, A<CancellationToken>._))
            .ReturnsLazily(call =>
            {
                seen = call.GetArgument<GetObjectOptions>(2);
                return Task.FromResult(new Object());
            });

        (await provider.ExistsAsync(Uri)).Should().BeTrue();

        seen.Should().NotBeNull();
        seen!.RetryOptions.Should().BeSameAs(RetryOptions.Never);
    }

    [Fact]
    public async Task ExistsAsync_WithPermanentError_DoesNotRetry()
    {
        var (provider, client) = CreateProvider(FastRetry);
        var attempts = 0;

        A.CallTo(() => client.GetObjectAsync(A<string>._, A<string>._, A<GetObjectOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                attempts++;
                return Task.FromException<Object>(ApiError(HttpStatusCode.Forbidden));
            });

        _ = await Assert.ThrowsAsync<UnauthorizedAccessException>(() => provider.ExistsAsync(Uri));
        attempts.Should().Be(1);
    }

    [Fact]
    public async Task OpenReadAsync_RetryStartsWithAnEmptyBuffer()
    {
        var (provider, client) = CreateProvider(FastRetry);
        var attempts = 0;

        A.CallTo(() => client.DownloadObjectAsync(
                A<string>._, A<string>._, A<Stream>._, A<DownloadObjectOptions>._, A<CancellationToken>._, A<IProgress<IDownloadProgress>>._))
            .ReturnsLazily(call =>
            {
                attempts++;
                var destination = call.GetArgument<Stream>(2)!;

                if (attempts == 1)
                {
                    // The connection drops part-way through the object.
                    destination.Write("partial-"u8);
                    return Task.FromException<Object>(new IOException("connection reset"));
                }

                destination.Write("complete"u8);
                return Task.FromResult(new Object());
            });

        await using var stream = await provider.OpenReadAsync(Uri);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        (await reader.ReadToEndAsync()).Should().Be("complete");
        attempts.Should().Be(2);
    }

    [Fact]
    public async Task OpenWriteAsync_RetryReSendsTheWholeObject()
    {
        var (provider, client) = CreateProvider(FastRetry);
        var uploads = new List<byte[]>();

        A.CallTo(() => client.UploadObjectAsync(A<Object>._, A<Stream>._, A<UploadObjectOptions>._, A<CancellationToken>._, A<IProgress<IUploadProgress>>._))
            .ReturnsLazily(call =>
            {
                var source = call.GetArgument<Stream>(1)!;

                if (uploads.Count == 0)
                {
                    // The first attempt sends part of the object before the server fails it.
                    var partial = new byte[3];
                    source.ReadExactly(partial);
                    uploads.Add(partial);
                    return Task.FromException<Object>(ApiError(HttpStatusCode.ServiceUnavailable));
                }

                using var copy = new MemoryStream();
                source.CopyTo(copy);
                uploads.Add(copy.ToArray());
                return Task.FromResult(new Object());
            });

        var payload = Encoding.UTF8.GetBytes("the whole object");

        await using (var stream = await provider.OpenWriteAsync(Uri))
        {
            await stream.WriteAsync(payload);
        }

        uploads.Should().HaveCount(2);
        uploads[1].Should().Equal(payload);
    }

    [Fact]
    public async Task OpenWriteAsync_WithPermanentError_UploadsOnce()
    {
        var (provider, client) = CreateProvider(FastRetry);
        var attempts = 0;

        A.CallTo(() => client.UploadObjectAsync(A<Object>._, A<Stream>._, A<UploadObjectOptions>._, A<CancellationToken>._, A<IProgress<IUploadProgress>>._))
            .ReturnsLazily(() =>
            {
                attempts++;
                return Task.FromException<Object>(ApiError(HttpStatusCode.Forbidden));
            });

        var stream = await provider.OpenWriteAsync(Uri);
        await stream.WriteAsync(new byte[] { 1, 2, 3 });

        _ = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await stream.DisposeAsync());
        attempts.Should().Be(1);
    }

    [Fact]
    public async Task ExistsAsync_WhenPipelineTokenIsCancelledDuringBackoff_StopsWithoutRetrying()
    {
        var slowRetry = GcsStorageResilience.Default with
        {
            Backoff = Backoff.Default with { TransientBase = TimeSpan.FromSeconds(30), MaximumDelay = TimeSpan.FromSeconds(30) },
        };

        var (provider, client) = CreateProvider(slowRetry);
        using var cts = new CancellationTokenSource();
        var attempts = 0;

        A.CallTo(() => client.GetObjectAsync(A<string>._, A<string>._, A<GetObjectOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                attempts++;
                cts.CancelAfter(TimeSpan.FromMilliseconds(50));
                return Task.FromException<Object>(ApiError(HttpStatusCode.ServiceUnavailable));
            });

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.ExistsAsync(Uri, cts.Token));
        attempts.Should().Be(1);
    }

    [Fact]
    public async Task ExistsAsync_WithForeignCancellation_FailsInsteadOfAnsweringOrRetrying()
    {
        var (provider, client) = CreateProvider(FastRetry);
        var attempts = 0;

        A.CallTo(() => client.GetObjectAsync(A<string>._, A<string>._, A<GetObjectOptions>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                attempts++;
                return Task.FromException<Object>(new OperationCanceledException("cancelled by something else"));
            });

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.ExistsAsync(Uri, CancellationToken.None));
        attempts.Should().Be(1);
    }

    [Fact]
    public async Task FactoryClient_SendsEachRequestOnce_SoOnlyNResilienceRetries()
    {
        using var server = new FakeGcsServer(_ => (HttpStatusCode.ServiceUnavailable, null));
        var provider = CreateRealProvider(server, Resilience.None);

        _ = await Assert.ThrowsAsync<GcsStorageException>(() => provider.ExistsAsync(Uri));

        // The Google SDK would try three times on its own; with its retry off, one attempt is one request.
        server.Requests.Should().Be(1);
    }

    [Fact]
    public async Task FactoryClient_RetriesServerErrorsThroughNResilience()
    {
        using var server = new FakeGcsServer(n => n < 3
            ? (HttpStatusCode.ServiceUnavailable, null)
            : (HttpStatusCode.OK, null));

        var provider = CreateRealProvider(server, FastRetry);

        (await provider.ExistsAsync(Uri)).Should().BeTrue();
        server.Requests.Should().Be(3);
    }

    [Fact]
    public async Task FactoryClient_HonorsRetryAfterOnRateLimit()
    {
        using var server = new FakeGcsServer(n => n == 1
            ? (HttpStatusCode.TooManyRequests, "1")
            : (HttpStatusCode.OK, null));

        var delays = new List<TimeSpan>();

        var resilience = (GcsStorageResilience.Default with
        {
            Backoff = Backoff.Default with { TransientBase = TimeSpan.FromMilliseconds(1), ThrottledBase = TimeSpan.FromMilliseconds(1) },
        }).WithListener(e =>
        {
            if (e.Kind == CallEventKind.Retrying && e.Delay is { } delay)
                delays.Add(delay);
        });

        var provider = CreateRealProvider(server, resilience);

        (await provider.ExistsAsync(Uri)).Should().BeTrue();
        server.Requests.Should().Be(2);
        delays.Should().ContainSingle().Which.Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task FactoryClient_DoesNotResumeAFailedUploadInsideTheSdk()
    {
        // Every upload session accepts the start request and then fails the data with 503.
        using var server = new FakeGcsServer((request, _) => request.HttpMethod == "PUT"
            ? (HttpStatusCode.ServiceUnavailable, null)
            : (HttpStatusCode.OK, null));

        var provider = CreateRealProvider(server, Resilience.None);

        var stream = await provider.OpenWriteAsync(Uri);
        await stream.WriteAsync(new byte[] { 1, 2, 3 });

        _ = await Assert.ThrowsAsync<GcsStorageException>(async () => await stream.DisposeAsync());

        // The SDK's in-session resume would send the data up to three times; with it off, one attempt is one PUT.
        server.UploadPuts.Should().Be(1);
    }

    [Fact]
    public async Task FactoryClient_RetriesAFailedUploadInANewSession()
    {
        var puts = 0;

        using var server = new FakeGcsServer((request, _) =>
            request.HttpMethod == "PUT" && Interlocked.Increment(ref puts) == 1
                ? (HttpStatusCode.ServiceUnavailable, null)
                : (HttpStatusCode.OK, null));

        var provider = CreateRealProvider(server, FastRetry);

        await using (var stream = await provider.OpenWriteAsync(Uri))
        {
            await stream.WriteAsync(new byte[] { 1, 2, 3 });
        }

        server.UploadPuts.Should().Be(2);
        server.Requests.Should().Be(4); // two session starts, two data requests
    }

    private static GcsStorageProvider CreateRealProvider(FakeGcsServer server, Resilience resilience)
    {
        var options = new GcsStorageProviderOptions
        {
            ServiceUrl = server.BaseUri,
            DefaultCredentials = GoogleCredential.FromAccessToken("test-token"),
            Resilience = resilience,
        };

        return new GcsStorageProvider(new GcsClientFactory(options), options);
    }

    /// <summary>A local HTTP server that answers every request with the status the script returns for it.</summary>
    private sealed class FakeGcsServer : IDisposable
    {
        private readonly HttpListener _listener = new();
        private readonly Func<HttpListenerRequest, int, (HttpStatusCode Status, string? RetryAfter)> _script;
        private int _requests;
        private int _uploadPuts;

        public FakeGcsServer(Func<int, (HttpStatusCode Status, string? RetryAfter)> script)
            : this((_, n) => script(n))
        {
        }

        public FakeGcsServer(Func<HttpListenerRequest, int, (HttpStatusCode Status, string? RetryAfter)> script)
        {
            _script = script;
            var port = FreePort();
            BaseUri = new Uri($"http://127.0.0.1:{port}/storage/v1/");
            _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
            _listener.Start();
            _ = Task.Run(ServeAsync);
        }

        public Uri BaseUri { get; }

        public int Requests => Volatile.Read(ref _requests);

        /// <summary>The number of requests that sent object data to an upload session.</summary>
        public int UploadPuts => Volatile.Read(ref _uploadPuts);

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

                var request = context.Request;

                // Read the body so the client sees the response rather than a reset connection.
                using var received = new MemoryStream();
                await request.InputStream.CopyToAsync(received);

                if (request.HttpMethod == "PUT")
                    _ = Interlocked.Increment(ref _uploadPuts);

                var (status, retryAfter) = _script(request, Interlocked.Increment(ref _requests));
                context.Response.StatusCode = (int)status;
                context.Response.ContentType = "application/json";

                // Starting a resumable upload: point the client at a session on this server.
                if (request.HttpMethod == "POST" && status == HttpStatusCode.OK)
                    context.Response.Headers["Location"] = $"http://127.0.0.1:{request.Url!.Port}/session/{Guid.NewGuid():N}";

                if (retryAfter is not null)
                    context.Response.Headers["Retry-After"] = retryAfter;

                // The SDK validates an upload against the object's CRC32C, so a completed upload reports it.
                var body = status == HttpStatusCode.OK
                    ? "{\"kind\":\"storage#object\",\"bucket\":\"test-bucket\",\"name\":\"test-object.txt\",\"crc32c\":\"" +
                      Crc32C(received.ToArray()) + "\"}"
                    : "{\"error\":{\"code\":" + (int)status + ",\"message\":\"scripted failure\"}}";

                var bytes = Encoding.UTF8.GetBytes(body);
                await context.Response.OutputStream.WriteAsync(bytes);
                context.Response.Close();
            }
        }

        private static string Crc32C(byte[] data)
        {
            var crc = 0xFFFFFFFFu;

            foreach (var b in data)
            {
                crc ^= b;

                for (var i = 0; i < 8; i++)
                {
                    crc = (crc & 1) != 0
                        ? (crc >> 1) ^ 0x82F63B78u
                        : crc >> 1;
                }
            }

            crc ^= 0xFFFFFFFFu;
            return Convert.ToBase64String([(byte)(crc >> 24), (byte)(crc >> 16), (byte)(crc >> 8), (byte)crc]);
        }

        private static int FreePort()
        {
            using var socket = new TcpListener(IPAddress.Loopback, 0);
            socket.Start();
            return ((IPEndPoint)socket.LocalEndpoint).Port;
        }
    }
}
