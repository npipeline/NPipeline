using System.Net;
using System.Net.Sockets;
using System.Text;
using AwesomeAssertions;
using Google;
using Google.Apis.Auth.OAuth2;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.DependencyInjection;
using NPipeline.StorageProviders.Gcp.Reliability;
using NPipeline.StorageProviders.Models;
using NResilience;

namespace NPipeline.StorageProviders.Gcp.Tests;

/// <summary>Tests the provider contract (list, delete, move, error translation, DI) against a scripted local HTTP server.</summary>
public sealed class GcsStorageProviderContractTests
{
    private static readonly Resilience NoRetry = GcsStorageResilience.Default with { Attempts = 1, Backoff = Backoff.None };

    private static GcsStorageProvider CreateProvider(ScriptedServer server)
    {
        var options = new GcsStorageProviderOptions
        {
            ServiceUrl = server.BaseUri,
            DefaultCredentials = GoogleCredential.FromAccessToken("test-token"),
            Resilience = NoRetry,
        };

        return new GcsStorageProvider(new GcsClientFactory(options), options);
    }

    [Fact]
    public async Task DeleteAsync_MissingKey_Succeeds()
    {
        using var server = new ScriptedServer((_, _) => (HttpStatusCode.NotFound, "{\"error\":{\"code\":404,\"message\":\"No such object\"}}"));
        var provider = CreateProvider(server);

        await provider.DeleteAsync(StorageUri.Parse("gs://bucket/missing.txt"));

        server.Requests.Should().ContainSingle().Which.Should().StartWith("DELETE ");
    }

    [Fact]
    public async Task DeleteAsync_ExistingKey_DeletesIt()
    {
        using var server = new ScriptedServer((_, _) => (HttpStatusCode.NoContent, ""));
        var provider = CreateProvider(server);

        await provider.DeleteAsync(StorageUri.Parse("gs://bucket/dir/file.txt"));

        server.Requests.Should().ContainSingle().Which.Should().Contain("/b/bucket/o/dir%2Ffile.txt");
    }

    [Fact]
    public async Task DeleteAsync_Forbidden_ThrowsUnauthorizedAccess()
    {
        using var server = new ScriptedServer((_, _) => (HttpStatusCode.Forbidden, "{\"error\":{\"code\":403,\"message\":\"denied\"}}"));
        var provider = CreateProvider(server);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => provider.DeleteAsync(StorageUri.Parse("gs://bucket/file.txt")));
    }

    [Fact]
    public async Task MoveAsync_CopiesThenDeletesSource()
    {
        using var server = new ScriptedServer((method, path) => method switch
        {
            "DELETE" => (HttpStatusCode.NoContent, ""),
            _ => (HttpStatusCode.OK, "{\"kind\":\"storage#rewriteResponse\",\"done\":true,\"resource\":{\"bucket\":\"bucket\",\"name\":\"new.txt\"}}"),
        });

        var provider = CreateProvider(server);

        await provider.MoveAsync(StorageUri.Parse("gs://bucket/old.txt"), StorageUri.Parse("gs://bucket/new.txt"));

        server.Requests.Should().HaveCount(2);
        server.Requests[0].Should().StartWith("POST ").And.Contain("old.txt").And.Contain("new.txt");
        server.Requests[1].Should().StartWith("DELETE ").And.Contain("old.txt");
    }

    [Fact]
    public async Task MoveAsync_MissingSource_ThrowsFileNotFound_AndDeletesNothing()
    {
        using var server = new ScriptedServer((_, _) => (HttpStatusCode.NotFound, "{\"error\":{\"code\":404,\"message\":\"No such object\"}}"));
        var provider = CreateProvider(server);

        await Assert.ThrowsAsync<FileNotFoundException>(
            () => provider.MoveAsync(StorageUri.Parse("gs://bucket/old.txt"), StorageUri.Parse("gs://bucket/new.txt")));

        server.Requests.Should().NotContain(r => r.StartsWith("DELETE "));
    }

    [Fact]
    public async Task MoveAsync_ToSameLocation_KeepsTheObject()
    {
        using var server = new ScriptedServer((_, _) => (HttpStatusCode.OK, "{\"kind\":\"storage#object\",\"bucket\":\"bucket\",\"name\":\"same.txt\"}"));
        var provider = CreateProvider(server);

        await provider.MoveAsync(StorageUri.Parse("gs://bucket/same.txt"), StorageUri.Parse("gs://bucket/same.txt"));

        server.Requests.Should().NotContain(r => r.StartsWith("DELETE "));
    }

    [Fact]
    public async Task ListAsync_NonRecursive_YieldsFilesAndPrefixEntriesWithNullPlaceholders()
    {
        using var server = new ScriptedServer((_, _) => (HttpStatusCode.OK,
            "{\"kind\":\"storage#objects\",\"items\":[{\"name\":\"logs/a.txt\",\"size\":\"12\",\"updated\":\"2024-01-02T03:04:05Z\"}," +
            "{\"name\":\"logs/\",\"size\":\"0\"}],\"prefixes\":[\"logs/2024/\"]}"));

        var provider = CreateProvider(server);
        var directory = StorageUri.Parse("gs://bucket/logs/?projectId=p");

        var items = await provider.ListAsync(directory).ToListAsync();

        items.Should().HaveCount(2);
        var file = items.Single(i => !i.IsDirectory);
        file.Uri.ToString().Should().Contain("logs/a.txt").And.Contain("projectId=p");
        file.Size.Should().Be(12);
        file.LastModified.Should().Be(new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero));

        var prefix = items.Single(i => i.IsDirectory);
        prefix.Uri.Path.Should().Be("/logs/2024/");
        prefix.Size.Should().BeNull();
        prefix.LastModified.Should().BeNull();

        server.Requests.Single().Should().Contain("prefix=logs%2F").And.Contain("delimiter=%2F");
    }

    [Fact]
    public async Task ListAsync_Recursive_UsesNoDelimiterAndYieldsNoDirectories()
    {
        using var server = new ScriptedServer((_, _) => (HttpStatusCode.OK,
            "{\"kind\":\"storage#objects\",\"items\":[{\"name\":\"logs/a/b.txt\",\"size\":\"1\"}]}"));

        var provider = CreateProvider(server);

        var items = await provider.ListAsync(StorageUri.Parse("gs://bucket/logs"), true).ToListAsync();

        items.Should().ContainSingle().Which.IsDirectory.Should().BeFalse();
        server.Requests.Single().Should().Contain("prefix=logs%2F").And.NotContain("delimiter");
    }

    [Fact]
    public async Task ListAsync_MissingBucket_YieldsNothing()
    {
        using var server = new ScriptedServer((_, _) => (HttpStatusCode.NotFound, "{\"error\":{\"code\":404,\"message\":\"no bucket\"}}"));
        var provider = CreateProvider(server);

        var items = await provider.ListAsync(StorageUri.Parse("gs://nope/")).ToListAsync();

        items.Should().BeEmpty();
    }

    [Theory]
    [InlineData(HttpStatusCode.Unauthorized, typeof(UnauthorizedAccessException))]
    [InlineData(HttpStatusCode.Forbidden, typeof(UnauthorizedAccessException))]
    [InlineData(HttpStatusCode.NotFound, typeof(FileNotFoundException))]
    [InlineData(HttpStatusCode.BadRequest, typeof(ArgumentException))]
    [InlineData(HttpStatusCode.Conflict, typeof(IOException))]
    [InlineData(HttpStatusCode.InternalServerError, typeof(IOException))]
    public void Translate_MapsStatusToContractException(HttpStatusCode status, Type expected)
    {
        var source = new GoogleApiException("storage", "boom") { HttpStatusCode = status };

        var translated = GcsErrors.Translate(source, "bucket", "key", "read");

        translated.GetType().Should().Be(expected);
        translated.InnerException.Should().BeSameAs(source);
    }

    [Fact]
    public void Translate_UsesErrorReasonWhenStatusIsUndecided()
    {
        var source = new GoogleApiException("storage", "boom")
        {
            HttpStatusCode = HttpStatusCode.PreconditionFailed,
            Error = new Google.Apis.Requests.RequestError
            {
                Errors = [new Google.Apis.Requests.SingleError { Reason = "forbidden" }],
            },
        };

        GcsErrors.Translate(source, "bucket", "key", "read").Should().BeOfType<UnauthorizedAccessException>();
    }

    [Fact]
    public void Translate_CopiesExceptionData()
    {
        var source = new GoogleApiException("storage", "boom") { HttpStatusCode = HttpStatusCode.InternalServerError };
        source.Data["marker"] = "retried";

        GcsErrors.Translate(source, "bucket", "key", "read").Data["marker"].Should().Be("retried");
    }

    [Fact]
    public void AddGcsStorageProvider_Twice_RegistersOneProvider()
    {
        var services = new ServiceCollection();

        _ = services.AddGcsStorageProvider().AddGcsStorageProvider();

        services.Count(d => d.ServiceType == typeof(IStorageProvider)).Should().Be(1);
    }

    [Fact]
    public void AddGcsStorageProvider_ProviderIsResolvableThroughIStorageResolver()
    {
        var services = new ServiceCollection();
        _ = services.AddGcsStorageProvider().AddStorageResolver(false);

        using var serviceProvider = services.BuildServiceProvider();
        var resolver = serviceProvider.GetRequiredService<IStorageResolver>();

        resolver.Resolve(StorageUri.Parse("gs://bucket/object")).Should().BeOfType<GcsStorageProvider>();
    }

    /// <summary>A local HTTP server that records each request and answers it with the scripted status and JSON body.</summary>
    private sealed class ScriptedServer : IDisposable
    {
        private readonly HttpListener _listener = new();
        private readonly Func<string, string, (HttpStatusCode Status, string Body)> _script;
        private readonly List<string> _requests = [];

        public ScriptedServer(Func<string, string, (HttpStatusCode Status, string Body)> script)
        {
            _script = script;
            var port = FreePort();
            BaseUri = new Uri($"http://127.0.0.1:{port}/storage/v1/");
            _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
            _listener.Start();
            _ = Task.Run(ServeAsync);
        }

        public Uri BaseUri { get; }

        /// <summary>Each request as "METHOD raw-url".</summary>
        public IReadOnlyList<string> Requests
        {
            get
            {
                lock (_requests)
                    return [.. _requests];
            }
        }

        public void Dispose() => _listener.Close();

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
                using var received = new MemoryStream();
                await request.InputStream.CopyToAsync(received);

                lock (_requests)
                    _requests.Add($"{request.HttpMethod} {request.RawUrl}");

                var (status, body) = _script(request.HttpMethod, request.RawUrl ?? string.Empty);
                context.Response.StatusCode = (int)status;
                context.Response.ContentType = "application/json";
                var bytes = Encoding.UTF8.GetBytes(body);
                await context.Response.OutputStream.WriteAsync(bytes);
                context.Response.Close();
            }
        }

        private static int FreePort()
        {
            using var socket = new TcpListener(IPAddress.Loopback, 0);
            socket.Start();
            return ((IPEndPoint)socket.LocalEndpoint).Port;
        }
    }
}
