using System.Net;
using System.Text;
using AwesomeAssertions;
using NPipeline.StorageProviders.Gcp.Reliability;
using NPipeline.StorageProviders.Gcp.Tests.Support;
using NResilience;

namespace NPipeline.StorageProviders.Gcp.Tests;

public class GcsStreamingReadTests
{
    private static readonly Resilience FastRetry = GcsStorageResilience.Default with { Backoff = Backoff.None };

    private static async Task ServeObjectAsync(HttpListenerContext context, string body, long generation = 5)
    {
        var bytes = Encoding.UTF8.GetBytes(body);
        context.Response.StatusCode = 200;
        context.Response.Headers["x-goog-generation"] = generation.ToString();
        context.Response.ContentLength64 = bytes.Length;
        await context.Response.OutputStream.WriteAsync(bytes);
        context.Response.Close();
    }

    [Fact]
    public async Task OpenReadAsync_ReturnsBeforeObjectIsFullyDownloaded()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var server = new FakeGcsHttpServer(async (context, _) =>
        {
            // The first chunk goes out and is flushed; the rest of the object stalls until the test lets it go.
            var response = context.Response;
            response.SendChunked = true;
            response.Headers["x-goog-generation"] = "5";
            await response.OutputStream.WriteAsync("first-chunk"u8.ToArray());
            await response.OutputStream.FlushAsync();
            await release.Task;
            await response.OutputStream.WriteAsync("|last-chunk"u8.ToArray());
            response.Close();
        });

        var provider = server.CreateProvider(Resilience.None);

        var open = provider.OpenReadAsync(FakeGcsHttpServer.Uri());
        (await Task.WhenAny(open, Task.Delay(TimeSpan.FromSeconds(10)))).Should().BeSameAs(open, "the open must not wait for the body");

        await using var stream = await open;
        var buffer = new byte[11];
        await stream.ReadExactlyAsync(buffer);
        Encoding.UTF8.GetString(buffer).Should().Be("first-chunk");

        release.SetResult();
        using var reader = new StreamReader(stream, Encoding.UTF8);
        (await reader.ReadToEndAsync()).Should().Be("|last-chunk");
    }

    [Fact]
    public async Task OpenReadAsync_ReadsTheWholeObject_WithoutATempFile()
    {
        var before = Directory.GetFiles(Path.GetTempPath(), "gcs-download-*").Length;
        using var server = new FakeGcsHttpServer((context, _) => ServeObjectAsync(context, "hello gcs"));
        var provider = server.CreateProvider(Resilience.None);

        await using (var stream = await provider.OpenReadAsync(FakeGcsHttpServer.Uri()))
        {
            stream.CanSeek.Should().BeFalse();
            using var reader = new StreamReader(stream, Encoding.UTF8);
            (await reader.ReadToEndAsync()).Should().Be("hello gcs");
        }

        Directory.GetFiles(Path.GetTempPath(), "gcs-download-*").Length.Should().Be(before);
        server.Requests.Single().PathAndQuery.Should().Contain("alt=media");
    }

    [Theory]
    [InlineData("gs://bucket/object", "/storage/v1/b/bucket/o/object")]
    [InlineData("gs://bucket/path/to/object", "/storage/v1/b/bucket/o/path%2Fto%2Fobject")]
    public async Task OpenReadAsync_RequestsTheBucketAndObject(string uri, string expectedPath)
    {
        using var server = new FakeGcsHttpServer((context, _) => ServeObjectAsync(context, "x"));
        var provider = server.CreateProvider(Resilience.None);

        await using var stream = await provider.OpenReadAsync(Models.StorageUri.Parse(uri));

        server.Requests.Single().PathAndQuery.Should().StartWith(expectedPath);
    }

    [Fact]
    public async Task OpenReadAsync_WithMissingObject_ThrowsFileNotFoundException()
    {
        using var server = new FakeGcsHttpServer((context, _) => FakeGcsHttpServer.WriteErrorAsync(context.Response, HttpStatusCode.NotFound));
        var provider = server.CreateProvider(FastRetry);

        await Assert.ThrowsAsync<FileNotFoundException>(() => provider.OpenReadAsync(FakeGcsHttpServer.Uri()));
        server.Requests.Should().HaveCount(1, "a missing object is permanent");
    }

    [Theory]
    [InlineData(HttpStatusCode.Unauthorized, typeof(UnauthorizedAccessException))]
    [InlineData(HttpStatusCode.Forbidden, typeof(UnauthorizedAccessException))]
    [InlineData(HttpStatusCode.BadRequest, typeof(ArgumentException))]
    [InlineData(HttpStatusCode.Conflict, typeof(IOException))]
    [InlineData(HttpStatusCode.InternalServerError, typeof(IOException))]
    public async Task OpenReadAsync_TranslatesFailures(HttpStatusCode status, Type expected)
    {
        using var server = new FakeGcsHttpServer((context, _) => FakeGcsHttpServer.WriteErrorAsync(context.Response, status));
        var provider = server.CreateProvider(Resilience.None);

        var exception = await Assert.ThrowsAnyAsync<Exception>(() => provider.OpenReadAsync(FakeGcsHttpServer.Uri()));

        exception.Should().BeOfType(expected);
    }

    [Fact]
    public async Task OpenReadAsync_WithTransientServerError_RetriesTheOpen()
    {
        using var server = new FakeGcsHttpServer((context, n) => n < 3
            ? FakeGcsHttpServer.WriteErrorAsync(context.Response, HttpStatusCode.ServiceUnavailable)
            : ServeObjectAsync(context, "recovered"));

        var provider = server.CreateProvider(FastRetry);

        await using var stream = await provider.OpenReadAsync(FakeGcsHttpServer.Uri());
        using var reader = new StreamReader(stream, Encoding.UTF8);

        (await reader.ReadToEndAsync()).Should().Be("recovered");
        server.Requests.Should().HaveCount(3);
    }

    [Fact]
    public async Task OpenReadAsync_SendsEachRequestOnce_WhenResilienceIsNone()
    {
        // The SDK would retry on its own; the provider's policy is the only retry layer for reads.
        using var server = new FakeGcsHttpServer((context, _) => FakeGcsHttpServer.WriteErrorAsync(context.Response, HttpStatusCode.ServiceUnavailable));
        var provider = server.CreateProvider(Resilience.None);

        await Assert.ThrowsAsync<IOException>(() => provider.OpenReadAsync(FakeGcsHttpServer.Uri()));

        server.Requests.Should().HaveCount(1);
    }

    [Fact]
    public async Task ListAsync_RequestsOnlyTheFieldsItReads_AndSendsEachRequestOnce()
    {
        using var server = new FakeGcsHttpServer((context, _) => FakeGcsHttpServer.WriteJsonAsync(
            context.Response,
            HttpStatusCode.OK,
            "{\"items\":[{\"name\":\"dir/a.txt\",\"size\":\"3\"}],\"prefixes\":[\"dir/sub/\"]}"));

        var provider = server.CreateProvider(Resilience.None);

        var items = new List<Models.StorageItem>();

        await foreach (var item in provider.ListAsync(Models.StorageUri.Parse("gs://test-bucket/dir/")))
        {
            items.Add(item);
        }

        items.Should().ContainSingle(i => !i.IsDirectory).Which.Size.Should().Be(3);
        items.Should().ContainSingle(i => i.IsDirectory);
        Uri.UnescapeDataString(server.Requests.Single().PathAndQuery).Should().Contain("fields=items(name,size,updated),prefixes,nextPageToken");
    }

    [Fact]
    public async Task ListAsync_SendsEachRequestOnce_WhenResilienceIsNone()
    {
        using var server = new FakeGcsHttpServer((context, _) => FakeGcsHttpServer.WriteErrorAsync(context.Response, HttpStatusCode.ServiceUnavailable));
        var provider = server.CreateProvider(Resilience.None);

        await Assert.ThrowsAsync<IOException>(async () =>
        {
            await foreach (var _ in provider.ListAsync(Models.StorageUri.Parse("gs://test-bucket/dir/")))
            {
            }
        });

        server.Requests.Should().HaveCount(1);
    }

    [Fact]
    public async Task Dispose_CanBeCalledRepeatedly_InAnyCombination()
    {
        using var server = new FakeGcsHttpServer((context, _) => ServeObjectAsync(context, "x"));
        var provider = server.CreateProvider(Resilience.None);
        var stream = await provider.OpenReadAsync(FakeGcsHttpServer.Uri());

        stream.Dispose();
        stream.Dispose();
        await stream.DisposeAsync();

        stream.CanRead.Should().BeFalse();
    }
}
