using System.Net;
using System.Text;
using System.Text.Json;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Retry;
using NPipeline.Connectors.Http.Tests.Helpers;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Http.Tests.Nodes;

public class HttpSinkNodeTests
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private static HttpClient CreateClient(MockHttpMessageHandler handler)
    {
        return new HttpClient(handler);
    }

    private static DataStream<T> PipeOf<T>(params T[] items)
    {
        async IAsyncEnumerable<T> Generate()
        {
            foreach (var item in items)
            {
                yield return item;
            }

            await Task.CompletedTask;
        }

        return new DataStream<T>(Generate(), "test");
    }

    [Fact]
    public async Task ExecuteAsync_SingleItemPost_SendsJsonBodyToConfiguredUri()
    {
        var handler = new MockHttpMessageHandler().Respond(HttpStatusCode.Created);
        using var httpClient = CreateClient(handler);

        var config = new HttpSinkConfiguration { Uri = new Uri("https://api.example.com/items") };
        var node = new HttpSinkNode<Item>(config, httpClient);

        await using var pipe = PipeOf(new Item(1, "Apple"));
        await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        handler.Requests.Should().HaveCount(1);
        var req = handler.Requests[0];
        req.Method.Should().Be(HttpMethod.Post);
        req.RequestUri!.AbsoluteUri.Should().Be("https://api.example.com/items");

        var body = handler.RequestBodies[0]!;
        var item = JsonSerializer.Deserialize<Item>(body, JsonOptions);
        item!.Id.Should().Be(1);
        item.Name.Should().Be("Apple");
    }

    [Fact]
    public async Task ExecuteAsync_PutMethod_UsesPutHttpMethod()
    {
        var handler = new MockHttpMessageHandler().Respond(HttpStatusCode.OK);
        using var httpClient = CreateClient(handler);

        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com/items/1"),
            Method = SinkHttpMethod.Put,
        };

        var node = new HttpSinkNode<Item>(config, httpClient);

        await using var pipe = PipeOf(new Item(1, "Apple"));
        await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        handler.Requests[0].Method.Should().Be(HttpMethod.Put);
    }

    [Fact]
    public async Task ExecuteAsync_PatchMethod_UsesPatchHttpMethod()
    {
        var handler = new MockHttpMessageHandler().Respond(HttpStatusCode.OK);
        using var httpClient = CreateClient(handler);

        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com/items/1"),
            Method = SinkHttpMethod.Patch,
        };

        var node = new HttpSinkNode<Item>(config, httpClient);

        await using var pipe = PipeOf(new Item(1, "Apple"));
        await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        handler.Requests[0].Method.Should().Be(HttpMethod.Patch);
    }

    [Fact]
    public async Task ExecuteAsync_WithUriFactory_CallsFactoryPerItem()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.OK)
            .Respond(HttpStatusCode.OK);

        using var httpClient = CreateClient(handler);

        var config = new HttpSinkConfiguration
        {
            UriFactory = item => new Uri($"https://api.example.com/items/{((Item)item).Id}"),
        };

        var node = new HttpSinkNode<Item>(config, httpClient);

        await using var pipe = PipeOf(new Item(1, "Apple"), new Item(2, "Banana"));
        await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        handler.Requests[0].RequestUri!.AbsolutePath.Should().Be("/items/1");
        handler.Requests[1].RequestUri!.AbsolutePath.Should().Be("/items/2");
    }

    [Fact]
    public async Task ExecuteAsync_WithTypedUriFactoryConstructor_UsesTypedFactory()
    {
        var handler = new MockHttpMessageHandler().Respond(HttpStatusCode.OK);
        using var httpClient = CreateClient(handler);
        var httpClientFactory = new MockHttpClientFactory(httpClient);

        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com/fallback"),
        };

        var node = new HttpSinkNode<Item>(
            config,
            item => new Uri($"https://api.example.com/items/{item.Id}"),
            httpClientFactory);

        await using var pipe = PipeOf(new Item(42, "Answer"));
        await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        handler.Requests.Should().HaveCount(1);
        handler.Requests[0].RequestUri!.AbsolutePath.Should().Be("/items/42");
    }

    [Fact]
    public async Task ExecuteAsync_WithBatching_GroupsItemsIntoBatches()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.OK)
            .Respond(HttpStatusCode.OK);

        using var httpClient = CreateClient(handler);

        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com/items"),
            BatchSize = 3,
        };

        var node = new HttpSinkNode<Item>(config, httpClient);

        // 5 items → 2 requests: [3] + [2]
        await using var pipe = PipeOf(
            new Item(1, "A"),
            new Item(2, "B"),
            new Item(3, "C"),
            new Item(4, "D"),
            new Item(5, "E"));

        await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        handler.Requests.Should().HaveCount(2);
        var body1 = handler.RequestBodies[0]!;
        var batch1 = JsonSerializer.Deserialize<Item[]>(body1, JsonOptions);
        batch1.Should().HaveCount(3);
    }

    [Fact]
    public async Task ExecuteAsync_WithEmptyPipe_SendsNoRequests()
    {
        var handler = new MockHttpMessageHandler();
        using var httpClient = CreateClient(handler);

        var config = new HttpSinkConfiguration { Uri = new Uri("https://api.example.com/items") };
        var node = new HttpSinkNode<Item>(config, httpClient);

        await using var pipe = PipeOf<Item>();
        await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        handler.Requests.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteAsync_NonSuccessResponse_ThrowsHttpRequestException()
    {
        var handler = new MockHttpMessageHandler().Respond(HttpStatusCode.BadRequest, "\"invalid request\"");
        using var httpClient = CreateClient(handler);

        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com/items"),
            RetryStrategy = new ExponentialBackoffHttpRetryStrategy { MaxRetries = 0 },
        };

        var node = new HttpSinkNode<Item>(config, httpClient);

        await using var pipe = PipeOf(new Item(1, "Apple"));
        var act = async () => await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        await act.Should().ThrowAsync<HttpRequestException>();
    }

    [Fact]
    public async Task ExecuteAsync_WithCaptureErrorResponses_DoesNotThrow()
    {
        var handler = new MockHttpMessageHandler().Respond(HttpStatusCode.BadRequest, "\"error\"");
        using var httpClient = CreateClient(handler);

        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com/items"),
            CaptureErrorResponses = true,
            RetryStrategy = new ExponentialBackoffHttpRetryStrategy { MaxRetries = 0 },
        };

        var node = new HttpSinkNode<Item>(config, httpClient);

        await using var pipe = PipeOf(new Item(1, "Apple"));
        var act = async () => await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ExecuteAsync_WithBatchWrapperKey_WrapsArrayInObject()
    {
        var handler = new MockHttpMessageHandler().Respond(HttpStatusCode.OK);
        using var httpClient = CreateClient(handler);

        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com/items"),
            BatchSize = 2,
            BatchWrapperKey = "items",
        };

        var node = new HttpSinkNode<Item>(config, httpClient);

        await using var pipe = PipeOf(new Item(1, "A"), new Item(2, "B"));
        await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        var body = handler.RequestBodies[0]!;
        using var doc = JsonDocument.Parse(body);
        doc.RootElement.TryGetProperty("items", out var itemsEl).Should().BeTrue();
        itemsEl.GetArrayLength().Should().Be(2);
    }

    [Fact]
    public async Task ExecuteAsync_WhenRequestExceedsTimeout_ThrowsTaskCanceledException()
    {
        using var httpClient = new HttpClient(new DelayedResponseHandler(TimeSpan.FromMilliseconds(250)));

        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com/items"),
            Timeout = TimeSpan.FromMilliseconds(50),
            RetryStrategy = new ExponentialBackoffHttpRetryStrategy { MaxRetries = 0 },
        };

        var node = new HttpSinkNode<Item>(config, httpClient);

        await using var pipe = PipeOf(new Item(1, "slow"));
        var act = async () => await node.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        await act.Should().ThrowAsync<TaskCanceledException>();
    }

    private sealed record Item(int Id, string Name);

    private sealed class DelayedResponseHandler(TimeSpan delay) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}", Encoding.UTF8, "application/json"),
            };
        }
    }

    private sealed class MockHttpClientFactory(HttpClient client) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name)
        {
            return client;
        }
    }
}
