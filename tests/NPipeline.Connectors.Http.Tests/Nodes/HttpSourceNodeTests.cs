using System.Net;
using System.Text;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Pagination;
using NPipeline.Connectors.Http.Retry;
using NPipeline.Connectors.Http.Tests.Helpers;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Http.Tests.Nodes;

public class HttpSourceNodeTests
{
    private static HttpClient CreateClient(MockHttpMessageHandler handler)
    {
        return new HttpClient(handler) { BaseAddress = null };
    }

    [Fact]
    public async Task Initialize_WithRootJsonArray_YieldsAllItems()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.OK, """[{"id":1,"name":"Alpha"},{"id":2,"name":"Beta"}]""");

        using var httpClient = CreateClient(handler);

        var config = new HttpSourceConfiguration { BaseUri = new Uri("https://api.example.com/items") };
        var node = new HttpSourceNode<Item>(config, httpClient);

        var pipe = node.OpenStream(new PipelineContext(), CancellationToken.None);
        var items = new List<Item>();

        await foreach (var item in pipe)
        {
            items.Add(item);
        }

        items.Should().HaveCount(2);
        items[0].Id.Should().Be(1);
        items[1].Name.Should().Be("Beta");
    }

    [Fact]
    public async Task Initialize_WithItemsJsonPath_ExtractsItemsFromNestedPath()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.OK, """{"data":[{"id":10,"name":"X"},{"id":11,"name":"Y"}],"total":2}""");

        using var httpClient = CreateClient(handler);

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com/items"),
            ItemsJsonPath = "data",
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var pipe = node.OpenStream(new PipelineContext(), CancellationToken.None);
        var items = new List<Item>();

        await foreach (var item in pipe)
        {
            items.Add(item);
        }

        items.Should().HaveCount(2);
        items[0].Id.Should().Be(10);
    }

    [Fact]
    public async Task Initialize_WithJsonPathPrefix_ExtractsItemsFromNestedPath()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.OK, """{"data":[{"id":10,"name":"X"}],"total":1}""");

        using var httpClient = CreateClient(handler);

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com/items"),
            ItemsJsonPath = "$.data",
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var pipe = node.OpenStream(new PipelineContext(), CancellationToken.None);
        var items = new List<Item>();

        await foreach (var item in pipe)
        {
            items.Add(item);
        }

        items.Should().HaveCount(1);
        items[0].Id.Should().Be(10);
    }

    [Fact]
    public async Task Initialize_WithOffsetPagination_FetchesMultiplePages()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.OK, """[{"id":1,"name":"A"},{"id":2,"name":"B"}]""")
            .Respond(HttpStatusCode.OK, """[{"id":3,"name":"C"}]"""); // short page → stop

        using var httpClient = CreateClient(handler);

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com/items"),
            Pagination = new OffsetPaginationStrategy(new OffsetPaginationOptions { PageSize = 2 }),
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var pipe = node.OpenStream(new PipelineContext(), CancellationToken.None);
        var items = new List<Item>();

        await foreach (var item in pipe)
        {
            items.Add(item);
        }

        items.Should().HaveCount(3);
        handler.Requests.Should().HaveCount(2);
    }

    [Fact]
    public async Task Initialize_WithLinkHeaderPagination_FollowsNextLinks()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(req =>
            {
                var response = new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("""[{"id":1,"name":"A"}]""",
                        Encoding.UTF8, "application/json"),
                };

                response.Headers.Add("Link", "<https://api.example.com/items?page=2>; rel=\"next\"");
                return response;
            })
            .Respond(HttpStatusCode.OK, """[{"id":2,"name":"B"}]"""); // no Link header → stop

        using var httpClient = CreateClient(handler);

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com/items"),
            Pagination = new LinkHeaderPaginationStrategy(),
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var items = new List<Item>();

        await foreach (var item in node.OpenStream(new PipelineContext(), CancellationToken.None))
        {
            items.Add(item);
        }

        items.Should().HaveCount(2);
    }

    [Fact]
    public async Task Initialize_WithMaxPages_StopsAfterLimit()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(req =>
            {
                var resp = new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("""[{"id":1,"name":"A"},{"id":2,"name":"B"}]""",
                        Encoding.UTF8, "application/json"),
                };

                resp.Headers.Add("Link", "<https://api.example.com/items?page=2>; rel=\"next\"");
                return resp;
            })
            .Respond(req =>
            {
                var resp = new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("""[{"id":3,"name":"C"}]""",
                        Encoding.UTF8, "application/json"),
                };

                resp.Headers.Add("Link", "<https://api.example.com/items?page=3>; rel=\"next\"");
                return resp;
            });

        using var httpClient = CreateClient(handler);

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com/items"),
            Pagination = new LinkHeaderPaginationStrategy(),
            MaxPages = 1,
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var items = new List<Item>();

        await foreach (var item in node.OpenStream(new PipelineContext(), CancellationToken.None))
        {
            items.Add(item);
        }

        // Only page 1 fetched (MaxPages=1), then check happens at start of second iteration
        items.Should().HaveCount(2);
        handler.Requests.Should().HaveCount(1);
    }

    [Fact]
    public async Task Initialize_WithNonRetriableError_ThrowsHttpRequestException()
    {
        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.Unauthorized);

        using var httpClient = CreateClient(handler);

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com/items"),
            RetryStrategy = new ExponentialBackoffHttpRetryStrategy { MaxRetries = 0 },
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var act = async () =>
        {
            await foreach (var _ in node.OpenStream(new PipelineContext(), CancellationToken.None))
            {
            }
        };

        await act.Should().ThrowAsync<HttpRequestException>();
    }

    [Fact]
    public async Task Initialize_WithCancellation_PropagatesCancellation()
    {
        using var cts = new CancellationTokenSource();

        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.OK, """[{"id":1,"name":"A"}]""");

        using var httpClient = CreateClient(handler);

        var config = new HttpSourceConfiguration { BaseUri = new Uri("https://api.example.com/items") };
        var node = new HttpSourceNode<Item>(config, httpClient);

        cts.Cancel();

        var act = async () =>
        {
            await foreach (var _ in node.OpenStream(new PipelineContext(), cts.Token))
            {
            }
        };

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Initialize_WhenRequestExceedsTimeout_ThrowsTaskCanceledException()
    {
        using var httpClient = new HttpClient(new DelayedResponseHandler(TimeSpan.FromMilliseconds(250)));

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com/items"),
            Timeout = TimeSpan.FromMilliseconds(50),
            RetryStrategy = new ExponentialBackoffHttpRetryStrategy { MaxRetries = 0 },
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var act = async () =>
        {
            await foreach (var _ in node.OpenStream(new PipelineContext(), CancellationToken.None))
            {
            }
        };

        await act.Should().ThrowAsync<TaskCanceledException>();
    }

    [Fact]
    public async Task Initialize_RequestCustomizerHook_InvokedForEachRequest()
    {
        var customHeaders = new List<string>();

        var handler = new MockHttpMessageHandler()
            .Respond(HttpStatusCode.OK, """[{"id":1,"name":"A"}]""");

        using var httpClient = CreateClient(handler);

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com/items"),
            RequestCustomizer = (req, _) =>
            {
                req.Headers.TryAddWithoutValidation("X-Correlation-Id", "test-123");
                customHeaders.Add("X-Correlation-Id");
                return ValueTask.CompletedTask;
            },
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        await foreach (var _ in node.OpenStream(new PipelineContext(), CancellationToken.None))
        {
        }

        customHeaders.Should().ContainSingle();
        handler.Requests[0].Headers.TryGetValues("X-Correlation-Id", out var vals).Should().BeTrue();
        vals.Should().ContainSingle().Which.Should().Be("test-123");
    }

    private sealed record Item(int Id, string Name);

    private sealed class DelayedResponseHandler(TimeSpan delay) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("[]", Encoding.UTF8, "application/json"),
            };
        }
    }
}
