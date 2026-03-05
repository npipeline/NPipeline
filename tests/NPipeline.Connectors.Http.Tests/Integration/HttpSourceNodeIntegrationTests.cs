using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Pagination;
using NPipeline.Connectors.Http.Tests.Fixtures;
using NPipeline.Pipeline;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;

namespace NPipeline.Connectors.Http.Tests.Integration;

[Collection("Http")]
public class HttpSourceNodeIntegrationTests(WireMockFixture fixture)
{
    [Fact]
    public async Task HttpSourceNode_FetchesSinglePage_ReturnsAllItems()
    {
        fixture.Server.Reset();

        fixture.Server
            .Given(Request.Create().WithPath("/items").UsingGet())
            .RespondWith(Response.Create()
                .WithStatusCode(200)
                .WithHeader("Content-Type", "application/json")
                .WithBody("""[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]"""));

        using var httpClient = new HttpClient();

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri($"{fixture.BaseUrl}/items"),
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var items = new List<Item>();

        await foreach (var item in node.Initialize(new PipelineContext(), CancellationToken.None))
        {
            items.Add(item);
        }

        items.Should().HaveCount(2);
        items[0].Name.Should().Be("Alice");
    }

    [Fact]
    public async Task HttpSourceNode_WithLinkHeaderPagination_FetchesAllPages()
    {
        fixture.Server.Reset();

        fixture.Server
            .Given(Request.Create().WithPath("/paged").WithParam("page", "1").UsingGet())
            .RespondWith(Response.Create()
                .WithStatusCode(200)
                .WithHeader("Content-Type", "application/json")
                .WithHeader("Link", $"<{fixture.BaseUrl}/paged?page=2>; rel=\"next\"")
                .WithBody("""[{"id":1,"name":"A"},{"id":2,"name":"B"}]"""));

        fixture.Server
            .Given(Request.Create().WithPath("/paged").WithParam("page", "2").UsingGet())
            .RespondWith(Response.Create()
                .WithStatusCode(200)
                .WithHeader("Content-Type", "application/json")
                .WithBody("""[{"id":3,"name":"C"}]"""));

        using var httpClient = new HttpClient();

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri($"{fixture.BaseUrl}/paged?page=1"),
            Pagination = new LinkHeaderPaginationStrategy(),
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var items = new List<Item>();

        await foreach (var item in node.Initialize(new PipelineContext(), CancellationToken.None))
        {
            items.Add(item);
        }

        items.Should().HaveCount(3);
    }

    [Fact]
    public async Task HttpSourceNode_WithBearerAuth_IncludesAuthorizationHeader()
    {
        fixture.Server.Reset();

        fixture.Server
            .Given(Request.Create()
                .WithPath("/secure")
                .WithHeader("Authorization", "Bearer my-token")
                .UsingGet())
            .RespondWith(Response.Create()
                .WithStatusCode(200)
                .WithHeader("Content-Type", "application/json")
                .WithBody("""[{"id":1,"name":"SecureItem"}]"""));

        // Without valid auth → 401
        fixture.Server
            .Given(Request.Create().WithPath("/secure").UsingGet())
            .RespondWith(Response.Create().WithStatusCode(401));

        using var httpClient = new HttpClient();

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri($"{fixture.BaseUrl}/secure"),
            Auth = new BearerTokenAuthProvider("my-token"),
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var items = new List<Item>();

        await foreach (var item in node.Initialize(new PipelineContext(), CancellationToken.None))
        {
            items.Add(item);
        }

        items.Should().HaveCount(1);
        items[0].Name.Should().Be("SecureItem");
    }

    [Fact]
    public async Task HttpSourceNode_WithNestedItemsPath_ExtractsCorrectly()
    {
        fixture.Server.Reset();

        fixture.Server
            .Given(Request.Create().WithPath("/nested").UsingGet())
            .RespondWith(Response.Create()
                .WithStatusCode(200)
                .WithHeader("Content-Type", "application/json")
                .WithBody("""{"meta":{"total":1},"results":[{"id":99,"name":"Nested"}]}"""));

        using var httpClient = new HttpClient();

        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri($"{fixture.BaseUrl}/nested"),
            ItemsJsonPath = "results",
        };

        var node = new HttpSourceNode<Item>(config, httpClient);

        var items = new List<Item>();

        await foreach (var item in node.Initialize(new PipelineContext(), CancellationToken.None))
        {
            items.Add(item);
        }

        items.Should().HaveCount(1);
        items[0].Id.Should().Be(99);
    }

    private record Item(int Id, string Name);
}
