using System.Net;
using System.Text;
using System.Web;
using NPipeline.Connectors.Http.Pagination;

namespace NPipeline.Connectors.Http.Tests.Pagination;

public class CursorPaginationStrategyTests
{
    private static readonly Uri BaseUri = new("https://api.example.com/items");

    [Fact]
    public async Task GetNextPageUriAsync_WithCursorInResponse_AppendsCursorToNextUri()
    {
        var strategy = new CursorPaginationStrategy(new CursorPaginationOptions
        {
            CursorJsonPath = "next_cursor",
        });

        var body = """{"data":[{"id":1}],"next_cursor":"abc123"}""";

        using var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

        var next = await strategy.GetNextPageUriAsync(BaseUri, response);

        next.Should().NotBeNull();
        next!.Query.Should().Contain("cursor=abc123");
    }

    [Fact]
    public async Task GetNextPageUriAsync_WithNestedCursorPath_ExtractsCursorCorrectly()
    {
        var strategy = new CursorPaginationStrategy(new CursorPaginationOptions
        {
            CursorJsonPath = "pagination.next_cursor",
        });

        var body = """{"data":[],"pagination":{"next_cursor":"page2token"}}""";

        using var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

        var next = await strategy.GetNextPageUriAsync(BaseUri, response);

        next.Should().NotBeNull();
        next!.Query.Should().Contain("cursor=page2token");
    }

    [Fact]
    public async Task GetNextPageUriAsync_WhenCursorMissing_ReturnsNull()
    {
        var strategy = new CursorPaginationStrategy(new CursorPaginationOptions
        {
            CursorJsonPath = "next_cursor",
        });

        var body = """{"data":[{"id":1}]}"""; // no next_cursor field

        using var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

        var next = await strategy.GetNextPageUriAsync(BaseUri, response);

        next.Should().BeNull();
    }

    [Fact]
    public async Task GetNextPageUriAsync_WhenCursorIsNull_ReturnsNull()
    {
        var strategy = new CursorPaginationStrategy(new CursorPaginationOptions
        {
            CursorJsonPath = "next_cursor",
        });

        var body = """{"data":[],"next_cursor":null}""";

        using var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

        var next = await strategy.GetNextPageUriAsync(BaseUri, response);

        next.Should().BeNull();
    }

    [Fact]
    public async Task GetNextPageUriAsync_WithMalformedJson_ReturnsNull()
    {
        var strategy = new CursorPaginationStrategy(new CursorPaginationOptions
        {
            CursorJsonPath = "next_cursor",
        });

        using var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("not-json", Encoding.UTF8, "application/json"),
        };

        var next = await strategy.GetNextPageUriAsync(BaseUri, response);

        next.Should().BeNull();
    }

    [Fact]
    public void BuildFirstPageUri_WithPageSizeConfig_AppendsPageSizeParam()
    {
        var strategy = new CursorPaginationStrategy(new CursorPaginationOptions
        {
            CursorJsonPath = "next_cursor",
            PageSizeParam = "limit",
            PageSize = 50,
        });

        var uri = strategy.BuildFirstPageUri(BaseUri);

        var query = HttpUtility.ParseQueryString(uri.Query);
        query["limit"].Should().Be("50");
    }
}
