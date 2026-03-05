using System.Net;
using NPipeline.Connectors.Http.Pagination;

namespace NPipeline.Connectors.Http.Tests.Pagination;

public class LinkHeaderPaginationStrategyTests
{
    private static readonly Uri BaseUri = new("https://api.example.com/items");

    [Fact]
    public async Task GetNextPageUriAsync_WithLinkNextHeader_ReturnsNextUri()
    {
        var strategy = new LinkHeaderPaginationStrategy();
        using var response = new HttpResponseMessage(HttpStatusCode.OK);
        response.Headers.Add("Link", "<https://api.example.com/items?page=2>; rel=\"next\"");

        var next = await strategy.GetNextPageUriAsync(BaseUri, response);

        next.Should().NotBeNull();
        next!.AbsoluteUri.Should().Be("https://api.example.com/items?page=2");
    }

    [Fact]
    public async Task GetNextPageUriAsync_WithMultipleRelations_FindsNextCorrectly()
    {
        var strategy = new LinkHeaderPaginationStrategy();
        using var response = new HttpResponseMessage(HttpStatusCode.OK);

        response.Headers.Add("Link",
            "<https://api.example.com/items?page=1>; rel=\"prev\", <https://api.example.com/items?page=3>; rel=\"next\"");

        var next = await strategy.GetNextPageUriAsync(BaseUri, response);

        next.Should().NotBeNull();
        next!.Query.Should().Contain("page=3");
    }

    [Fact]
    public async Task GetNextPageUriAsync_WhenNoLinkHeader_ReturnsNull()
    {
        var strategy = new LinkHeaderPaginationStrategy();
        using var response = new HttpResponseMessage(HttpStatusCode.OK);

        // No Link header

        var next = await strategy.GetNextPageUriAsync(BaseUri, response);

        next.Should().BeNull();
    }

    [Fact]
    public async Task GetNextPageUriAsync_WhenOnlyPrevRelation_ReturnsNull()
    {
        var strategy = new LinkHeaderPaginationStrategy();
        using var response = new HttpResponseMessage(HttpStatusCode.OK);
        response.Headers.Add("Link", "<https://api.example.com/items?page=1>; rel=\"prev\"");

        var next = await strategy.GetNextPageUriAsync(BaseUri, response);

        next.Should().BeNull();
    }

    [Fact]
    public async Task GetNextPageUriAsync_WithRelativeNextUrl_ResolvesAgainstBase()
    {
        var strategy = new LinkHeaderPaginationStrategy();
        using var response = new HttpResponseMessage(HttpStatusCode.OK);
        response.Headers.Add("Link", "</items?page=2>; rel=\"next\"");

        var next = await strategy.GetNextPageUriAsync(BaseUri, response);

        next.Should().NotBeNull();
        next!.Host.Should().Be("api.example.com");
        next.Query.Should().Contain("page=2");
    }

    [Fact]
    public void BuildFirstPageUri_ReturnsUnchangedBaseUri()
    {
        var strategy = new LinkHeaderPaginationStrategy();

        var uri = strategy.BuildFirstPageUri(BaseUri);

        uri.Should().Be(BaseUri);
    }
}
