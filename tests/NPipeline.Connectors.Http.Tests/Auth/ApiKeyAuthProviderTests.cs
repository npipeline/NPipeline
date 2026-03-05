using NPipeline.Connectors.Http.Auth;

namespace NPipeline.Connectors.Http.Tests.Auth;

public class ApiKeyAuthProviderTests
{
    [Fact]
    public async Task ApplyAsync_AsHeader_InjectsKeyInConfiguredHeaderName()
    {
        var provider = new ApiKeyAuthProvider("X-Api-Key", "secret123");
        var request = new HttpRequestMessage(HttpMethod.Get, "https://api.example.com/items");

        await provider.ApplyAsync(request);

        request.Headers.TryGetValues("X-Api-Key", out var values).Should().BeTrue();
        values.Should().ContainSingle().Which.Should().Be("secret123");
    }

    [Fact]
    public async Task ApplyAsync_AsQueryString_InjectsKeyAsQueryParameter()
    {
        var provider = new ApiKeyAuthProvider("api_key", "secret123", ApiKeyLocation.QueryString);
        var request = new HttpRequestMessage(HttpMethod.Get, "https://api.example.com/items");

        await provider.ApplyAsync(request);

        request.RequestUri!.Query.Should().Contain("api_key=secret123");
    }

    [Fact]
    public async Task ApplyAsync_AsQueryString_PreservesExistingQueryParameters()
    {
        var provider = new ApiKeyAuthProvider("api_key", "secret123", ApiKeyLocation.QueryString);
        var request = new HttpRequestMessage(HttpMethod.Get, "https://api.example.com/items?page=1");

        await provider.ApplyAsync(request);

        var query = request.RequestUri!.Query;
        query.Should().Contain("page=1");
        query.Should().Contain("api_key=secret123");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_WithEmptyKey_ThrowsArgumentException(string key)
    {
        var act = () => new ApiKeyAuthProvider("X-Api-Key", key);
        act.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_WithEmptyParameterName_ThrowsArgumentException(string name)
    {
        var act = () => new ApiKeyAuthProvider(name, "secret");
        act.Should().Throw<ArgumentException>();
    }
}
