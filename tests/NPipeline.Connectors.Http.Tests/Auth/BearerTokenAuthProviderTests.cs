using NPipeline.Connectors.Http.Auth;

namespace NPipeline.Connectors.Http.Tests.Auth;

public class BearerTokenAuthProviderTests
{
    [Fact]
    public async Task ApplyAsync_WithStaticToken_AddsAuthorizationBearerHeader()
    {
        var provider = new BearerTokenAuthProvider("my-secret-token");
        var request = new HttpRequestMessage(HttpMethod.Get, "https://api.example.com/items");

        await provider.ApplyAsync(request);

        request.Headers.Authorization.Should().NotBeNull();
        request.Headers.Authorization!.Scheme.Should().Be("Bearer");
        request.Headers.Authorization.Parameter.Should().Be("my-secret-token");
    }

    [Fact]
    public async Task ApplyAsync_WithTokenFactory_InvokesFactoryPerRequest()
    {
        var callCount = 0;

        var provider = new BearerTokenAuthProvider(_ =>
        {
            callCount++;
            return ValueTask.FromResult($"token-{callCount}");
        });

        var request1 = new HttpRequestMessage(HttpMethod.Get, "https://api.example.com/items");
        var request2 = new HttpRequestMessage(HttpMethod.Get, "https://api.example.com/items");

        await provider.ApplyAsync(request1);
        await provider.ApplyAsync(request2);

        callCount.Should().Be(2);
        request1.Headers.Authorization!.Parameter.Should().Be("token-1");
        request2.Headers.Authorization!.Parameter.Should().Be("token-2");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_WithEmptyToken_ThrowsArgumentException(string token)
    {
        var act = () => new BearerTokenAuthProvider(token);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithNullToken_ThrowsArgumentException()
    {
        var act = () => new BearerTokenAuthProvider((string)null!);
        act.Should().Throw<ArgumentException>();
    }
}
