using System.Text;
using NPipeline.Connectors.Http.Auth;

namespace NPipeline.Connectors.Http.Tests.Auth;

public class BasicAuthProviderTests
{
    [Fact]
    public async Task ApplyAsync_AddsCorrectBase64EncodedAuthorizationHeader()
    {
        var provider = new BasicAuthProvider("alice", "p@ssw0rd");
        var request = new HttpRequestMessage(HttpMethod.Get, "https://api.example.com/items");

        await provider.ApplyAsync(request);

        var expected = Convert.ToBase64String(Encoding.UTF8.GetBytes("alice:p@ssw0rd"));
        request.Headers.Authorization.Should().NotBeNull();
        request.Headers.Authorization!.Scheme.Should().Be("Basic");
        request.Headers.Authorization.Parameter.Should().Be(expected);
    }

    [Fact]
    public async Task ApplyAsync_WithSpecialCharacters_EncodesCorrectly()
    {
        var username = "user@domain.com";
        var password = "P@$$w0rd!";
        var provider = new BasicAuthProvider(username, password);
        var request = new HttpRequestMessage(HttpMethod.Get, "https://api.example.com/items");

        await provider.ApplyAsync(request);

        var expected = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
        request.Headers.Authorization!.Parameter.Should().Be(expected);
    }

    [Fact]
    public async Task ApplyAsync_WithEmptyPassword_AppliesHeaderWithColonSeparator()
    {
        var provider = new BasicAuthProvider("admin", "");
        var request = new HttpRequestMessage(HttpMethod.Get, "https://api.example.com/items");

        await provider.ApplyAsync(request);

        var expected = Convert.ToBase64String(Encoding.UTF8.GetBytes("admin:"));
        request.Headers.Authorization!.Parameter.Should().Be(expected);
    }
}
