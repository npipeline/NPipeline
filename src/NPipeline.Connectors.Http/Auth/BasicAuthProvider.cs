using System.Net.Http.Headers;
using System.Text;

namespace NPipeline.Connectors.Http.Auth;

/// <summary>
///     Applies HTTP Basic authentication by Base64-encoding <c>username:password</c>
///     into the <c>Authorization: Basic</c> header.
/// </summary>
public sealed class BasicAuthProvider : IHttpAuthProvider
{
    private readonly string _encodedCredentials;

    /// <summary>
    ///     Creates a new instance with the specified credentials.
    /// </summary>
    /// <param name="username">The username.</param>
    /// <param name="password">The password.</param>
    public BasicAuthProvider(string username, string password)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(username);
        ArgumentNullException.ThrowIfNull(password);

        _encodedCredentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
    }

    /// <inheritdoc />
    public ValueTask ApplyAsync(HttpRequestMessage request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", _encodedCredentials);
        return ValueTask.CompletedTask;
    }
}
