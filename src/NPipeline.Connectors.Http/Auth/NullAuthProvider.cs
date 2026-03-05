namespace NPipeline.Connectors.Http.Auth;

/// <summary>A no-op <see cref="IHttpAuthProvider" /> that applies no credentials.</summary>
public sealed class NullAuthProvider : IHttpAuthProvider
{
    /// <summary>Gets the singleton instance.</summary>
    public static readonly NullAuthProvider Instance = new();

    private NullAuthProvider()
    {
    }

    /// <inheritdoc />
    public ValueTask ApplyAsync(HttpRequestMessage request, CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }
}
