namespace NPipeline.Connectors.Http.Models;

/// <summary>Captures the result of a single HTTP sink write operation.</summary>
public sealed class HttpSinkResult
{
    /// <summary>Gets the HTTP status code returned by the server.</summary>
    public required int StatusCode { get; init; }

    /// <summary>Gets whether the request was considered successful (2xx status).</summary>
    public bool IsSuccess => StatusCode is >= 200 and <= 299;

    /// <summary>Gets the response body, if the response was captured.</summary>
    public string? ResponseBody { get; init; }

    /// <summary>Gets the URI that was targeted.</summary>
    public required Uri RequestUri { get; init; }

    /// <summary>Gets the HTTP method used.</summary>
    public required string Method { get; init; }
}
