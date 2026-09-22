using System.Net;

namespace NPipeline.Extensions.AI.Decisions.Jev;

/// <summary>Represents an unsuccessful response from the TypeSafe API.</summary>
public sealed class JevApiException : AIInvocationException
{
    internal JevApiException(
        HttpStatusCode statusCode,
        string message,
        string? responseBody,
        string? model,
        string? requestId,
        IReadOnlyDictionary<string, IReadOnlyList<string>> headers)
        : base(message)
    {
        StatusCode = statusCode;
        ResponseBody = responseBody;
        Headers = headers;
        Provider = "typesafe";
        Model = model;
        RequestId = requestId;
    }

    /// <summary>Gets the HTTP status code.</summary>
    public HttpStatusCode StatusCode { get; }

    /// <summary>Gets the unmodified error response body, when present.</summary>
    public string? ResponseBody { get; }

    /// <summary>Gets the response headers.</summary>
    public IReadOnlyDictionary<string, IReadOnlyList<string>> Headers { get; }
}
