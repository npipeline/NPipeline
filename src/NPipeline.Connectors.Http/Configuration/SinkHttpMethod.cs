namespace NPipeline.Connectors.Http.Configuration;

/// <summary>Specifies the HTTP method used when the sink writes data.</summary>
public enum SinkHttpMethod
{
    /// <summary>HTTP POST — typically used to create resources.</summary>
    Post,

    /// <summary>HTTP PUT — typically used to replace an existing resource.</summary>
    Put,

    /// <summary>HTTP PATCH — typically used for partial resource updates.</summary>
    Patch,
}
