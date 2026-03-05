namespace NPipeline.Connectors.Http.Metrics;

/// <summary>Records operational metrics for the HTTP connector nodes.</summary>
public interface IHttpConnectorMetrics
{
    /// <summary>Records that an outbound HTTP request is about to be sent.</summary>
    void RecordRequest(string endpoint, string method);

    /// <summary>Records that an HTTP response was received.</summary>
    void RecordResponse(string endpoint, string method, int statusCode, TimeSpan latency);

    /// <summary>Records that a request is being retried.</summary>
    void RecordRetry(string endpoint, string method, int attempt);

    /// <summary>Records time spent waiting for the rate limiter to grant a token.</summary>
    void RecordRateLimitWait(string endpoint, TimeSpan waited);

    /// <summary>Records a terminal error for a request.</summary>
    void RecordError(string endpoint, string method, Exception ex);

    /// <summary>Records that a page of items was successfully fetched by the source node.</summary>
    void RecordPageFetched(string endpoint, int itemCount);

    /// <summary>Records that a batch was written successfully by the sink node.</summary>
    void RecordSinkWritten(string endpoint, string method, int statusCode);
}
