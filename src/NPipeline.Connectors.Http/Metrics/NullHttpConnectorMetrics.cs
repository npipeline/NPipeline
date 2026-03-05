namespace NPipeline.Connectors.Http.Metrics;

/// <summary>A null-object <see cref="IHttpConnectorMetrics" /> that silently discards all measurements.</summary>
public sealed class NullHttpConnectorMetrics : IHttpConnectorMetrics
{
    /// <summary>Gets the singleton instance.</summary>
    public static readonly NullHttpConnectorMetrics Instance = new();

    private NullHttpConnectorMetrics()
    {
    }

    /// <inheritdoc />
    public void RecordRequest(string endpoint, string method)
    {
    }

    /// <inheritdoc />
    public void RecordResponse(string endpoint, string method, int statusCode, TimeSpan latency)
    {
    }

    /// <inheritdoc />
    public void RecordRetry(string endpoint, string method, int attempt)
    {
    }

    /// <inheritdoc />
    public void RecordRateLimitWait(string endpoint, TimeSpan waited)
    {
    }

    /// <inheritdoc />
    public void RecordError(string endpoint, string method, Exception ex)
    {
    }

    /// <inheritdoc />
    public void RecordPageFetched(string endpoint, int itemCount)
    {
    }

    /// <inheritdoc />
    public void RecordSinkWritten(string endpoint, string method, int statusCode)
    {
    }
}
