namespace NPipeline.Observability.Tracing;

/// <summary>
///     A null object implementation of <see cref="IPipelineTracer" /> that does nothing.
/// </summary>
public sealed class NullPipelineTracer : IPipelineTracer
{
    private NullPipelineTracer()
    {
    }

    public static NullPipelineTracer Instance { get; } = new();

    /// <inheritdoc />
    public IPipelineActivity StartActivity(string name)
    {
        return NullPipelineActivity.Instance;
    }

    /// <inheritdoc />
    public IPipelineActivity CurrentActivity => NullPipelineActivity.Instance;
}
