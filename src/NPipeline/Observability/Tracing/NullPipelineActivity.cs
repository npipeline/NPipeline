namespace NPipeline.Observability.Tracing;

/// <summary>
///     An activity that does nothing.
/// </summary>
public sealed class NullPipelineActivity : IPipelineActivity
{
    private NullPipelineActivity()
    {
    }

    /// <summary>
    ///     Returns the shared instance of <see cref="NullPipelineActivity" />.
    /// </summary>
    public static NullPipelineActivity Instance { get; } = new();

    /// <inheritdoc />
    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public void SetTag(string key, object value)
    {
    }

    /// <inheritdoc />
    public void RecordException(Exception exception)
    {
    }
}
