using System.Runtime.CompilerServices;

namespace NPipeline.Sampling;

/// <summary>
///     Null object pattern implementation of <see cref="IPipelineSampleRecorder" />.
/// </summary>
public sealed class NullPipelineSampleRecorder : IPipelineSampleRecorder
{
    /// <summary>
    ///     Singleton instance of the null sample recorder.
    /// </summary>
    public static readonly NullPipelineSampleRecorder Instance = new();

    private NullPipelineSampleRecorder()
    {
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordSample(
        string nodeId,
        string direction,
        Guid correlationId,
        int[]? ancestryInputIndices,
        object? serializedRecord,
        DateTimeOffset timestamp,
        string? pipelineName = null,
        Guid? runId = null,
        SampleOutcome outcome = SampleOutcome.Success,
        int retryCount = 0)
    {
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordError(
        string nodeId,
        string originNodeId,
        Guid correlationId,
        int[]? ancestryInputIndices,
        object? serializedRecord,
        string errorMessage,
        string? exceptionType,
        string? stackTrace,
        int retryCount = 0,
        string? pipelineName = null,
        Guid? runId = null,
        DateTimeOffset timestamp = default)
    {
    }
}
