namespace NPipeline.Sampling;

/// <summary>
/// Allows pipeline instrumentation to record sampled data records with correlation metadata.
/// </summary>
public interface IPipelineSampleRecorder
{
    /// <summary>
    /// Records a sampled data item with its correlation metadata.
    /// </summary>
    /// <param name="nodeId">The pipeline node that produced this sample.</param>
    /// <param name="direction">The sample direction, for example input or output.</param>
    /// <param name="correlationId">Correlation identifier used to pair samples for a node.</param>
    /// <param name="ancestryInputIndices">Optional contributor indices for multi-input lineage mappings.</param>
    /// <param name="serializedRecord">JSON-safe serialized payload.</param>
    /// <param name="timestamp">UTC timestamp when the sample was captured.</param>
    /// <param name="pipelineName">Optional pipeline name.</param>
    /// <param name="runId">Optional run identifier.</param>
    void RecordSample(
        string nodeId,
        string direction,
        Guid correlationId,
        int[]? ancestryInputIndices,
        object? serializedRecord,
        DateTimeOffset timestamp,
        string? pipelineName = null,
        Guid? runId = null);
}
