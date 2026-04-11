namespace NPipeline.Sampling;

/// <summary>
///     Allows pipeline instrumentation to record sampled data records with correlation metadata.
/// </summary>
public interface IPipelineSampleRecorder
{
    /// <summary>
    ///     Records a sampled data item with its correlation metadata.
    /// </summary>
    /// <param name="nodeId">The pipeline node that produced this sample.</param>
    /// <param name="direction">The sample direction, for example input or output.</param>
    /// <param name="correlationId">Correlation identifier used to pair samples for a node.</param>
    /// <param name="ancestryInputIndices">Optional contributor indices for multi-input lineage mappings.</param>
    /// <param name="serializedRecord">JSON-safe serialized payload.</param>
    /// <param name="timestamp">UTC timestamp when the sample was captured.</param>
    /// <param name="pipelineName">Optional pipeline name.</param>
    /// <param name="runId">Optional run identifier.</param>
    /// <param name="outcome">Processing outcome inferred for the sampled item.</param>
    /// <param name="retryCount">Observed retry count for the sampled item.</param>
    void RecordSample(
        string nodeId,
        string direction,
        Guid correlationId,
        int[]? ancestryInputIndices,
        object? serializedRecord,
        DateTimeOffset timestamp,
        string? pipelineName = null,
        Guid? runId = null,
        SampleOutcome outcome = SampleOutcome.Success,
        int retryCount = 0);

    /// <summary>
    ///     Records an item-level processing error with correlation metadata.
    /// </summary>
    /// <param name="nodeId">The pipeline node where the error occurred.</param>
    /// <param name="correlationId">Correlation identifier used to pair samples for a node.</param>
    /// <param name="ancestryInputIndices">Optional contributor indices for multi-input lineage mappings.</param>
    /// <param name="serializedRecord">JSON-safe serialized payload for the failed item.</param>
    /// <param name="errorMessage">The exception message.</param>
    /// <param name="exceptionType">The full exception type name.</param>
    /// <param name="stackTrace">The exception stack trace.</param>
    /// <param name="retryCount">Observed retry count for the failed item.</param>
    /// <param name="pipelineName">Optional pipeline name.</param>
    /// <param name="runId">Optional run identifier.</param>
    /// <param name="timestamp">UTC timestamp when the error was captured.</param>
    void RecordError(
        string nodeId,
        Guid correlationId,
        int[]? ancestryInputIndices,
        object? serializedRecord,
        string errorMessage,
        string? exceptionType,
        string? stackTrace,
        int retryCount = 0,
        string? pipelineName = null,
        Guid? runId = null,
        DateTimeOffset timestamp = default);
}
