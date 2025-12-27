namespace NPipeline.Lineage;

/// <summary>
///     Defines the contract for a sink that receives and processes data lineage information.
/// </summary>
/// <remarks>
///     <para>
///         A lineage sink tracks the provenance (origin and transformation path) of data as it flows
///         through the pipeline. This is essential for:
///         - Data governance and compliance (knowing data origins)
///         - Debugging data quality issues (identifying which node introduced the problem)
///         - Audit trails (proving data integrity and transformations)
///         - Data discovery (finding what data depends on a particular source)
///     </para>
///     <para>
///         When a data item completes processing through the pipeline, the lineage information
///         (recording all nodes it passed through) is passed to the lineage sink.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Simple in-memory lineage sink for testing
/// public class InMemoryLineageSink : ILineageSink
/// {
///     private readonly List&lt;LineageInfo&gt; _lineages = new();
/// 
///     public Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
///     {
///         _lineages.Add(lineageInfo);
///         return Task.CompletedTask;
///     }
/// 
///     public IReadOnlyList&lt;LineageInfo&gt; GetRecords() => _lineages.AsReadOnly();
/// }
/// 
/// // File-based lineage sink
/// public class JsonLineageSink : ILineageSink
/// {
///     private readonly string _filePath;
/// 
///     public JsonLineageSink(string filePath) => _filePath = filePath;
/// 
///     public async Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
///     {
///         var json = JsonSerializer.Serialize(lineageInfo);
///         await File.AppendAllTextAsync(_filePath, json + Environment.NewLine, cancellationToken);
///     }
/// }
/// 
/// // Use in pipeline
/// var lineageSink = new JsonLineageSink("/var/log/pipeline-lineage.jsonl");
/// var context = new PipelineContext(
///     PipelineContextConfiguration.Default with { LineageFactory = new DefaultLineageFactory(null, lineageSink) });
/// </code>
/// </example>
public interface ILineageSink
{
    /// <summary>
    ///     Asynchronously records a completed lineage trail for a data item.
    /// </summary>
    /// <param name="lineageInfo">The lineage information for the completed item, including all nodes it passed through.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    /// <remarks>
    ///     This is called after a data item has completed its journey through the pipeline.
    ///     The lineageInfo contains the full path of nodes the item passed through, including timing
    ///     and node-specific metrics.
    /// </remarks>
    Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken);
}
