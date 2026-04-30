namespace NPipeline.Lineage;

/// <summary>
///     Defines the contract for sinks that receive lineage event records.
/// </summary>
/// <remarks>
///     <para>
///         Each <see cref="LineageRecord" /> represents a correlation-scoped event at a node.
///         Sinks receive both emitted and non-emitted outcomes, enabling completeness guarantees.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Simple in-memory lineage sink for testing
/// public class InMemoryLineageSink : ILineageSink
/// {
///     private readonly List&lt;LineageRecord&gt; _records = new();
/// 
///     public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken)
///     {
///         _records.Add(record);
///         return Task.CompletedTask;
///     }
/// 
///     public IReadOnlyList&lt;LineageRecord&gt; GetRecords() => _records.AsReadOnly();
/// }
/// 
/// // File-based lineage sink
/// public class JsonLineageSink : ILineageSink
/// {
///     private readonly string _filePath;
/// 
///     public JsonLineageSink(string filePath) => _filePath = filePath;
/// 
///     public async Task RecordAsync(LineageRecord record, CancellationToken cancellationToken)
///     {
///         var json = JsonSerializer.Serialize(record);
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
    ///     Asynchronously records a lineage event.
    /// </summary>
    /// <param name="record">Lineage event record.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    Task RecordAsync(LineageRecord record, CancellationToken cancellationToken);
}
