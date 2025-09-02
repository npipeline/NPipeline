using NPipeline.Pipeline;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Defines the contract for a dead-letter sink, which handles items that fail processing permanently.
/// </summary>
/// <remarks>
///     <para>
///         A dead-letter sink is a special sink that collects items that fail to process despite
///         retry attempts. It enables post-mortems and analysis of problem items.
///     </para>
///     <para>
///         Common implementations:
///         - File-based storage (JSON, CSV)
///         - Database tables for failed items
///         - Message queues for reprocessing attempts
///         - Logging/alerting systems
///     </para>
///     <para>
///         When an error handler returns <see cref="NodeErrorDecision.DeadLetter" />,
///         the failed item and exception are passed to the dead-letter sink for handling.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Simple in-memory dead-letter sink
/// public class InMemoryDeadLetterSink : IDeadLetterSink
/// {
///     private readonly List&lt;FailedItem&gt; _deadLetters = new();
/// 
///     public async Task HandleAsync(
///         string nodeId,
///         object item,
///         Exception error,
///         PipelineContext context,
///         CancellationToken cancellationToken)
///     {
///         _deadLetters.Add(new FailedItem
///         {
///             NodeId = nodeId,
///             Item = item,
///             Error = error,
///             Timestamp = DateTimeOffset.UtcNow
///         });
/// 
///         await Task.CompletedTask;
///     }
/// 
///     public IReadOnlyList&lt;FailedItem&gt; GetDeadLetters() => _deadLetters.AsReadOnly();
/// }
/// 
/// // File-based dead-letter sink
/// public class FilesDeadLetterSink : IDeadLetterSink
/// {
///     private readonly string _directory;
/// 
///     public FilesDeadLetterSink(string directory) => _directory = directory;
/// 
///     public async Task HandleAsync(
///         string nodeId,
///         object item,
///         Exception error,
///         PipelineContext context,
///         CancellationToken cancellationToken)
///     {
///         var fileName = $"{nodeId}_{DateTimeOffset.UtcNow:yyyyMMdd_HHmmss_fff}.json";
///         var filePath = Path.Combine(_directory, fileName);
/// 
///         var deadLetterRecord = new
///         {
///             NodeId = nodeId,
///             Item = item,
///             Error = error.Message,
///             StackTrace = error.StackTrace,
///             Timestamp = DateTimeOffset.UtcNow
///         };
/// 
///         var json = JsonSerializer.Serialize(deadLetterRecord);
///         await File.WriteAllTextAsync(filePath, json, cancellationToken);
///     }
/// }
/// 
/// // Use in pipeline
/// var deadLetterSink = new FilesDeadLetterSink("/tmp/dead-letters");
/// var context = new PipelineContextBuilder()
///     .WithDeadLetterSink(deadLetterSink)
///     .Build();
/// </code>
/// </example>
public interface IDeadLetterSink
{
    /// <summary>
    ///     Handles a failed item by persisting it for later analysis.
    /// </summary>
    /// <param name="nodeId">The ID of the node where the error occurred.</param>
    /// <param name="item">The item that failed processing.</param>
    /// <param name="error">The exception that was thrown.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <remarks>
    ///     This method is called when an error handler decides to dead-letter an item.
    ///     Implement this to store the failed item and error information for later analysis or reprocessing.
    /// </remarks>
    Task HandleAsync(string nodeId, object item, Exception error, PipelineContext context, CancellationToken cancellationToken);
}
