using NPipeline.DataFlow;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A base class for strongly-typed sink nodes that simplifies implementation by handling the untyped interface.
/// </summary>
/// <typeparam name="TIn">The type of the input data.</typeparam>
/// <remarks>
///     <para>
///         Inherit from this class to create custom sink nodes that consume data from the pipeline
///         and write it to external systems. Sink nodes are terminal nodes—they produce no output.
///     </para>
///     <para>
///         You only need to implement <see cref="ExecuteAsync" />, which receives an <see cref="IDataPipe{TIn}" />
///         containing all items to process. Iterate using <c>await foreach</c> to consume them.
///         The framework handles stream lifecycle and cleanup.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Sink that writes to console
/// public class ConsoleSink : SinkNode&lt;string&gt;
/// {
///     public override async Task ExecuteAsync(
///         IDataPipe&lt;string&gt; input,
///         PipelineContext context,
///         CancellationToken cancellationToken)
///     {
///         await foreach (var line in input.WithCancellation(cancellationToken))
///         {
///             Console.WriteLine(line);
///         }
///     }
/// }
/// 
/// // Sink that writes to file
/// public class FileSink : SinkNode&lt;string&gt;
/// {
///     private readonly string _filePath;
/// 
///     public FileSink(string filePath) => _filePath = filePath;
/// 
///     public override async Task ExecuteAsync(
///         IDataPipe&lt;string&gt; input,
///         PipelineContext context,
///         CancellationToken cancellationToken)
///     {
///         using var writer = new StreamWriter(_filePath);
/// 
///         await foreach (var line in input.WithCancellation(cancellationToken))
///         {
///             await writer.WriteLineAsync(line.AsMemory(), cancellationToken);
///         }
///     }
/// }
/// 
/// // Sink that writes to database
/// public class DatabaseSink : SinkNode&lt;Product&gt;
/// {
///     private readonly string _connectionString;
/// 
///     public DatabaseSink(string connectionString) => _connectionString = connectionString;
/// 
///     public override async Task ExecuteAsync(
///         IDataPipe&lt;Product&gt; input,
///         PipelineContext context,
///         CancellationToken cancellationToken)
///     {
///         using var connection = new SqlConnection(_connectionString);
///         await connection.OpenAsync(cancellationToken);
/// 
///         await foreach (var product in input.WithCancellation(cancellationToken))
///         {
///             // INSERT product into database
///         }
///     }
/// }
/// </code>
/// </example>
public abstract class SinkNode<TIn> : ISinkNode<TIn>, INodeTypeMetadata
{
    /// <summary>
    ///     Gets the input type of the sink node.
    /// </summary>
    public Type InputType => typeof(TIn);

    /// <summary>
    ///     Gets the output type of the sink node, which is always null as sink nodes produce no output.
    /// </summary>
    public Type? OutputType => null;

    /// <inheritdoc />
    public abstract Task ExecuteAsync(IDataPipe<TIn> input, PipelineContext context, CancellationToken cancellationToken);

    /// <summary>
    ///     Asynchronously disposes of the node. This can be overridden by derived classes to release resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask; // base holds no resources
    }
}
