using NPipeline.DataFlow;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A base class for strongly-typed source nodes that simplifies implementation by handling the untyped interface.
/// </summary>
/// <typeparam name="TOut">The type of the output data.</typeparam>
/// <remarks>
///     <para>
///         Inherit from this class to create custom source nodes that produce data into the pipeline.
///         Source nodes are the entry points of a pipeline—they read from external sources and emit
///         the data as an <see cref="IDataPipe{T}" /> for downstream processing.
///     </para>
///     <para>
///         You only need to implement <see cref="ExecuteAsync" />, which should return an <see cref="IDataPipe{TOut}" />
///         containing all the items to process. The framework handles stream lifecycle and cleanup.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Source that reads integers from 1 to 100
/// public class RangeSource : SourceNode&lt;int&gt;
/// {
///     private readonly int _start;
///     private readonly int _end;
/// 
///     public RangeSource(int start = 1, int end = 100)
///     {
///         _start = start;
///         _end = end;
///     }
/// 
///     public override async Task&lt;IDataPipe&lt;int&gt;&gt; ExecuteAsync(
///         PipelineContext context,
///         CancellationToken cancellationToken)
///     {
///         // Generate range
///         var numbers = Enumerable.Range(_start, _end - _start + 1).ToList();
///         return new InMemoryDataPipe&lt;int&gt;(numbers, "RangeSource");
///     }
/// }
/// 
/// // Or source that reads from file
/// public class TextFileSource : SourceNode&lt;string&gt;
/// {
///     private readonly string _filePath;
/// 
///     public TextFileSource(string filePath) => _filePath = filePath;
/// 
///     public override async Task&lt;IDataPipe&lt;string&gt;&gt; ExecuteAsync(
///         PipelineContext context,
///         CancellationToken cancellationToken)
///     {
///         var lines = await File.ReadAllLinesAsync(_filePath, cancellationToken);
///         return new InMemoryDataPipe&lt;string&gt;(lines, "TextFileSource");
///     }
/// }
/// </code>
/// </example>
public abstract class SourceNode<TOut> : ISourceNode<TOut>, INodeTypeMetadata
{
    public Type? InputType => null;
    public Type OutputType => typeof(TOut);

    /// <inheritdoc />
    public abstract IDataPipe<TOut> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken);

    /// <summary>
    ///     Asynchronously disposes of the node. This can be overridden by derived classes to release resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask; // no managed resources in base
    }
}
