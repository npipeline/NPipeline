namespace NPipeline.DataFlow;

/// <summary>
///     A non-generic marker interface for data pipes.
/// </summary>
/// <remarks>
///     <para>
///         Data pipes are the channels through which data flows between pipeline nodes.
///         They abstract the underlying storage mechanism (streaming, buffered, materialized, etc.)
///         and provide a consistent asynchronous enumerable interface.
///     </para>
///     <para>
///         Data pipes handle:
///         - Buffering strategies (streaming vs. in-memory)
///         - Resource lifecycle management
///         - Cancellation token propagation
///         - Optional lineage tracking for provenance
///     </para>
///     <para>
///         For external code, use the strongly-typed <see cref="IDataPipe{T}" /> interface
///         which implements <see cref="IAsyncEnumerable{T}" />.
///     </para>
/// </remarks>
public interface IDataPipe : IAsyncDisposable
{
    string StreamName { get; }
    Type GetDataType();

    /// <summary>
    ///     Converts the pipe to a non-generic async enumerable.
    ///     This method is for internal framework use and should not be called directly by external code.
    ///     External code should use the typed <see cref="IDataPipe{T}" /> interface directly,
    ///     which implements <see cref="IAsyncEnumerable{T}" />.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable of objects.</returns>
    /// <remarks>
    ///     This is marked public in the interface (due to C# constraints on interface implementation)
    ///     but should be treated as internal. The main public API is the typed IDataPipe&lt;T&gt;.
    /// </remarks>
    IAsyncEnumerable<object?> ToAsyncEnumerable(CancellationToken cancellationToken = default);
}

/// <summary>
///     Represents a strongly-typed data pipe that carries items of type <typeparamref name="T" />.
/// </summary>
/// <typeparam name="T">The type of the data being carried through the pipe.</typeparam>
/// <remarks>
///     <para>
///         Data pipes are consumed using C#'s <c>await foreach</c> syntax. They implement
///         <see cref="IAsyncEnumerable{T}" /> for full async enumeration support.
///     </para>
///     <para>
///         Example usage in a node:
///         <code>
/// await foreach (var item in input.WithCancellation(cancellationToken))
/// {
///     // Process item
/// }
/// </code>
///     </para>
///     <para>
///         Data pipes own their underlying resources and must be properly disposed.
///         The pipeline framework handles disposal automatically for pipes created by nodes.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Consuming a data pipe in a sink node
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
/// // Creating a data pipe in a source node
/// public class ListSource : SourceNode&lt;int&gt;
/// {
///     private readonly List&lt;int&gt; _data;
/// 
///     public override Task&lt;IDataPipe&lt;int&gt;&gt; ExecuteAsync(
///         PipelineContext context,
///         CancellationToken cancellationToken)
///     {
///         return Task.FromResult&lt;IDataPipe&lt;int&gt;&gt;(
///             new ListDataPipe&lt;int&gt;(_data, "numbers"));
///     }
/// }
/// </code>
/// </example>
public interface IDataPipe<out T> : IDataPipe, IAsyncEnumerable<T>
{
}
