namespace NPipeline.Graph;

/// <summary>
///     Represents a handle to a node in the pipeline, encapsulating its unique identifier.
/// </summary>
/// <param name="Id">The unique identifier of the node.</param>
public abstract record NodeHandle(string Id);

/// <summary>
///     Marker interface for node handles that produce output of a specified type.
/// </summary>
/// <typeparam name="TOut">The type of data produced by the node.</typeparam>
public interface IOutputNodeHandle<out TOut> : INodeHandle
{
}

/// <summary>
///     Marker interface for node handles that consume input of a specified type.
/// </summary>
/// <typeparam name="TIn">The type of data consumed by the node.</typeparam>
public interface IInputNodeHandle<in TIn> : INodeHandle
{
}

/// <summary>
///     Base interface for all node handles.
/// </summary>
public interface INodeHandle
{
    /// <summary>Gets the unique identifier of the node.</summary>
    string Id { get; }
}

/// <summary>
///     Represents a handle to a source node, which produces output of a specified type.
/// </summary>
/// <typeparam name="TOut">The type of data produced by the source node.</typeparam>
public sealed record SourceNodeHandle<TOut>(string Id) : NodeHandle(Id), IOutputNodeHandle<TOut>, INodeHandle;

/// <summary>
///     Represents a handle to a sink node, which consumes input of a specified type.
/// </summary>
/// <typeparam name="TIn">The type of data consumed by the sink node.</typeparam>
public sealed record SinkNodeHandle<TIn>(string Id) : NodeHandle(Id), IInputNodeHandle<TIn>, INodeHandle;

/// <summary>
///     Represents a handle to a transform node, which consumes input and produces output of specified types.
/// </summary>
/// <typeparam name="TIn">The type of data consumed by the transform node.</typeparam>
/// <typeparam name="TOut">The type of data produced by the transform node.</typeparam>
public sealed record TransformNodeHandle<TIn, TOut>(string Id) : NodeHandle(Id), IInputNodeHandle<TIn>, IOutputNodeHandle<TOut>, INodeHandle;

/// <summary>
///     Represents a handle to an aggregate node, which consumes input and produces output of specified types.
/// </summary>
/// <typeparam name="TIn">The type of data consumed by the aggregate node.</typeparam>
/// <typeparam name="TOut">The type of data produced by the aggregate node.</typeparam>
public sealed record AggregateNodeHandle<TIn, TOut>(string Id) : NodeHandle(Id), IInputNodeHandle<TIn>, IOutputNodeHandle<TOut>, INodeHandle;
