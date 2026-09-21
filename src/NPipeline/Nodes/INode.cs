namespace NPipeline.Nodes;

/// <summary>
///     Marker interface for all pipeline nodes.
/// </summary>
/// <remarks>
///     A node is not required to be disposable. A node that owns resources implements
///     <see cref="IAsyncDisposable" /> (or <see cref="IDisposable" />) itself; the runtime checks for it and
///     disposes the instance at the end of the run that created it.
/// </remarks>
public interface INode
{
}
