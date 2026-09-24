using NPipeline.Nodes;

namespace NPipeline.State;

/// <summary>
///     Identifies a pipeline node whose state is managed by an <see cref="IStatefulRegistry" />.
/// </summary>
public interface IStatefulNode : INode
{
}
