using NPipeline.Execution;
using NPipeline.Nodes;

namespace NPipeline.Attributes.Nodes;

/// <summary>
///     Specifies the merge strategy for a node that receives input from multiple upstream nodes.
///     Apply this attribute to a class that implements <see cref="ITransformNode" /> or a merge-capable transform.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class MergeStrategyAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="MergeStrategyAttribute" /> class.
    /// </summary>
    /// <param name="mergeType">The merge strategy to use for this node.</param>
    public MergeStrategyAttribute(MergeType mergeType)
    {
        MergeType = mergeType;
    }

    /// <summary>
    ///     Gets the merge strategy type.
    /// </summary>
    public MergeType MergeType { get; }
}
