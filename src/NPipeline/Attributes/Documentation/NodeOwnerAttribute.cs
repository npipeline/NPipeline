namespace NPipeline.Attributes.Documentation;

/// <summary>
///     Documents the node's owner or responsible team.
///     Displayed in NPipeline Studio's node inspector.
/// </summary>
/// <example>
///     <code>
/// [NodeOwner("Order Processing Team")]
/// public class EnrichOrderTransform : ITransformNode&lt;PipelineOrder, PipelineOrder&gt;
/// { }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class NodeOwnerAttribute(string owner) : Attribute
{
    /// <summary>
    ///     Gets the owner or responsible team name.
    /// </summary>
    public string Owner { get; } = owner;
}
