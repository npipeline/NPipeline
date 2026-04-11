namespace NPipeline.Attributes.Documentation;

/// <summary>
///     Provides a human-readable description of what this node does.
///     Displayed in NPipeline Studio's node inspector.
/// </summary>
/// <example>
///     <code>
/// [NodeDescription("Enriches orders with customer data from the CRM. " +
///     "Performs a lookup by CustomerId and attaches the customer's tier and region.")]
/// public class EnrichOrderTransform : ITransformNode&lt;PipelineOrder, PipelineOrder&gt;
/// { }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class NodeDescriptionAttribute(string description) : Attribute
{
    /// <summary>
    ///     Gets the human-readable description of what this node does.
    /// </summary>
    public string Description { get; } = description;
}
