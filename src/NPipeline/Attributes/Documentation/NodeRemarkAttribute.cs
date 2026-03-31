namespace NPipeline.Attributes.Documentation;

/// <summary>
///     Documents a specific aspect of a node's behavior.
///     Multiple remarks can be applied to a single node.
///     Displayed in NPipeline Studio's node inspector.
/// </summary>
/// <example>
/// <code>
/// [NodeRemark("Falls back to 'Unknown' tier if CRM lookup fails", Category = "edge-case")]
/// [NodeRemark("CRM API has a 100 req/s rate limit", Category = "performance")]
/// public class EnrichOrderTransform : ITransformNode&lt;PipelineOrder, PipelineOrder&gt;
/// { }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = false)]
public sealed class NodeRemarkAttribute(string text) : Attribute
{
    /// <summary>
    ///     Gets the remark text.
    /// </summary>
    public string Text { get; } = text;

    /// <summary>
    ///     Gets or sets an optional category for grouping and colour-coding remarks in Studio.
    ///     Suggested values: <c>"warning"</c>, <c>"performance"</c>, <c>"business-rule"</c>, <c>"edge-case"</c>.
    /// </summary>
    public string? Category { get; init; }
}
