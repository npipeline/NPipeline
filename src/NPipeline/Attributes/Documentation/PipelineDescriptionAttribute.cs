namespace NPipeline.Attributes.Documentation;

/// <summary>
///     Provides a human-readable description of what this pipeline does.
///     Displayed in NPipeline Studio's pipeline inspector.
/// </summary>
/// <example>
///     <code>
/// [PipelineDescription("Processes incoming orders by validating, enriching with customer data, and persisting to the database.")]
/// public sealed class OrderProcessingPipeline : IPipelineDefinition
/// { }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class PipelineDescriptionAttribute(string description) : Attribute
{
    /// <summary>
    ///     Gets the human-readable description of what this pipeline does.
    /// </summary>
    public string Description { get; } = description;
}
