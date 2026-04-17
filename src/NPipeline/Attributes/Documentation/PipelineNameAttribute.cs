namespace NPipeline.Attributes.Documentation;

/// <summary>
///     Provides a human-readable name for a pipeline definition.
///     Displayed in NPipeline Studio's pipeline inspector.
/// </summary>
/// <example>
///     <code>
/// [PipelineName("Order Processing Pipeline")]
/// public sealed class OrderProcessingPipeline : IPipelineDefinition
/// { }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class PipelineNameAttribute(string name) : Attribute
{
    /// <summary>
    ///     Gets the human-readable name of the pipeline.
    /// </summary>
    public string Name { get; } = name;
}
