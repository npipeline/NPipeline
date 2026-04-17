namespace NPipeline.Attributes;

/// <summary>
///     Helper methods for resolving pipeline attributes.
/// </summary>
public static class PipelineAttributeHelper
{
    /// <summary>
    ///     Gets the pipeline name from <see cref="Documentation.PipelineNameAttribute"/> if present,
    ///     otherwise returns the type's name.
    /// </summary>
    /// <param name="definitionType">The pipeline definition type.</param>
    /// <returns>The attribute's Name or the type's name.</returns>
    public static string GetPipelineName(Type definitionType)
    {
        ArgumentNullException.ThrowIfNull(definitionType);

        var attr = Attribute.GetCustomAttribute(
            definitionType,
            typeof(Documentation.PipelineNameAttribute),
            false) as Documentation.PipelineNameAttribute;

        return attr?.Name ?? definitionType.Name;
    }
}
