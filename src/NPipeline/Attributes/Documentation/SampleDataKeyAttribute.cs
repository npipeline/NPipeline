namespace NPipeline.Attributes.Documentation;

/// <summary>
///    Marks a property as a key property that will be displayed in Studio's Node Detail Data table.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SampleDataKeyAttribute : Attribute
{
}
