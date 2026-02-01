namespace NPipeline.Connectors.Csv.Attributes;

/// <summary>
///     Specifies that a property should be ignored during convention-based mapping.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class CsvIgnoreAttribute : Attribute
{
}
