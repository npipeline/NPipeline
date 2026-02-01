namespace NPipeline.Connectors.Excel.Attributes;

/// <summary>
///     Specifies that a property should be ignored during convention-based mapping.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ExcelIgnoreAttribute : Attribute
{
}
