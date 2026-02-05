namespace NPipeline.Connectors.Attributes;

/// <summary>
///     Specifies that a property should be ignored during convention-based mapping.
///     This is the base attribute for connector-specific ignore attributes.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class IgnoreColumnAttribute : Attribute
{
}
