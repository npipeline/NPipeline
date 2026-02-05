namespace NPipeline.Connectors.Attributes;

/// <summary>
///     Specifies the column mapping for a property or field.
///     This is the base attribute for connector-specific column attributes.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class ColumnAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="ColumnAttribute" /> class.
    /// </summary>
    /// <param name="name">The column name.</param>
    public ColumnAttribute(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be empty.", nameof(name));

        Name = name;
    }

    /// <summary>
    ///     Gets the column name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    ///     Gets or sets whether this column should be ignored during mapping.
    /// </summary>
    public bool Ignore { get; set; }
}