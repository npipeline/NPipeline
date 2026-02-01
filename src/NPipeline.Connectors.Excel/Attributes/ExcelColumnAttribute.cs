namespace NPipeline.Connectors.Excel.Attributes;

/// <summary>
///     Specifies the Excel column mapping for a property or field.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class ExcelColumnAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="ExcelColumnAttribute" /> class.
    /// </summary>
    /// <param name="name">The column name.</param>
    public ExcelColumnAttribute(string name)
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
