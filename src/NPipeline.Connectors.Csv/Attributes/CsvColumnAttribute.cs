namespace NPipeline.Connectors.Csv.Attributes;

/// <summary>
///     Specifies the CSV column mapping for a property or field.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class CsvColumnAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="CsvColumnAttribute" /> class.
    /// </summary>
    /// <param name="name">The column name.</param>
    public CsvColumnAttribute(string name)
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
