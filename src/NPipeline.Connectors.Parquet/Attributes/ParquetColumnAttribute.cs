namespace NPipeline.Connectors.Parquet.Attributes;

/// <summary>
///     Specifies the column mapping for a property when reading from or writing to Parquet files.
///     If no attribute is present, the property name is used as-is (Parquet is case-sensitive).
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ParquetColumnAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="ParquetColumnAttribute" /> class
    ///     with the specified column name.
    /// </summary>
    /// <param name="name">The Parquet column name.</param>
    public ParquetColumnAttribute(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be empty.", nameof(name));

        Name = name;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ParquetColumnAttribute" /> class
    ///     using the property name as the column name.
    /// </summary>
    public ParquetColumnAttribute()
    {
    }

    /// <summary>
    ///     Gets the Parquet column name. If <c>null</c>, the property name is used as-is.
    /// </summary>
    public string? Name { get; init; }

    /// <summary>
    ///     Gets or sets a value indicating whether this property should be ignored during Parquet mapping.
    /// </summary>
    public bool Ignore { get; init; }
}
