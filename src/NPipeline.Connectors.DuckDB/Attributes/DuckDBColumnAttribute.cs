namespace NPipeline.Connectors.DuckDB.Attributes;

/// <summary>
///     Maps a property to a specific DuckDB column name.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class DuckDBColumnAttribute : Attribute
{
    /// <summary>
    ///     Creates a new <see cref="DuckDBColumnAttribute" /> with the specified column name.
    /// </summary>
    /// <param name="name">The DuckDB column name.</param>
    public DuckDBColumnAttribute(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be empty.", nameof(name));

        Name = name;
    }

    /// <summary>
    ///     Creates a new <see cref="DuckDBColumnAttribute" /> using the property name as the column name.
    /// </summary>
    public DuckDBColumnAttribute()
    {
    }

    /// <summary>
    ///     The DuckDB column name. Null when using property name convention.
    /// </summary>
    public string? Name { get; init; }

    /// <summary>
    ///     Mark this property as ignored during mapping.
    /// </summary>
    public bool Ignore { get; init; }

    /// <summary>
    ///     Mark this column as part of the primary key (used for upsert support with the SQL write strategy).
    /// </summary>
    public bool PrimaryKey { get; set; }
}
