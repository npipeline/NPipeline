using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.MySql.Mapping;

/// <summary>
///     Specifies the MySQL column mapping for a property or field.
///     Inherits from <see cref="ColumnAttribute"/>.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class MySqlColumnAttribute : ColumnAttribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="MySqlColumnAttribute"/> class.
    /// </summary>
    /// <param name="name">The MySQL column name.</param>
    public MySqlColumnAttribute(string name)
        : base(name)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MySqlColumnAttribute"/> class
    ///     with an option to ignore the column entirely.
    /// </summary>
    public MySqlColumnAttribute()
        : base("_")
    {
        Name = string.Empty;
    }

    /// <summary>
    ///     Gets or sets a value indicating whether the column is an AUTO_INCREMENT primary key.
    ///     When true, the column is excluded from INSERT statements.
    /// </summary>
    public bool AutoIncrement { get; set; }
}
