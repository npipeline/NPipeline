using System.Data;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Snowflake.Mapping;

/// <summary>
///     Specifies the Snowflake column mapping for a property or field.
///     Inherits from <see cref="ColumnAttribute" />.
///     This is for Snowflake-specific features only.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class SnowflakeColumnAttribute : ColumnAttribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeColumnAttribute" /> class.
    /// </summary>
    /// <param name="name">The column name.</param>
    public SnowflakeColumnAttribute(string name) : base(name)
    {
    }

    /// <summary>
    ///     Gets or sets the database type.
    /// </summary>
    public DbType DbType
    {
        get => DbTypeNullable ?? default;
        set => DbTypeNullable = value;
    }

    /// <summary>
    ///     Gets the database type (nullable).
    /// </summary>
    public DbType? DbTypeNullable { get; private set; }

    /// <summary>
    ///     Gets or sets the Snowflake native type name (e.g., "NUMBER(18,2)", "TIMESTAMP_NTZ").
    /// </summary>
    public string? NativeTypeName { get; set; }

    /// <summary>
    ///     Gets or sets the column size (for variable-length types).
    /// </summary>
    public int Size
    {
        get => SizeNullable ?? 0;
        set => SizeNullable = value;
    }

    /// <summary>
    ///     Gets the column size (nullable).
    /// </summary>
    public int? SizeNullable { get; private set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the column participates in the primary key.
    /// </summary>
    public bool PrimaryKey { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the column is an identity (AUTOINCREMENT) column.
    /// </summary>
    public bool Identity { get; set; }
}
