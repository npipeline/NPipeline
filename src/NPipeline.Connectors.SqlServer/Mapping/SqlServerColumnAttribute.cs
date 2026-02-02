using System.Data;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.SqlServer.Mapping;

/// <summary>
///     Specifies the SQL Server column mapping for a property or field.
///     Inherits from <see cref="ColumnAttribute" />.
///     This is for SQL Server-specific features only.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class SqlServerColumnAttribute : ColumnAttribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="SqlServerColumnAttribute" /> class.
    /// </summary>
    /// <param name="name">The column name.</param>
    public SqlServerColumnAttribute(string name) : base(name)
    {
    }

    /// <summary>
    ///     Gets or sets the SQL Server database type.
    ///     Use DbType = SqlDbType.VarChar to specify the type.
    /// </summary>
    public SqlDbType DbType
    {
        get => DbTypeNullable ?? default;
        set => DbTypeNullable = value;
    }

    /// <summary>
    ///     Gets the SQL Server database type (nullable).
    ///     Returns null if not explicitly set.
    /// </summary>
    public SqlDbType? DbTypeNullable { get; private set; }

    /// <summary>
    ///     Gets or sets the column size (for variable-length types).
    ///     Use Size = 100 to specify the size.
    ///     A value of 0 indicates not set.
    /// </summary>
    public int Size
    {
        get => SizeNullable ?? 0;
        set => SizeNullable = value;
    }

    /// <summary>
    ///     Gets the column size (nullable).
    ///     Returns null if not explicitly set.
    /// </summary>
    public int? SizeNullable { get; private set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the column participates in the primary key.
    /// </summary>
    public bool PrimaryKey { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the column is an identity column.
    /// </summary>
    public bool Identity { get; set; }
}
