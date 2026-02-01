using NpgsqlTypes;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.PostgreSQL.Mapping;

/// <summary>
///     Specifies the PostgreSQL column mapping for a property or field.
///     Inherits from <see cref="ColumnAttribute" />.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class PostgresColumnAttribute : ColumnAttribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="PostgresColumnAttribute" /> class.
    /// </summary>
    /// <param name="name">The column name.</param>
    public PostgresColumnAttribute(string name) : base(name)
    {
    }

    /// <summary>
    ///     Gets or sets the PostgreSQL database type.
    /// </summary>
    public NpgsqlDbType? DbType { get; set; }

    /// <summary>
    ///     Gets or sets the column size (for variable-length types).
    /// </summary>
    public int? Size { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the column participates in the primary key.
    /// </summary>
    public bool PrimaryKey { get; set; }
}
