namespace NPipeline.Connectors.SqlServer.Mapping;

/// <summary>
///     Specifies the SQL Server table name and schema for a class.
///     Used by convention-based mapping to determine the target table.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public sealed class SqlServerTableAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the SqlServerTableAttribute.
    /// </summary>
    /// <param name="name">The table name.</param>
    public SqlServerTableAttribute(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Table name cannot be empty.", nameof(name));

        Name = name;
    }

    /// <summary>
    ///     Gets the table name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    ///     Gets or sets the schema name.
    ///     Default is "dbo".
    /// </summary>
    public string Schema { get; set; } = "dbo";
}
