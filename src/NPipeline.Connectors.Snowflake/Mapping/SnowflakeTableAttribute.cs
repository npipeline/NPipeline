namespace NPipeline.Connectors.Snowflake.Mapping;

/// <summary>
///     Specifies the Snowflake table name and schema for a class.
///     Used by convention-based mapping to determine the target table.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class SnowflakeTableAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the SnowflakeTableAttribute.
    /// </summary>
    /// <param name="name">The table name.</param>
    public SnowflakeTableAttribute(string name)
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
    ///     Default is "PUBLIC".
    /// </summary>
    public string Schema { get; set; } = "PUBLIC";

    /// <summary>
    ///     Gets or sets the database name.
    /// </summary>
    public string? Database { get; set; }
}
