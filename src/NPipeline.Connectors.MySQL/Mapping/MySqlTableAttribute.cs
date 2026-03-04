using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.MySql.Mapping;

/// <summary>
///     Specifies the MySQL table mapping for a class.
///     Overrides the inferred table name derived from the class name.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class MySqlTableAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="MySqlTableAttribute"/> class.
    /// </summary>
    /// <param name="name">The MySQL table name.</param>
    public MySqlTableAttribute(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Table name cannot be empty.", nameof(name));

        Name = name;
    }

    /// <summary>
    ///     Gets the MySQL table name.
    /// </summary>
    public string Name { get; }
}
