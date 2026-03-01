namespace NPipeline.Connectors.Aws.Redshift.Mapping;

/// <summary>
///     Specifies the table name and schema for a type when writing to Redshift.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class RedshiftTableAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftTableAttribute" /> class.
    /// </summary>
    /// <param name="name">The name of the table in Redshift.</param>
    public RedshiftTableAttribute(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentNullException(nameof(name));

        Name = name;
    }

    /// <summary>
    ///     Gets the name of the table in Redshift.
    /// </summary>
    public string Name { get; }

    /// <summary>
    ///     Gets or sets the schema name. Default is "public".
    /// </summary>
    public string? Schema { get; init; }

    /// <summary>
    ///     Gets or sets the distribution key column name for staging tables.
    /// </summary>
    public string? DistributionKey { get; init; }

    /// <summary>
    ///     Gets or sets the sort key column names for staging tables.
    /// </summary>
    public string[]? SortKeys { get; init; }
}
