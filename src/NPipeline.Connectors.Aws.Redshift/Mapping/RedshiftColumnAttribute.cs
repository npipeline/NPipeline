namespace NPipeline.Connectors.Aws.Redshift.Mapping;

/// <summary>
///     Specifies the column name mapping for a property when reading from or writing to Redshift.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class RedshiftColumnAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftColumnAttribute" /> class.
    /// </summary>
    /// <param name="name">The name of the column in Redshift.</param>
    public RedshiftColumnAttribute(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentNullException(nameof(name));

        Name = name;
    }

    /// <summary>
    ///     Gets the name of the column in Redshift.
    /// </summary>
    public string Name { get; }

    /// <summary>
    ///     Gets or sets a value indicating whether this column should be ignored during mapping.
    /// </summary>
    public bool Ignore { get; init; }
}
