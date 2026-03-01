namespace NPipeline.Connectors.Aws.Redshift.Exceptions;

/// <summary>
///     Exception thrown when mapping between Redshift columns and .NET types fails.
/// </summary>
public class RedshiftMappingException : RedshiftException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftMappingException" /> class.
    /// </summary>
    public RedshiftMappingException()
        : base("A Redshift mapping error occurred")
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftMappingException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public RedshiftMappingException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftMappingException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public RedshiftMappingException(string message, Exception innerException) : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftMappingException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="mappedType">The type being mapped.</param>
    /// <param name="propertyName">The property that failed to map.</param>
    /// <param name="columnName">The column that failed to map.</param>
    /// <param name="innerException">The inner exception (can be null).</param>
    public RedshiftMappingException(
        string message,
        Type? mappedType = null,
        string? propertyName = null,
        string? columnName = null,
        Exception? innerException = null)
        : base(message, innerException ?? new Exception("Redshift mapping error"))
    {
        MappedType = mappedType;
        PropertyName = propertyName;
        ColumnName = columnName;
    }

    /// <summary>Gets the type being mapped from/to, if available.</summary>
    public Type? MappedType { get; }

    /// <summary>Gets the property name that failed to map, if available.</summary>
    public string? PropertyName { get; }

    /// <summary>Gets the column name that failed to map, if available.</summary>
    public string? ColumnName { get; }

    /// <summary>
    ///     Returns a string representation of the exception including type, property, and column if available.
    /// </summary>
    /// <returns>A string representation of the exception.</returns>
    public override string ToString()
    {
        var result = base.ToString();

        if (MappedType != null)
            result += $"{Environment.NewLine}Type: {MappedType.Name}";

        if (!string.IsNullOrEmpty(PropertyName))
            result += $"{Environment.NewLine}Property: {PropertyName}";

        if (!string.IsNullOrEmpty(ColumnName))
            result += $"{Environment.NewLine}Column: {ColumnName}";

        return result;
    }
}
