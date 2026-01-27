namespace NPipeline.Connectors.Exceptions;

/// <summary>
/// Exception thrown when a database mapping error occurs.
/// </summary>
public class DatabaseMappingException : DatabaseExceptionBase
{
    /// <summary>
    /// Gets the property name that caused the mapping error.
    /// </summary>
    public string? PropertyName { get; }

    /// <summary>
    /// Initializes a new instance of the DatabaseMappingException.
    /// </summary>
    /// <param name="message">The error message.</param>
    public DatabaseMappingException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the DatabaseMappingException with an inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public DatabaseMappingException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Initializes a new instance of the DatabaseMappingException with a property name.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="propertyName">The property name that caused the error.</param>
    public DatabaseMappingException(string message, string? propertyName)
        : base(message)
    {
        PropertyName = propertyName;
    }

    /// <summary>
    /// Initializes a new instance of the DatabaseMappingException with a property name and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="propertyName">The property name that caused the error.</param>
    /// <param name="innerException">The inner exception.</param>
    public DatabaseMappingException(string message, string? propertyName, Exception innerException)
        : base(message, innerException)
    {
        PropertyName = propertyName;
    }
}
