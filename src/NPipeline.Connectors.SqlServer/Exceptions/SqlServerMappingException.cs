namespace NPipeline.Connectors.SqlServer.Exceptions;

/// <summary>
///     Exception thrown when a mapping error occurs between SQL Server data and CLR types.
/// </summary>
public class SqlServerMappingException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="SqlServerMappingException" /> class.
    /// </summary>
    public SqlServerMappingException()
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SqlServerMappingException" /> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public SqlServerMappingException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SqlServerMappingException" /> class with a specified error message
    ///     and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public SqlServerMappingException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
