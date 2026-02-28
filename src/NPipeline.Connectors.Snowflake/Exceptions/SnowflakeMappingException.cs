namespace NPipeline.Connectors.Snowflake.Exceptions;

/// <summary>
///     Exception thrown when a mapping error occurs between Snowflake data and CLR types.
/// </summary>
public class SnowflakeMappingException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeMappingException" /> class.
    /// </summary>
    public SnowflakeMappingException()
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeMappingException" /> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public SnowflakeMappingException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeMappingException" /> class with a specified error message
    ///     and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public SnowflakeMappingException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
