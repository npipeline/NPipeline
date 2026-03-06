namespace NPipeline.Connectors.MySql.Exceptions;

/// <summary>
///     Exception thrown when a mapping error occurs between MySQL data and CLR types.
/// </summary>
public class MySqlMappingException : Exception
{
    /// <summary>Initialises a new <see cref="MySqlMappingException" />.</summary>
    public MySqlMappingException()
    {
    }

    /// <summary>Initialises a new <see cref="MySqlMappingException" /> with a message.</summary>
    public MySqlMappingException(string message) : base(message)
    {
    }

    /// <summary>Initialises a new <see cref="MySqlMappingException" /> with a message and inner exception.</summary>
    public MySqlMappingException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
