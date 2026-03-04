namespace NPipeline.Connectors.MySql.Checkpointing;

/// <summary>
///     Exception thrown when a MySQL CDC operation fails.
/// </summary>
public class MySqlCdcException : Exception
{
    /// <summary>Initialises a new <see cref="MySqlCdcException"/>.</summary>
    public MySqlCdcException() { }

    /// <summary>Initialises a new <see cref="MySqlCdcException"/> with a message.</summary>
    public MySqlCdcException(string message) : base(message) { }

    /// <summary>Initialises a new <see cref="MySqlCdcException"/> with a message and inner exception.</summary>
    public MySqlCdcException(string message, Exception innerException)
        : base(message, innerException) { }
}
