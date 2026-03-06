namespace NPipeline.Connectors.MySql.Exceptions;

/// <summary>
///     Factory for creating MySQL-specific exceptions from driver exceptions.
/// </summary>
public static class MySqlExceptionFactory
{
    /// <summary>
    ///     Creates a <see cref="MySqlException" /> from a generic exception.
    /// </summary>
    public static MySqlException Create(string message, Exception? innerException = null)
    {
        if (innerException is null)
            return new MySqlException(message);

        return innerException switch
        {
            MySqlException ex => ex,
            MySqlConnector.MySqlException mysqlEx => CreateFromDriverException(message, mysqlEx),
            TimeoutException timeoutEx => new MySqlException($"{message} (Timeout)", null, true, timeoutEx),
            OperationCanceledException canceledEx =>
                new MySqlException($"{message} (Operation canceled)", null, true, canceledEx),
            _ => new MySqlException(message, innerException),
        };
    }

    /// <summary>
    ///     Creates a transient <see cref="MySqlException" /> from a generic exception.
    /// </summary>
    public static MySqlException CreateTransient(string message, Exception? innerException = null)
    {
        if (innerException is null)
            return new MySqlException(message, null, true);

        return innerException switch
        {
            MySqlException ex => ex,
            MySqlConnector.MySqlException mysqlEx => CreateFromDriverException(message, mysqlEx),
            TimeoutException timeoutEx => new MySqlException($"{message} (Timeout)", null, true, timeoutEx),
            OperationCanceledException canceledEx =>
                new MySqlException($"{message} (Operation canceled)", null, true, canceledEx),
            _ => new MySqlException(message, null, true, innerException),
        };
    }

    /// <summary>
    ///     Creates a <see cref="MySqlConnectionException" /> from a generic exception.
    /// </summary>
    public static MySqlConnectionException CreateConnection(string message, Exception? innerException = null)
    {
        if (innerException is null)
            return new MySqlConnectionException(message);

        return innerException switch
        {
            MySqlConnectionException ex => ex,
            MySqlConnector.MySqlException mysqlEx =>
                new MySqlConnectionException(message, mysqlEx.Number.ToString(), mysqlEx),
            TimeoutException timeoutEx =>
                new MySqlConnectionException($"{message} (Connection timeout)", null, timeoutEx),
            InvalidOperationException invalidOpEx
                when invalidOpEx.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) =>
                new MySqlConnectionException(message, null, invalidOpEx),
            _ => new MySqlConnectionException(message, innerException),
        };
    }

    /// <summary>
    ///     Creates a <see cref="MySqlMappingException" /> from a generic exception.
    /// </summary>
    public static MySqlMappingException CreateMapping(string message, Exception? innerException = null)
    {
        if (innerException is MySqlMappingException mappingEx)
            return mappingEx;

        return innerException is null
            ? new MySqlMappingException(message)
            : new MySqlMappingException(message, innerException);
    }

    private static MySqlException CreateFromDriverException(string message, MySqlConnector.MySqlException mysqlEx)
    {
        var errorCode = mysqlEx.Number.ToString();
        var isTransient = MySqlTransientErrorDetector.IsTransient(mysqlEx);
        return new MySqlException(message, errorCode, isTransient, mysqlEx);
    }
}
