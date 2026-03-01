using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.Connectors.Aws.Redshift.Exceptions;

/// <summary>
///     Exception thrown when a connection to Redshift fails.
/// </summary>
public class RedshiftConnectionException : DatabaseConnectionException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftConnectionException" /> class.
    /// </summary>
    public RedshiftConnectionException()
        : base("A Redshift connection error occurred")
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftConnectionException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public RedshiftConnectionException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftConnectionException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public RedshiftConnectionException(string message, Exception innerException) : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftConnectionException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="connectionString">The connection string that failed (password will be redacted).</param>
    /// <param name="innerException">The inner exception (can be null).</param>
    public RedshiftConnectionException(string message, string? connectionString, Exception? innerException = null)
        : base(message, innerException ?? new Exception("Redshift connection error"))
    {
        ConnectionString = RedactPassword(connectionString);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftConnectionException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The error code.</param>
    /// <param name="connectionString">The connection string that failed (password will be redacted).</param>
    /// <param name="innerException">The inner exception (can be null).</param>
    public RedshiftConnectionException(
        string message,
        string? errorCode,
        string? connectionString,
        Exception? innerException = null)
        : base(message, errorCode, null, innerException ?? new Exception("Redshift connection error"))
    {
        ConnectionString = RedactPassword(connectionString);
    }

    /// <summary>Gets the connection string (with password redacted) that failed.</summary>
    public string? ConnectionString { get; }

    private static string? RedactPassword(string? connectionString)
    {
        if (string.IsNullOrEmpty(connectionString))
            return connectionString;

        // Simple password redaction - replace password value with ***
        var parts = connectionString.Split(';');

        for (var i = 0; i < parts.Length; i++)
        {
            var part = parts[i];
            var equalsIndex = part.IndexOf('=');

            if (equalsIndex > 0)
            {
                var key = part.Substring(0, equalsIndex).Trim();

                if (key.Equals("Password", StringComparison.OrdinalIgnoreCase) ||
                    key.Equals("PWD", StringComparison.OrdinalIgnoreCase))
                    parts[i] = $"{key}=***";
            }
        }

        return string.Join(';', parts);
    }
}
