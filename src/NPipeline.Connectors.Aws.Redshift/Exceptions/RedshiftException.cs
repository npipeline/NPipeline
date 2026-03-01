using Npgsql;
using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.Connectors.Aws.Redshift.Exceptions;

/// <summary>
///     Base exception for all Redshift connector errors.
///     Wraps NpgsqlException with additional context.
/// </summary>
public class RedshiftException : DatabaseException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftException" /> class.
    /// </summary>
    public RedshiftException()
        : base("A Redshift error occurred")
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public RedshiftException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public RedshiftException(string message, Exception innerException) : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="sql">The SQL statement that caused the error.</param>
    /// <param name="innerException">The inner Npgsql exception (can be null).</param>
    public RedshiftException(string message, string? sql, NpgsqlException? innerException = null)
        : base(message, innerException ?? new Exception("Redshift error"))
    {
        Sql = sql;
        NpgsqlException = innerException;
        SqlState = innerException?.SqlState;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="sql">The SQL statement that caused the error.</param>
    /// <param name="sqlState">The SQLSTATE error code.</param>
    /// <param name="innerException">The inner exception (can be null).</param>
    public RedshiftException(string message, string? sql, string? sqlState, Exception? innerException)
        : base(message, innerException ?? new Exception("Redshift error"))
    {
        Sql = sql;
        SqlState = sqlState;
    }

    /// <summary>Gets the SQL statement that caused the error, if available.</summary>
    public string? Sql { get; }

    /// <summary>Gets the Redshift SQLSTATE error code as string, if available.</summary>
    public new string? SqlState { get; }

    /// <summary>Gets the underlying Npgsql exception, if available.</summary>
    public NpgsqlException? NpgsqlException { get; }

    /// <summary>
    ///     Returns a string representation of the exception including SQL and SQLState if available.
    /// </summary>
    /// <returns>A string representation of the exception.</returns>
    public override string ToString()
    {
        var result = base.ToString();

        if (!string.IsNullOrEmpty(Sql))
            result += $"{Environment.NewLine}SQL: {Sql}";

        if (!string.IsNullOrEmpty(SqlState))
            result += $"{Environment.NewLine}SQLState: {SqlState}";

        return result;
    }
}
