using NPipeline.Connectors.Exceptions;

namespace NPipeline.Connectors.PostgreSQL.Exceptions
{
    /// <summary>
    /// PostgreSQL-specific exception for database operations.
    /// </summary>
    public class PostgresException : DatabaseException
    {
        /// <summary>
        /// Gets a value indicating whether the error is considered transient.
        /// </summary>
        public bool IsTransient { get; }

        /// <summary>
        /// Initializes a new instance of the PostgresException class.
        /// </summary>
        /// <param name="message">The error message.</param>
        public PostgresException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the PostgresException class.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <param name="innerException">The inner exception.</param>
        public PostgresException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the PostgresException class.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <param name="errorCode">The PostgreSQL error code.</param>
        /// <param name="isTransient">Whether the error is transient.</param>
        public PostgresException(string message, string? errorCode, bool isTransient = false)
            : base(message, errorCode, sqlState: null)
        {
            IsTransient = isTransient;
        }

        /// <summary>
        /// Initializes a new instance of the PostgresException class.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <param name="errorCode">The PostgreSQL error code.</param>
        /// <param name="isTransient">Whether the error is transient.</param>
        /// <param name="innerException">The inner exception.</param>
        public PostgresException(string message, string? errorCode, bool isTransient, Exception innerException)
            : base(message, errorCode, sqlState: null, innerException)
        {
            IsTransient = isTransient;
        }
    }
}
