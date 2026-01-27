using NPipeline.Connectors.Exceptions;

namespace NPipeline.Connectors.PostgreSQL.Exceptions
{
    /// <summary>
    /// PostgreSQL mapping exception.
    /// </summary>
    public class PostgresMappingException : DatabaseMappingException
    {
        /// <summary>
        /// Initializes a new instance of the PostgresMappingException class.
        /// </summary>
        /// <param name="message">The error message.</param>
        public PostgresMappingException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the PostgresMappingException class.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <param name="innerException">The inner exception.</param>
        public PostgresMappingException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the PostgresMappingException class.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <param name="propertyName">The property name that caused the error.</param>
        public PostgresMappingException(string message, string? propertyName)
            : base(message, propertyName)
        {
        }

        /// <summary>
        /// Initializes a new instance of the PostgresMappingException class.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <param name="propertyName">The property name that caused the error.</param>
        /// <param name="innerException">The inner exception.</param>
        public PostgresMappingException(string message, string? propertyName, Exception innerException)
            : base(message, propertyName, innerException)
        {
        }
    }
}
