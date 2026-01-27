namespace NPipeline.Connectors.PostgreSQL.Exceptions
{
    /// <summary>
    /// Factory for creating PostgreSQL-specific exceptions from Npgsql exceptions.
    /// </summary>
    public static class PostgresExceptionFactory
    {
        /// <summary>
        /// Creates a PostgreSQL exception from a generic exception.
        /// </summary>
        /// <param name="exception">The exception to convert.</param>
        /// <returns>A PostgreSQL-specific exception.</returns>
        public static PostgresException Create(Exception exception)
        {
            return exception switch
            {
                PostgresException postgresEx => postgresEx,
                Npgsql.PostgresException npgsqlEx => new PostgresException(
                    npgsqlEx.Message,
                    npgsqlEx.SqlState,
                    PostgresTransientErrorDetector.IsTransientError(exception),
                    exception),
                Npgsql.NpgsqlException npgsqlEx2 => new PostgresException(
                    npgsqlEx2.Message,
                    null,
                    PostgresTransientErrorDetector.IsTransientError(exception),
                    exception),
                _ => new PostgresException(exception.Message, exception)
            };
        }

        /// <summary>
        /// Creates a PostgreSQL connection exception from a generic exception.
        /// </summary>
        /// <param name="exception">The exception to convert.</param>
        /// <returns>A PostgreSQL connection exception.</returns>
        public static PostgresConnectionException CreateConnectionException(Exception exception)
        {
            return exception switch
            {
                PostgresConnectionException postgresEx => postgresEx,
                Npgsql.PostgresException npgsqlEx => new PostgresConnectionException(
                    npgsqlEx.Message,
                    npgsqlEx.SqlState,
                    exception),
                Npgsql.NpgsqlException npgsqlEx2 => new PostgresConnectionException(
                    npgsqlEx2.Message,
                    null,
                    exception),
                _ => new PostgresConnectionException(exception.Message, exception)
            };
        }

        /// <summary>
        /// Creates a PostgreSQL mapping exception from a generic exception.
        /// </summary>
        /// <param name="exception">The exception to convert.</param>
        /// <param name="propertyName">The property name that caused the error.</param>
        /// <returns>A PostgreSQL mapping exception.</returns>
        public static PostgresMappingException CreateMappingException(Exception exception, string? propertyName = null)
        {
            if (exception is PostgresMappingException postgresEx)
            {
                return postgresEx;
            }

            var message = propertyName != null
                ? $"Error mapping property '{propertyName}': {exception.Message}"
                : exception.Message;

            return new PostgresMappingException(message, propertyName, exception);
        }
    }
}
