using System.Reflection;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Exceptions;
using NPipeline.Connectors.Nodes;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Connection;
using NPipeline.Connectors.PostgreSQL.Mapping;
namespace NPipeline.Connectors.PostgreSQL.Nodes
{
    /// <summary>
    /// PostgreSQL source node for reading data from PostgreSQL database.
    /// </summary>
    /// <typeparam name="T">The type of objects emitted by source.</typeparam>
    public class PostgresSourceNode<T> : DatabaseSourceNode<IDatabaseReader, T>
    {
        private readonly PostgresConfiguration _configuration;
        private readonly IPostgresConnectionPool _connectionPool;
        private readonly Func<PostgresRow, T>? _mapper;
        private readonly string _query;
        private readonly DatabaseParameter[] _parameters;
        private readonly bool _continueOnError;
        private readonly string? _connectionName;
        private readonly PropertyInfo[] _writableProperties;

        /// <summary>
        /// Gets whether to stream results.
        /// </summary>
        protected override bool StreamResults => _configuration.StreamResults;

        /// <summary>
        /// Gets fetch size for streaming.
        /// </summary>
        protected override int FetchSize => _configuration.FetchSize;

        /// <summary>
        /// Gets delivery semantic.
        /// </summary>
        protected override DeliverySemantic DeliverySemantic => _configuration.DeliverySemantic;

        /// <summary>
        /// Gets checkpoint strategy.
        /// </summary>
        protected override CheckpointStrategy CheckpointStrategy => _configuration.CheckpointStrategy;

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresSourceNode{T}"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="query">The SQL query.</param>
        /// <param name="mapper">Optional custom mapper function.</param>
        /// <param name="configuration">Optional configuration.</param>
        /// <param name="parameters">Optional query parameters.</param>
        /// <param name="continueOnError">Whether to continue on row-level errors.</param>
        public PostgresSourceNode(
            string connectionString,
            string query,
            Func<PostgresRow, T>? mapper = null,
            PostgresConfiguration? configuration = null,
            DatabaseParameter[]? parameters = null,
            bool continueOnError = false)
        {
            _configuration = configuration ?? new PostgresConfiguration();
            _configuration.Validate();
            _connectionPool = new PostgresConnectionPool(connectionString);
            _mapper = mapper;
            _query = query;
            _parameters = parameters ?? [];
            _continueOnError = continueOnError;
            _connectionName = null;
            _writableProperties = GetWritableProperties();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresSourceNode{T}"/> class with connection pool.
        /// </summary>
        /// <param name="connectionPool">The connection pool.</param>
        /// <param name="query">The SQL query.</param>
        /// <param name="mapper">Optional custom mapper function.</param>
        /// <param name="configuration">Optional configuration.</param>
        /// <param name="parameters">Optional query parameters.</param>
        /// <param name="continueOnError">Whether to continue on row-level errors.</param>
        /// <param name="connectionName">Optional named connection when using a shared pool.</param>
        public PostgresSourceNode(
            IPostgresConnectionPool connectionPool,
            string query,
            Func<PostgresRow, T>? mapper = null,
            PostgresConfiguration? configuration = null,
            DatabaseParameter[]? parameters = null,
            bool continueOnError = false,
            string? connectionName = null)
        {
            _configuration = configuration ?? new PostgresConfiguration();
            _configuration.Validate();
            _connectionPool = connectionPool;
            _mapper = mapper;
            _query = query;
            _parameters = parameters ?? [];
            _continueOnError = continueOnError;
            _connectionName = string.IsNullOrWhiteSpace(connectionName) ? null : connectionName;
            _writableProperties = GetWritableProperties();
        }

        /// <summary>
        /// Gets a database connection asynchronously.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected override async Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken)
        {
            var connection = _connectionName is { Length: > 0 }
                ? await _connectionPool.GetConnectionAsync(_connectionName, cancellationToken)
                : await _connectionPool.GetConnectionAsync(cancellationToken);
            return new PostgresDatabaseConnection(connection);
        }

        /// <summary>
        /// Executes query and returns a database reader.
        /// </summary>
        /// <param name="connection">The database connection.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected override async Task<IDatabaseReader> ExecuteQueryAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
        {
            var postgresConnection = (PostgresDatabaseConnection)connection;
            var command = await postgresConnection.CreateCommandAsync(cancellationToken);

            command.CommandText = _query;
            command.CommandTimeout = _configuration.CommandTimeout;

            foreach (var param in _parameters)
            {
                command.AddParameter(param.Name, param.Value);
            }

            var reader = await command.ExecuteReaderAsync(cancellationToken);
            return reader;
        }

        /// <summary>
        /// Maps a database row to an object.
        /// </summary>
        /// <param name="reader">The database reader.</param>
        /// <returns>The mapped object.</returns>
        protected override T MapRow(IDatabaseReader reader)
        {
            var postgresReader = (PostgresDatabaseReader)reader;
            var row = new PostgresRow(postgresReader.Reader, _configuration.CaseInsensitiveMapping);
            return _mapper != null ? _mapper(row) : MapConventionBased(row);
        }

        /// <summary>
        /// Maps a row using convention-based mapping.
        /// </summary>
        /// <param name="row">The PostgreSQL row.</param>
        /// <returns>The mapped object.</returns>
        protected virtual T MapConventionBased(PostgresRow row)
        {
            var instance = Activator.CreateInstance<T>();
            foreach (var property in _writableProperties)
            {
                var columnName = GetColumnName(property);
                try
                {
                    var value = row.GetValue(columnName);
                    if (value != null)
                    {
                        var convertedValue = ConvertValue(value, property.PropertyType);
                        if (convertedValue != null || property.PropertyType.IsClass || Nullable.GetUnderlyingType(property.PropertyType) != null)
                        {
                            property.SetValue(instance, convertedValue);
                        }
                    }
                }
                catch
                {
                    if (!_continueOnError)
                    {
                        throw;
                    }
                }
            }

            return instance;
        }

        /// <summary>
        /// Gets the column name for a property.
        /// </summary>
        /// <param name="property">The property.</param>
        /// <returns>The column name.</returns>
        protected virtual string GetColumnName(PropertyInfo property)
        {
            var columnAttr = property.GetCustomAttribute<PostgresColumnAttribute>();
            return columnAttr?.Name is { Length: > 0 } name
                ? name
                : ToSnakeCase(property.Name);
        }

        /// <summary>
        /// Converts a PascalCase string to snake_case.
        /// </summary>
        /// <param name="str">The string to convert.</param>
        /// <returns>The snake_case string.</returns>
        protected static string ToSnakeCase(string str)
        {
            return string.Concat(str.Select((x, i) => i > 0 && char.IsUpper(x) ? "_" + x : x.ToString())).ToLowerInvariant();
        }

        private static object? ConvertValue(object? value, Type targetType)
        {
            if (value is null)
            {
                return null;
            }

            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;
            return underlyingType.IsInstanceOfType(value)
                ? value
                : Convert.ChangeType(value, underlyingType);
        }

        private static PropertyInfo[] GetWritableProperties()
        {
            return
            [
                .. typeof(T)
                    .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanWrite)
            ];
        }
    }
}
