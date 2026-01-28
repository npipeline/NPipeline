using System.Reflection;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Exceptions;
using NPipeline.Connectors.Utilities;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.PostgreSQL.Configuration;

namespace NPipeline.Connectors.PostgreSQL.Writers
{
    /// <summary>
    /// Batch write strategy for PostgreSQL.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    internal sealed class PostgresBatchWriter<T> : IDatabaseWriter<T>
    {
        private readonly IDatabaseConnection _connection;
        private readonly string _schema;
        private readonly string _tableName;
        private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
        private readonly PostgresConfiguration _configuration;
        private readonly string _insertSql;
        private readonly List<DatabaseParameter> _batchParameters;

        /// <summary>
        /// Initializes a new instance of <see cref="PostgresBatchWriter{T}"/> class.
        /// </summary>
        /// <param name="connection">The database connection.</param>
        /// <param name="schema">The schema name.</param>
        /// <param name="tableName">The table name.</param>
        /// <param name="parameterMapper">Optional parameter mapper function.</param>
        /// <param name="configuration">The configuration.</param>
        public PostgresBatchWriter(
            IDatabaseConnection connection,
            string schema,
            string tableName,
            Func<T, IEnumerable<DatabaseParameter>>? parameterMapper,
            PostgresConfiguration configuration)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _parameterMapper = parameterMapper;
            _batchParameters = [];
            _insertSql = BuildInsertSql();
        }

        /// <summary>
        /// Builds the INSERT SQL statement.
        /// </summary>
        /// <returns>The INSERT SQL statement.</returns>
        private string BuildInsertSql()
        {
            var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && !p.IsDefined(typeof(PostgresIgnoreAttribute), false));

            var columns = new List<string>();
            foreach (var property in properties)
            {
                columns.Add(GetColumnName(property));
            }

            var quotedTableName = DatabaseIdentifierValidator.QuoteIdentifier($"{_schema}.{_tableName}");
            var quotedColumnsList = new List<string>();
            foreach (var column in columns)
            {
                quotedColumnsList.Add(DatabaseIdentifierValidator.QuoteIdentifier(column));
            }
            var quotedColumns = string.Join(", ", quotedColumnsList);

            return $"INSERT INTO {quotedTableName} ({quotedColumns}) VALUES ";
        }

        /// <summary>
        /// Writes a single item to the batch buffer.
        /// </summary>
        /// <param name="item">The item to write.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
        {
            var parameters = _parameterMapper?.Invoke(item) ?? GetParametersFromItem(item);

            foreach (var param in parameters)
            {
                _batchParameters.Add(param);
            }

            // Check if batch size is reached and flush
            var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && !p.IsDefined(typeof(PostgresIgnoreAttribute), false));
            var propertyCount = properties.Count();

            if (_batchParameters.Count >= propertyCount * _configuration.BatchSize)
            {
                await FlushAsync(cancellationToken);
            }
        }

        /// <summary>
        /// Writes a batch of items to the batch buffer.
        /// </summary>
        /// <param name="items">The items to write.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
        {
            foreach (var item in items)
            {
                await WriteAsync(item, cancellationToken);
            }

            // Flush any remaining buffered items after writing all
            if (_batchParameters.Count > 0)
            {
                await FlushAsync(cancellationToken);
            }
        }

        /// <summary>
        /// Flushes the batch buffer to the database.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            if (_batchParameters.Count == 0)
            {
                return;
            }

            var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && !p.IsDefined(typeof(PostgresIgnoreAttribute), false));

            var itemCount = _batchParameters.Count / properties.Count();
            var valueClauses = new List<string>();
            var paramIndex = 0;

            for (var i = 0; i < itemCount; i++)
            {
                var paramList = new List<string>();
                foreach (var property in properties)
                {
                    paramList.Add($"@p{paramIndex}");
                    paramIndex++;
                }
                valueClauses.Add($"({string.Join(", ", paramList)})");
            }

            await using var command = await _connection.CreateCommandAsync(cancellationToken);
            command.CommandText = _insertSql + string.Join(", ", valueClauses);
            command.CommandType = System.Data.CommandType.Text;
            command.CommandTimeout = _configuration.CommandTimeout;

            foreach (var param in _batchParameters)
            {
                command.AddParameter(param.Name, param.Value);
            }

            _ = await command.ExecuteNonQueryAsync(cancellationToken);
            _batchParameters.Clear();
        }

        /// <summary>
        /// Gets parameters from an item using convention-based mapping.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns>The parameters.</returns>
        private IEnumerable<DatabaseParameter> GetParametersFromItem(T item)
        {
            var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && !p.IsDefined(typeof(PostgresIgnoreAttribute), false));

            foreach (var property in properties)
            {
                var value = property.GetValue(item);
                yield return new DatabaseParameter($"@{property.Name}", value);
            }
        }

        /// <summary>
        /// Gets the column name for a property.
        /// </summary>
        /// <param name="property">The property.</param>
        /// <returns>The column name.</returns>
        private string GetColumnName(PropertyInfo property)
        {
            var columnAttr = property.GetCustomAttribute<PostgresColumnAttribute>();
            return columnAttr?.Name ?? ToSnakeCase(property.Name);
        }

        /// <summary>
        /// Converts a PascalCase string to snake_case.
        /// </summary>
        /// <param name="str">The string to convert.</param>
        /// <returns>The snake_case string.</returns>
        private static string ToSnakeCase(string str)
        {
            return string.Concat(str.Select((x, i) => i > 0 && char.IsUpper(x) ? "_" + x : x.ToString())).ToLowerInvariant();
        }

        /// <summary>
        /// Disposes the writer.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            // Flush any buffered items before disposal
            await FlushAsync();
        }
    }
}
