using System.Reflection;
using System.Linq;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Exceptions;
using NPipeline.Connectors.Utilities;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.PostgreSQL.Configuration;

namespace NPipeline.Connectors.PostgreSQL.Writers
{
    /// <summary>
    /// Per-row write strategy for PostgreSQL.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    internal sealed class PostgresPerRowWriter<T> : IDatabaseWriter<T>
    {
        private readonly IDatabaseConnection _connection;
        private readonly string _schema;
        private readonly string _tableName;
        private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
        private readonly PostgresConfiguration _configuration;
        private readonly string _insertSql;

        /// <summary>
        /// Initializes a new instance of <see cref="PostgresPerRowWriter{T}"/> class.
        /// </summary>
        /// <param name="connection">The database connection.</param>
        /// <param name="schema">The schema name.</param>
        /// <param name="tableName">The table name.</param>
        /// <param name="parameterMapper">Optional parameter mapper function.</param>
        /// <param name="configuration">The configuration.</param>
        public PostgresPerRowWriter(
            IDatabaseConnection connection,
            string schema,
            string tableName,
            Func<T, IEnumerable<DatabaseParameter>>? parameterMapper,
            PostgresConfiguration configuration)
        {
            _connection = connection;
            _schema = schema;
            _tableName = tableName;
            _parameterMapper = parameterMapper;
            _configuration = configuration;
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

            var columns = properties.Select(GetColumnName).ToArray();
            var parameters = properties.Select(p => $"@{p.Name}").ToArray();

            var quotedTableName = DatabaseIdentifierValidator.QuoteIdentifier($"{_schema}.{_tableName}");
            var quotedColumnsList = new List<string>();
            foreach (var column in columns)
            {
                quotedColumnsList.Add(DatabaseIdentifierValidator.QuoteIdentifier(column));
            }
            var quotedColumns = string.Join(", ", quotedColumnsList);
            var paramList = string.Join(", ", parameters);

            return $"INSERT INTO {quotedTableName} ({quotedColumns}) VALUES ({paramList})";
        }

        /// <summary>
        /// Writes a single item to the database.
        /// </summary>
        /// <param name="item">The item to write.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
        {
            await using var command = await _connection.CreateCommandAsync(cancellationToken);
            command.CommandText = _insertSql;
            command.CommandType = System.Data.CommandType.Text;
            command.CommandTimeout = _configuration.CommandTimeout;

            var parameters = _parameterMapper?.Invoke(item) ?? GetParametersFromItem(item);

            foreach (var param in parameters)
            {
                command.AddParameter(param.Name, param.Value);
            }

            _ = await command.ExecuteNonQueryAsync(cancellationToken);
        }

        /// <summary>
        /// Writes a batch of items to the database.
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
        }

        /// <summary>
        /// Flushes any buffered data.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
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
            // Connection is owned by the sink node, not the writer
            await ValueTask.CompletedTask;
        }
    }
}
