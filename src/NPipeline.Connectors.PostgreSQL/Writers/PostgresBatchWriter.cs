using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Exceptions;
using NPipeline.Connectors.Utilities;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Exceptions;

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
        private readonly List<object?[]> _pendingRows;
        private readonly PropertyMapping[] _mappings;
        private readonly int _parameterCount;
        private readonly Func<T, object?[]> _valueFactory;
        private readonly int _flushThreshold;

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
            _configuration.Validate();
            _parameterMapper = parameterMapper;
            _mappings = BuildMappings();
            _parameterCount = _mappings.Length;
            _valueFactory = BuildValueFactory(_mappings);
            _flushThreshold = Math.Clamp(_configuration.BatchSize, 1, _configuration.MaxBatchSize);
            _pendingRows = new List<object?[]>(_flushThreshold);
            _insertSql = BuildInsertSql();
        }

        /// <summary>
        /// Builds the INSERT SQL statement.
        /// </summary>
        /// <returns>The INSERT SQL statement.</returns>
        private string BuildInsertSql()
        {
            if (_parameterCount == 0)
            {
                throw new InvalidOperationException($"Type '{typeof(T).Name}' does not expose any writable properties to persist.");
            }

            var quotedTableName = DatabaseIdentifierValidator.QuoteIdentifier($"{_schema}.{_tableName}");
            var quotedColumns = _mappings
                .Select(m => ValidateAndQuoteIdentifier(m.ColumnName, nameof(m.ColumnName)))
                .ToArray();

            return $"INSERT INTO {quotedTableName} ({string.Join(", ", quotedColumns)}) VALUES ";
        }

        /// <summary>
        /// Writes a single item to the batch buffer.
        /// </summary>
        /// <param name="item">The item to write.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
        {
            _pendingRows.Add(GetValues(item));

            if (_pendingRows.Count >= _flushThreshold)
            {
                await FlushAsync(cancellationToken).ConfigureAwait(false);
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
                await WriteAsync(item, cancellationToken).ConfigureAwait(false);
            }

            if (_pendingRows.Count > 0)
            {
                await FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Flushes the batch buffer to the database.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            if (_pendingRows.Count == 0)
            {
                return;
            }






            var valueClauses = new List<string>(_pendingRows.Count);
            var paramIndex = 0;

            await using var command = await _connection.CreateCommandAsync(cancellationToken).ConfigureAwait(false);
            command.CommandType = System.Data.CommandType.Text;
            command.CommandTimeout = _configuration.CommandTimeout;

            foreach (var row in _pendingRows)
            {
                EnsureValueCount(row);

                var parameterNames = new string[_parameterCount];
                for (var i = 0; i < _parameterCount; i++)
                {
                    var paramName = $"@p{paramIndex++}";
                    parameterNames[i] = paramName;
                    command.AddParameter(paramName, row[i] ?? DBNull.Value);
                }

                valueClauses.Add($"({string.Join(", ", parameterNames)})");
            }

            command.CommandText = _insertSql + string.Join(", ", valueClauses);

            _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _pendingRows.Clear();
        }

        /// <summary>
        /// Gets parameters from an item using convention-based mapping.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns>The parameters.</returns>
        private object?[] GetValues(T item)
        {
            if (_parameterMapper == null)
            {
                return _valueFactory(item);
            }

            var mapped = _parameterMapper(item)?.ToArray() ?? Array.Empty<DatabaseParameter>();
            if (mapped.Length != _parameterCount)
            {
                throw new InvalidOperationException($"Custom parameter mapper for '{typeof(T).Name}' must return exactly {_parameterCount} values to match the mapped columns.");
            }

            var values = new object?[_parameterCount];
            for (var i = 0; i < _parameterCount; i++)
            {
                values[i] = mapped[i].Value;
            }

            return values;
        }

        private static PropertyMapping[] BuildMappings()
        {
            return
            [
                .. typeof(T)
                    .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanWrite && !IsIgnored(p))
                    .Select(p => new PropertyMapping(GetColumnName(p), BuildGetter(p)))
            ];
        }

        private static bool IsIgnored(PropertyInfo property)
        {
            var columnAttribute = property.GetCustomAttribute<PostgresColumnAttribute>();
            var ignoredByAttribute = columnAttribute?.Ignore == true;
            var hasIgnoreMarker = property.IsDefined(typeof(PostgresIgnoreAttribute), inherit: true);
            return ignoredByAttribute || hasIgnoreMarker;
        }

        private static string GetColumnName(PropertyInfo property)
        {
            var columnAttr = property.GetCustomAttribute<PostgresColumnAttribute>();
            return columnAttr?.Name ?? ToSnakeCase(property.Name);
        }

        private static Func<T, object?> BuildGetter(PropertyInfo property)
        {
            var instanceParam = Expression.Parameter(typeof(T), "item");
            var propertyAccess = Expression.Property(instanceParam, property);
            var convert = Expression.Convert(propertyAccess, typeof(object));
            return Expression.Lambda<Func<T, object?>>(convert, instanceParam).Compile();
        }

        private static Func<T, object?[]> BuildValueFactory(IReadOnlyList<PropertyMapping> mappings)
        {
            return item =>
            {
                var values = new object?[mappings.Count];
                for (var i = 0; i < mappings.Count; i++)
                {
                    values[i] = mappings[i].Getter(item);
                }

                return values;
            };
        }

        private void EnsureValueCount(object?[] values)
        {
            if (values.Length != _parameterCount)
            {
                throw new PostgresException($"Expected {_parameterCount} values for table '{_tableName}' but received {values.Length}.");
            }
        }

        private string ValidateAndQuoteIdentifier(string identifier, string paramName)
        {
            if (_configuration.ValidateIdentifiers)
            {
                DatabaseIdentifierValidator.ValidateIdentifier(identifier, paramName);
            }

            return DatabaseIdentifierValidator.QuoteIdentifier(identifier);
        }

        private static string ToSnakeCase(string str)
        {
            return string.Concat(str.Select((x, i) => i > 0 && char.IsUpper(x) ? "_" + x : x.ToString())).ToLowerInvariant();
        }

        private sealed record PropertyMapping(string ColumnName, Func<T, object?> Getter);

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
