using System.Collections.Concurrent;
using System.Data;
using System.Runtime.CompilerServices;
using Npgsql;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Exceptions;
using NPipeline.Connectors.PostgreSQL.Mapping;

namespace NPipeline.Connectors.PostgreSQL
{
    /// <summary>
    /// A source node that reads data from a PostgreSQL database.
    /// </summary>
    /// <typeparam name="T">The type of the output data.</typeparam>
    public sealed class PostgresSourceNode<T> : SourceNode<T>
    {
        private static readonly ConcurrentDictionary<Type, Func<PostgresRow, T>> MapperCache = new();

        private readonly PostgresConfiguration _configuration;
        private readonly string _sql;
        private readonly Func<PostgresRow, T>? _rowMapper;
        private readonly NpgsqlDataSource? _dataSource;

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresSourceNode{T}"/> class.
        /// </summary>
        /// <param name="sql">The SQL query to execute.</param>
        /// <param name="configuration">The PostgreSQL configuration.</param>
        /// <param name="rowMapper">The optional custom row mapper.</param>
        public PostgresSourceNode(
            string sql,
            PostgresConfiguration configuration,
            Func<PostgresRow, T>? rowMapper = null)
        {
            _sql = sql ?? throw new ArgumentNullException(nameof(sql));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _rowMapper = rowMapper;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresSourceNode{T}"/> class.
        /// </summary>
        /// <param name="sql">The SQL query to execute.</param>
        /// <param name="dataSource">The Npgsql data source.</param>
        /// <param name="rowMapper">The optional custom row mapper.</param>
        public PostgresSourceNode(
            string sql,
            NpgsqlDataSource dataSource,
            Func<PostgresRow, T>? rowMapper = null)
        {
            _sql = sql ?? throw new ArgumentNullException(nameof(sql));
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _rowMapper = rowMapper;
            _configuration = new PostgresConfiguration(); // Default config if none provided with data source
        }

        /// <inheritdoc />
        public override IDataPipe<T> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            var stream = ReadAsync(cancellationToken);
            return new StreamingDataPipe<T>(stream, $"PostgresSourceNode<{typeof(T).Name}>");
        }

        private async IAsyncEnumerable<T> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var attempts = 0;
            var maxAttempts = Math.Max(1, _configuration.MaxRetryAttempts);
            var retryDelay = _configuration.RetryDelay <= TimeSpan.Zero ? TimeSpan.FromSeconds(1) : _configuration.RetryDelay;

            while (true)
            {
                attempts++;
                var yielded = false;
                Exception? failure = null;

                var enumerator = ReadOnceAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
                try
                {
                    while (true)
                    {
                        bool moved;
                        try
                        {
                            moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            failure = ex;
                            break;
                        }

                        if (!moved)
                        {
                            break;
                        }

                        yielded = true;
                        yield return enumerator.Current;
                    }
                }
                finally
                {
                    await enumerator.DisposeAsync().ConfigureAwait(false);
                }

                if (failure == null)
                {
                    yield break;
                }

                if (!yielded && attempts < maxAttempts && PostgresExceptionHandler.IsTransient(failure))
                {
                    await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                throw failure;
            }
        }

        private async IAsyncEnumerable<T> ReadOnceAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var dataSource = _dataSource;
            var ownsDataSource = false;

            if (dataSource == null)
            {
                dataSource = CreateDataSource();
                ownsDataSource = true;
            }

            try
            {
                await using var command = dataSource.CreateCommand(_sql);
                command.CommandTimeout = _configuration.CommandTimeout;

                var behavior = _configuration.StreamResults
                    ? CommandBehavior.SequentialAccess | CommandBehavior.SingleResult
                    : CommandBehavior.Default;

                NpgsqlDataReader reader;
                try
                {
                    reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw PostgresExceptionHandler.Translate("Error executing PostgreSQL query.", ex);
                }

                await using (reader.ConfigureAwait(false))
                {
                    var mapper = _rowMapper ?? CreateDefaultMapper();

                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var row = new PostgresRow(reader, _configuration.CaseInsensitiveMapping);
                        T? item = default;

                        try
                        {
                            item = mapper(row);
                        }
                        catch (Exception ex)
                        {
                            if (_configuration.RowErrorHandler?.Invoke(ex, row) == true)
                            {
                                continue;
                            }

                            throw new PostgresMappingException("Error mapping PostgreSQL row to object.", typeof(T), ex);
                        }

                        if (item != null)
                        {
                            yield return item;
                        }
                    }
                }
            }
            finally
            {
                if (ownsDataSource && dataSource != null)
                {
                    await dataSource.DisposeAsync().ConfigureAwait(false);
                }
            }
        }

        private Func<PostgresRow, T> CreateDefaultMapper()
        {
            return _configuration.CacheMappingMetadata
                ? MapperCache.GetOrAdd(typeof(T), _ => PostgresMapperBuilder.Build<T>())
                : PostgresMapperBuilder.Build<T>();
        }

        private NpgsqlDataSource CreateDataSource()
        {
            if (string.IsNullOrWhiteSpace(_configuration.ConnectionString))
            {
                throw new PostgresConnectionException("Connection string is required when no data source is provided.", _configuration.ConnectionString);
            }

            try
            {
                var builder = new NpgsqlConnectionStringBuilder(_configuration.ConnectionString)
                {
                    CommandTimeout = _configuration.CommandTimeout,
                    Timeout = _configuration.ConnectionTimeout,
                    MinPoolSize = _configuration.MinPoolSize,
                    MaxPoolSize = _configuration.MaxPoolSize,
                    ReadBufferSize = _configuration.ReadBufferSize
                };

                if (_configuration.UseSslMode && _configuration.SslMode.HasValue)
                {
                    builder.SslMode = _configuration.SslMode.Value;
                }
                else if (_configuration.SslMode.HasValue)
                {
                    builder.SslMode = _configuration.SslMode.Value;
                }

                return NpgsqlDataSource.Create(builder.ConnectionString);
            }
            catch (Exception ex)
            {
                throw new PostgresConnectionException("Failed to create PostgreSQL data source.", _configuration.ConnectionString, ex);
            }
        }
    }
}
