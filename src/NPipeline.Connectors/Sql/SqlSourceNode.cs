using System.Data.Common;
using System.Runtime.CompilerServices;
using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Sql;

/// <summary>
///     A source that runs a query and streams its rows as records. Columns bind to <typeparamref name="T" />'s members by
///     name once per query, through a mapper compiled for the result's layout; each value is read typed at its ordinal.
///     A row that fails to map goes to <see cref="SqlSourceOptions.RowErrorHandler" />.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public abstract class SqlSourceNode<T> : SourceNode<T>
{
    private readonly RecordBindingOptions _binding;
    private readonly Func<SqlRow, T>? _map;
    private readonly SqlSourceOptions _options;
    private InMemoryCheckpointStorage? _inMemoryCheckpoints;

    /// <summary>Creates the source and validates <paramref name="options" />.</summary>
    /// <param name="options">The source's options.</param>
    /// <param name="map">Builds a record from each row, instead of binding columns to members.</param>
    /// <param name="shape">How members map to columns: the connector's attributes and <paramref name="options" />' naming policy.</param>
    protected SqlSourceNode(SqlSourceOptions options, Func<SqlRow, T>? map, RecordShapeOptions shape)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(shape);
        options.Validate();

        _options = options;
        _map = map;
        _binding = new RecordBindingOptions { Shape = shape, MissingColumns = options.MissingColumns };

        if (map is null)
            SqlShape.ThrowIfUnsupported(RecordShape.For<T>(shape));
    }

    /// <summary>The connector's name in metrics and messages.</summary>
    protected abstract string ConnectorName { get; }

    /// <summary>Creates an unopened connection for <paramref name="connectionString" />.</summary>
    protected abstract DbConnection CreateConnection(string connectionString);

    /// <summary>The connection provider used for a URI when the options name no provider of their own.</summary>
    protected abstract IDatabaseConnectionProvider DefaultProvider { get; }

    /// <summary>
    ///     Opens a connection for the options' connection string or URI. Connectors with a connection pool override this to
    ///     take one from the pool when the options name it.
    /// </summary>
    protected virtual Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken) =>
        SqlConnections.OpenAsync(_options, CreateConnection, () => DefaultProvider, cancellationToken);

    /// <summary>Types a query parameter. The default leaves it to the provider.</summary>
    protected virtual void BindParameter(DbParameter parameter, object? value) => parameter.Value = value ?? DBNull.Value;

    /// <inheritdoc />
    public sealed override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        var deadLetters = OpenDeadLetterChannel(context);
        var checkpoints = CreateCheckpointManager(context);
        return new DataStream<T>(ReadAsync(deadLetters, checkpoints, cancellationToken), $"{GetType().Name}<{typeof(T).Name}>");
    }

    /// <summary>
    ///     Runs <paramref name="open" /> (connect and execute the query), retrying it if the connector retries transient
    ///     failures. Nothing has been read when it runs, so a retry cannot emit a row twice; once rows flow, a failure is
    ///     not retried. The default runs it once.
    /// </summary>
    protected virtual Task<TResult> OpenWithRetryAsync<TResult>(Func<CancellationToken, Task<TResult>> open, CancellationToken cancellationToken) =>
        open(cancellationToken);

    private async IAsyncEnumerable<T> ReadAsync(DeadLetterChannel deadLetters, CheckpointManager? checkpoints, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (connection, command, reader) = await OpenWithRetryAsync(OpenReaderAsync, cancellationToken).ConfigureAwait(false);

        try
        {
            await foreach (var item in ReadRowsAsync(reader, deadLetters, checkpoints, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await reader.DisposeAsync().ConfigureAwait(false);
            await command.DisposeAsync().ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task<(DbConnection Connection, DbCommand Command, DbDataReader Reader)> OpenReaderAsync(CancellationToken cancellationToken)
    {
        var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        DbCommand? command = null;

        try
        {
            command = connection.CreateCommand();
            command.CommandText = _options.Query;
            command.CommandTimeout = _options.CommandTimeout;

            foreach (var parameter in _options.Parameters)
            {
                var dbParameter = command.CreateParameter();
                dbParameter.ParameterName = parameter.Name;
                BindParameter(dbParameter, parameter.Value);
                _ = command.Parameters.Add(dbParameter);
            }

            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            return (connection, command, reader);
        }
        catch
        {
            if (command is not null)
                await command.DisposeAsync().ConfigureAwait(false);

            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async IAsyncEnumerable<T> ReadRowsAsync(DbDataReader reader, DeadLetterChannel deadLetters, CheckpointManager? checkpoints, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var columns = new string[reader.FieldCount];

        for (var i = 0; i < columns.Length; i++)
        {
            columns[i] = reader.GetName(i);
        }

        var mapper = _map is null ? RecordBinder.Bind<T, SqlFieldReader>(columns, _binding) : null;
        var fields = new SqlFieldReader(reader);
        var row = new SqlRow(reader, columns);
        var skip = checkpoints is null ? 0 : (await checkpoints.LoadAsync(cancellationToken).ConfigureAwait(false))?.GetAsOffset() ?? 0;
        long recordNumber = 0;
        long emitted = 0;

        try
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                recordNumber++;

                if (recordNumber <= skip)
                    continue;

                row.RecordNumber = recordNumber;
                T item = default!;
                Exception? error = null;

                try
                {
                    item = mapper is not null ? mapper(fields) : _map!(row);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    error = ex;
                }

                if (error is null)
                {
                    emitted++;
                    yield return item;
                }
                else
                {
                    await RowErrorDispatch.HandleAsync(_options.RowErrorHandler, _options.RawExcerptLength, deadLetters, ConnectorName, ConnectorName,
                        ConnectorName, recordNumber, error, row.Describe(), cancellationToken).ConfigureAwait(false);
                }

                if (checkpoints is not null)
                    await checkpoints.UpdateOffsetAsync(recordNumber, null, false, cancellationToken).ConfigureAwait(false);
            }

            if (checkpoints is not null)
                await checkpoints.SaveAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ConnectorDiagnostics.RecordRowsRead(ConnectorName, ConnectorName, emitted);

            if (checkpoints is not null)
                await checkpoints.DisposeAsync().ConfigureAwait(false);
        }
    }

    private CheckpointManager? CreateCheckpointManager(PipelineContext context)
    {
        if (_options.CheckpointStrategy == CheckpointStrategy.None)
            return null;

        var storage = _options.CheckpointStorage ?? (_inMemoryCheckpoints ??= new InMemoryCheckpointStorage());

        // Checkpoints belong to a position in the graph, so the node's id is the key when it has one.
        var pipelineId = context.NodeEnvironment.TryGetNodeId(this, out var nodeId) ? nodeId : "default";
        return new CheckpointManager(storage, pipelineId, GetType().FullName ?? GetType().Name, _options.CheckpointStrategy, _options.CheckpointInterval);
    }
}

/// <summary>Checks shared by SQL sources and sinks.</summary>
internal static class SqlShape
{
    /// <summary>Throws when a mapped member is not a single value, naming every such member.</summary>
    public static void ThrowIfUnsupported(RecordShape shape)
    {
        var unsupported = shape.Members.Where(m => !TypeClassifier.IsScalar(m.Type)).Select(m => $"{m.Name} ({m.Type.Name})").ToList();

        if (unsupported.Count > 0)
        {
            throw new NotSupportedException(
                $"SQL columns hold single values, but {shape.Type.Name} maps {string.Join(", ", unsupported)}. Mark those members [IgnoreColumn], or flatten them.");
        }
    }
}
