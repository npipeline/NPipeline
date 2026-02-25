using System.Text.Json;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.Connectors.Checkpointing;

/// <summary>
///     Abstract base class for database-based checkpoint storage.
///     Stores checkpoints in a dedicated database table.
///     Implementations provide database-specific SQL syntax for CREATE TABLE and UPSERT operations.
/// </summary>
public abstract class DatabaseCheckpointStorage : ICheckpointStorage, IAsyncDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly IDatabaseConnection _connection;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly bool _ownsConnection;
    private bool _disposed;
    private bool _initialized;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DatabaseCheckpointStorage" /> class.
    /// </summary>
    /// <param name="connection">The database connection to use.</param>
    /// <param name="tableName">The table name for storing checkpoints.</param>
    /// <param name="ownsConnection">Whether this instance owns the connection.</param>
    /// <param name="quoteChar">The quote character to use for identifiers.</param>
    protected DatabaseCheckpointStorage(
        IDatabaseConnection connection,
        string tableName = "pipeline_checkpoints",
        bool ownsConnection = false,
        string quoteChar = "\"")
    {
        ArgumentNullException.ThrowIfNull(connection);

        // Validate and quote the table name to prevent SQL injection
        var normalizedTableName = tableName ?? "pipeline_checkpoints";
        DatabaseIdentifierValidator.ValidateIdentifier(normalizedTableName, nameof(tableName));
        QuotedTableName = DatabaseIdentifierValidator.QuoteIdentifier(normalizedTableName, quoteChar);

        _connection = connection;
        _ownsConnection = ownsConnection;
    }

    /// <summary>
    ///     Gets the quoted table name for use in SQL commands.
    /// </summary>
    protected string QuotedTableName { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        GC.SuppressFinalize(this);

        _lock.Dispose();

        if (_ownsConnection && _connection is IAsyncDisposable asyncDisposable)
            await asyncDisposable.DisposeAsync();
    }

    /// <inheritdoc />
    public async Task<Checkpoint?> LoadAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            var command = await _connection.CreateCommandAsync(cancellationToken);

            command.CommandText = $@"
                SELECT checkpoint_value, checkpoint_timestamp, metadata
                FROM {QuotedTableName}
                WHERE pipeline_id = @pipelineId AND node_id = @nodeId";

            command.AddParameter("@pipelineId", pipelineId);
            command.AddParameter("@nodeId", nodeId);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            if (!await reader.ReadAsync(cancellationToken))
                return null;

            var value = reader.GetFieldValue<string>(0);
            var timestamp = reader.GetFieldValue<DateTimeOffset>(1);

            var metadataJson = reader.IsDBNull(2)
                ? null
                : reader.GetFieldValue<string>(2);

            var metadata = metadataJson != null
                ? JsonSerializer.Deserialize<Dictionary<string, string>>(metadataJson, JsonOptions)
                : null;

            return value is null
                ? null
                : new Checkpoint(value, timestamp, metadata);
        }
        finally
        {
            _ = _lock.Release();
        }
    }

    /// <inheritdoc />
    public async Task SaveAsync(string pipelineId, string nodeId, Checkpoint checkpoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);

        await EnsureInitializedAsync(cancellationToken);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            var metadataJson = checkpoint.Metadata != null
                ? JsonSerializer.Serialize(checkpoint.Metadata, JsonOptions)
                : null;

            var command = await _connection.CreateCommandAsync(cancellationToken);
            command.CommandText = GetUpsertSql();

            command.AddParameter("@pipelineId", pipelineId);
            command.AddParameter("@nodeId", nodeId);
            command.AddParameter("@value", checkpoint.Value);
            command.AddParameter("@timestamp", checkpoint.Timestamp);
            command.AddParameter("@metadata", metadataJson ?? (object)DBNull.Value);
            command.AddParameter("@createdAt", DateTimeOffset.UtcNow);
            command.AddParameter("@updatedAt", DateTimeOffset.UtcNow);

            _ = await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            _ = _lock.Release();
        }
    }

    /// <inheritdoc />
    public async Task DeleteAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            var command = await _connection.CreateCommandAsync(cancellationToken);

            command.CommandText = $@"
                DELETE FROM {QuotedTableName}
                WHERE pipeline_id = @pipelineId AND node_id = @nodeId";

            command.AddParameter("@pipelineId", pipelineId);
            command.AddParameter("@nodeId", nodeId);

            _ = await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            _ = _lock.Release();
        }
    }

    /// <inheritdoc />
    public async Task<bool> ExistsAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            var command = await _connection.CreateCommandAsync(cancellationToken);

            command.CommandText = $@"
                SELECT COUNT(1) FROM {QuotedTableName}
                WHERE pipeline_id = @pipelineId AND node_id = @nodeId";

            command.AddParameter("@pipelineId", pipelineId);
            command.AddParameter("@nodeId", nodeId);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            if (await reader.ReadAsync(cancellationToken))
            {
                var count = reader.GetFieldValue<int>(0);
                return count > 0;
            }

            return false;
        }
        finally
        {
            _ = _lock.Release();
        }
    }

    /// <summary>
    ///     Gets the SQL statement to create the checkpoint table.
    ///     Implementations should return database-specific DDL.
    /// </summary>
    /// <returns>The CREATE TABLE SQL statement.</returns>
    protected abstract string GetCreateTableSql();

    /// <summary>
    ///     Gets the SQL statement to save (insert or update) a checkpoint.
    ///     Implementations should return database-specific UPSERT syntax.
    /// </summary>
    /// <returns>The UPSERT SQL statement with parameter placeholders.</returns>
    protected abstract string GetUpsertSql();

    /// <summary>
    ///     Ensures the checkpoint table exists.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
            return;

        await _lock.WaitAsync(cancellationToken);

        try
        {
            if (_initialized)
                return;

            var command = await _connection.CreateCommandAsync(cancellationToken);
            command.CommandText = GetCreateTableSql();

            _ = await command.ExecuteNonQueryAsync(cancellationToken);
            _initialized = true;
        }
        finally
        {
            _ = _lock.Release();
        }
    }
}
