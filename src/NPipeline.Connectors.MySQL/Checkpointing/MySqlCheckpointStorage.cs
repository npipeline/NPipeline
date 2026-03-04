using NPipeline.Connectors.Checkpointing;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.MySql.Checkpointing;

/// <summary>
///     MySQL-specific implementation of database checkpoint storage.
///     Uses MySQL's <c>INSERT … ON DUPLICATE KEY UPDATE</c> syntax for UPSERT operations.
/// </summary>
public sealed class MySqlCheckpointStorage : DatabaseCheckpointStorage
{
    /// <summary>
    ///     Initialises a new <see cref="MySqlCheckpointStorage" />.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="tableName">The checkpoint table name.</param>
    /// <param name="ownsConnection">Whether this instance owns the connection lifetime.</param>
    public MySqlCheckpointStorage(
        IDatabaseConnection connection,
        string tableName = "pipeline_checkpoints",
        bool ownsConnection = false)
        : base(connection, tableName, ownsConnection, "`")
    {
    }

    /// <inheritdoc />
    protected override string GetCreateTableSql()
    {
        var rawName = QuotedTableName.Trim('`');

        return $@"
            CREATE TABLE IF NOT EXISTS {QuotedTableName} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                pipeline_id VARCHAR(255) NOT NULL,
                node_id VARCHAR(255) NOT NULL,
                checkpoint_value TEXT NOT NULL,
                checkpoint_timestamp DATETIME(6) NOT NULL,
                metadata TEXT NULL,
                created_at DATETIME(6) NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                UNIQUE KEY uq_{rawName}_pipeline_node (pipeline_id, node_id),
                INDEX idx_{rawName}_pipeline_id (pipeline_id),
                INDEX idx_{rawName}_updated_at (updated_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
    }

    /// <inheritdoc />
    protected override string GetUpsertSql()
    {
        return $@"
            INSERT INTO {QuotedTableName}
                (pipeline_id, node_id, checkpoint_value, checkpoint_timestamp, metadata, created_at, updated_at)
            VALUES
                (@pipelineId, @nodeId, @value, @timestamp, @metadata, @createdAt, @updatedAt)
            ON DUPLICATE KEY UPDATE
                checkpoint_value = VALUES(checkpoint_value),
                checkpoint_timestamp = VALUES(checkpoint_timestamp),
                metadata = VALUES(metadata),
                updated_at = VALUES(updated_at);";
    }
}
