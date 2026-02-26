using NPipeline.Connectors.Checkpointing;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Postgres.Checkpointing;

/// <summary>
///     PostgreSQL-specific implementation of database checkpoint storage.
///     Uses PostgreSQL-specific ON CONFLICT syntax for UPSERT operations.
/// </summary>
public sealed class PostgresCheckpointStorage : DatabaseCheckpointStorage
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="PostgresCheckpointStorage" /> class.
    /// </summary>
    /// <param name="connection">The database connection to use.</param>
    /// <param name="tableName">The table name for storing checkpoints.</param>
    /// <param name="ownsConnection">Whether this instance owns the connection.</param>
    public PostgresCheckpointStorage(
        IDatabaseConnection connection,
        string tableName = "pipeline_checkpoints",
        bool ownsConnection = false)
        : base(connection, tableName, ownsConnection)
    {
    }

    /// <inheritdoc />
    protected override string GetCreateTableSql()
    {
        return $@"
            CREATE TABLE IF NOT EXISTS {QuotedTableName} (
                id BIGSERIAL PRIMARY KEY,
                pipeline_id VARCHAR(255) NOT NULL,
                node_id VARCHAR(255) NOT NULL,
                checkpoint_value TEXT NOT NULL,
                checkpoint_timestamp TIMESTAMPTZ NOT NULL,
                metadata JSONB NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                CONSTRAINT {QuotedTableName}_pipeline_node_unique UNIQUE (pipeline_id, node_id)
            );
            
            CREATE INDEX IF NOT EXISTS {QuotedTableName}_pipeline_id_idx ON {QuotedTableName}(pipeline_id);
            CREATE INDEX IF NOT EXISTS {QuotedTableName}_updated_at_idx ON {QuotedTableName}(updated_at);";
    }

    /// <inheritdoc />
    protected override string GetUpsertSql()
    {
        return $@"
            INSERT INTO {QuotedTableName} (pipeline_id, node_id, checkpoint_value, checkpoint_timestamp, metadata, created_at, updated_at)
            VALUES (@pipelineId, @nodeId, @value, @timestamp, @metadata, @createdAt, @updatedAt)
            ON CONFLICT (pipeline_id, node_id) 
            DO UPDATE SET 
                checkpoint_value = EXCLUDED.checkpoint_value,
                checkpoint_timestamp = EXCLUDED.checkpoint_timestamp,
                metadata = EXCLUDED.metadata,
                updated_at = EXCLUDED.updated_at;";
    }
}
