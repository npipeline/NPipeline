using NPipeline.Connectors.Checkpointing;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Aws.Redshift.Checkpointing;

/// <summary>
///     Redshift-specific implementation of database checkpoint storage.
///     Uses Redshift-specific DDL and SQL syntax for checkpoint persistence.
/// </summary>
/// <remarks>
///     Redshift differences from PostgreSQL:
///     - Uses IDENTITY instead of BIGSERIAL for auto-increment
///     - Uses VARCHAR(MAX) instead of JSONB for metadata
///     - Supports DISTKEY and SORTKEY for table optimization
///     - Uses MERGE or ON CONFLICT syntax depending on cluster version
/// </remarks>
public sealed class RedshiftCheckpointStorage : DatabaseCheckpointStorage
{
    private readonly string _schema;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftCheckpointStorage" /> class.
    /// </summary>
    /// <param name="connection">The database connection to use.</param>
    /// <param name="schema">The schema for the checkpoint table. Defaults to "public".</param>
    /// <param name="tableName">The table name for storing checkpoints.</param>
    /// <param name="ownsConnection">Whether this instance owns the connection.</param>
    public RedshiftCheckpointStorage(
        IDatabaseConnection connection,
        string? schema = null,
        string tableName = "pipeline_checkpoints",
        bool ownsConnection = false)
        : base(connection, tableName, ownsConnection)
    {
        _schema = schema ?? "public";
    }

    /// <summary>
    ///     Gets the fully qualified table name with schema.
    /// </summary>
    private string QualifiedTableName => $"\"{_schema}\".{QuotedTableName}";

    /// <inheritdoc />
    protected override string GetCreateTableSql()
    {
        return $@"
            CREATE TABLE IF NOT EXISTS {QualifiedTableName} (
                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                pipeline_id VARCHAR(255) NOT NULL,
                node_id VARCHAR(255) NOT NULL,
                checkpoint_value VARCHAR(65535) NOT NULL,
                checkpoint_timestamp TIMESTAMP NOT NULL,
                metadata VARCHAR(65535) NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                CONSTRAINT {QuotedTableName}_pipeline_node_unique UNIQUE (pipeline_id, node_id)
            )
            DISTSTYLE KEY
            DISTKEY(pipeline_id)
            SORTKEY(pipeline_id, node_id);";
    }

    /// <inheritdoc />
    protected override string GetUpsertSql()
    {
        // Redshift-compatible upsert pattern (works across cluster versions):
        // 1) UPDATE existing row
        // 2) INSERT when no row exists
        return $@"
            UPDATE {QualifiedTableName}
            SET checkpoint_value = @value,
                checkpoint_timestamp = @timestamp,
                metadata = @metadata,
                updated_at = @updatedAt
            WHERE pipeline_id = @pipelineId
              AND node_id = @nodeId;

            INSERT INTO {QualifiedTableName} (pipeline_id, node_id, checkpoint_value, checkpoint_timestamp, metadata, created_at, updated_at)
            SELECT @pipelineId, @nodeId, @value, @timestamp, @metadata, @createdAt, @updatedAt
            WHERE NOT EXISTS (
                SELECT 1
                FROM {QualifiedTableName}
                WHERE pipeline_id = @pipelineId
                  AND node_id = @nodeId
            );";
    }
}
