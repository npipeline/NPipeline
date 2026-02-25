using NPipeline.Connectors.Checkpointing;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.SqlServer.Checkpointing;

/// <summary>
///     SQL Server-specific implementation of database checkpoint storage.
///     Uses SQL Server-specific MERGE syntax for UPSERT operations.
/// </summary>
public sealed class SqlServerCheckpointStorage : DatabaseCheckpointStorage
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="SqlServerCheckpointStorage" /> class.
    /// </summary>
    /// <param name="connection">The database connection to use.</param>
    /// <param name="tableName">The table name for storing checkpoints.</param>
    /// <param name="ownsConnection">Whether this instance owns the connection.</param>
    public SqlServerCheckpointStorage(
        IDatabaseConnection connection,
        string tableName = "pipeline_checkpoints",
        bool ownsConnection = false)
        : base(connection, tableName, ownsConnection, "[")
    {
    }

    /// <inheritdoc />
    protected override string GetCreateTableSql()
    {
        return $@"
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{QuotedTableName.TrimStart('[').TrimEnd(']')}')
            BEGIN
                CREATE TABLE {QuotedTableName} (
                    id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    pipeline_id NVARCHAR(255) NOT NULL,
                    node_id NVARCHAR(255) NOT NULL,
                    checkpoint_value NVARCHAR(MAX) NOT NULL,
                    checkpoint_timestamp DATETIMEOFFSET NOT NULL,
                    metadata NVARCHAR(MAX) NULL,
                    created_at DATETIMEOFFSET NOT NULL,
                    updated_at DATETIMEOFFSET NOT NULL,
                    CONSTRAINT UQ_{QuotedTableName.TrimStart('[').TrimEnd(']')}_pipeline_node UNIQUE (pipeline_id, node_id)
                );
                
                CREATE INDEX IX_{QuotedTableName.TrimStart('[').TrimEnd(']')}_pipeline_id ON {QuotedTableName}(pipeline_id);
                CREATE INDEX IX_{QuotedTableName.TrimStart('[').TrimEnd(']')}_updated_at ON {QuotedTableName}(updated_at);
            END";
    }

    /// <inheritdoc />
    protected override string GetUpsertSql()
    {
        return $@"
            MERGE INTO {QuotedTableName} AS target
            USING (SELECT @pipelineId AS pipeline_id, @nodeId AS node_id) AS source
            ON (target.pipeline_id = source.pipeline_id AND target.node_id = source.node_id)
            WHEN MATCHED THEN
                UPDATE SET checkpoint_value = @value, checkpoint_timestamp = @timestamp, metadata = @metadata, updated_at = @updatedAt
            WHEN NOT MATCHED THEN
                INSERT (pipeline_id, node_id, checkpoint_value, checkpoint_timestamp, metadata, created_at, updated_at)
                VALUES (@pipelineId, @nodeId, @value, @timestamp, @metadata, @createdAt, @updatedAt);";
    }
}