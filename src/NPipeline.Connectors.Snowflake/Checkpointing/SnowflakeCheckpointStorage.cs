using NPipeline.Connectors.Checkpointing;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Snowflake.Checkpointing;

/// <summary>
///     Snowflake-specific implementation of database checkpoint storage.
///     Uses Snowflake-specific MERGE syntax for UPSERT operations and
///     Snowflake data types (VARIANT, TIMESTAMP_NTZ, NUMBER AUTOINCREMENT).
/// </summary>
public sealed class SnowflakeCheckpointStorage : DatabaseCheckpointStorage
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeCheckpointStorage" /> class.
    /// </summary>
    /// <param name="connection">The database connection to use.</param>
    /// <param name="tableName">The table name for storing checkpoints.</param>
    /// <param name="ownsConnection">Whether this instance owns the connection.</param>
    public SnowflakeCheckpointStorage(
        IDatabaseConnection connection,
        string tableName = "PIPELINE_CHECKPOINTS",
        bool ownsConnection = false)
        : base(connection, tableName, ownsConnection)
    {
    }

    /// <inheritdoc />
    protected override string GetCreateTableSql()
    {
        var cleanName = QuotedTableName.Trim('"');

        return $@"
            CREATE TABLE IF NOT EXISTS {QuotedTableName} (
                ""ID"" NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
                ""PIPELINE_ID"" VARCHAR(255) NOT NULL,
                ""NODE_ID"" VARCHAR(255) NOT NULL,
                ""CHECKPOINT_VALUE"" VARCHAR(16777216) NOT NULL,
                ""CHECKPOINT_TIMESTAMP"" TIMESTAMP_NTZ NOT NULL,
                ""METADATA"" VARIANT NULL,
                ""CREATED_AT"" TIMESTAMP_NTZ NOT NULL,
                ""UPDATED_AT"" TIMESTAMP_NTZ NOT NULL,
                CONSTRAINT ""UQ_{cleanName}_PIPELINE_NODE"" UNIQUE (""PIPELINE_ID"", ""NODE_ID"")
            )";
    }

    /// <inheritdoc />
    protected override string GetUpsertSql()
    {
        return $@"
            MERGE INTO {QuotedTableName} AS target
            USING (SELECT :pipelineId AS ""PIPELINE_ID"", :nodeId AS ""NODE_ID"") AS source
            ON (target.""PIPELINE_ID"" = source.""PIPELINE_ID"" AND target.""NODE_ID"" = source.""NODE_ID"")
            WHEN MATCHED THEN
                UPDATE SET ""CHECKPOINT_VALUE"" = :value, ""CHECKPOINT_TIMESTAMP"" = :timestamp, ""METADATA"" = :metadata, ""UPDATED_AT"" = :updatedAt
            WHEN NOT MATCHED THEN
                INSERT (""PIPELINE_ID"", ""NODE_ID"", ""CHECKPOINT_VALUE"", ""CHECKPOINT_TIMESTAMP"", ""METADATA"", ""CREATED_AT"", ""UPDATED_AT"")
                VALUES (:pipelineId, :nodeId, :value, :timestamp, :metadata, :createdAt, :updatedAt)";
    }
}
