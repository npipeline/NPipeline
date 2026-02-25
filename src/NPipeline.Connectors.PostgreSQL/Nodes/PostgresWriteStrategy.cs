namespace NPipeline.Connectors.PostgreSQL.Nodes
{
    /// <summary>
    /// Write strategy for PostgreSQL sink node.
    /// </summary>
    public enum PostgresWriteStrategy
    {
        /// <summary>
        /// Write each row individually.
        /// </summary>
        PerRow,

        /// <summary>
        /// Write rows in batches.
        /// </summary>
        Batch,

        /// <summary>
        /// Use COPY command for bulk insert. Highest throughput for large datasets.
        /// </summary>
        Copy
    }
}
