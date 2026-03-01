namespace NPipeline.Connectors.Aws.Redshift.Configuration;

/// <summary>Write strategies supported by the Redshift sink.</summary>
public enum RedshiftWriteStrategy
{
    /// <summary>Write each row individually using separate INSERT statements.</summary>
    PerRow,

    /// <summary>
    ///     Write rows using batched multi-row INSERT with VALUES clauses.
    ///     Good for up to ~10,000 rows; above that, prefer <see cref="CopyFromS3" />.
    /// </summary>
    Batch,

    /// <summary>
    ///     Upload data as compressed CSV files to an S3 bucket, then issue a Redshift
    ///     COPY command to load from S3 using an IAM role.
    ///     Best performance for large loads (100k+ rows). Requires S3 bucket + IAM role.
    /// </summary>
    CopyFromS3,
}
