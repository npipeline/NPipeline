namespace NPipeline.Connectors.Aws.Redshift.Analyzers;

/// <summary>
///     Diagnostic IDs for Redshift configuration analyzers.
/// </summary>
public static class DiagnosticIds
{
    /// <summary>
    ///     REDSHIFT001: Missing IamRoleArn when WriteStrategy is CopyFromS3.
    /// </summary>
    public const string MissingIamRoleArn = "REDSHIFT001";

    /// <summary>
    ///     REDSHIFT002: Missing S3BucketName when WriteStrategy is CopyFromS3.
    /// </summary>
    public const string MissingS3BucketName = "REDSHIFT002";

    /// <summary>
    ///     REDSHIFT003: Missing UpsertKeyColumns when UseUpsert is true.
    /// </summary>
    public const string MissingUpsertKeyColumns = "REDSHIFT003";

    /// <summary>
    ///     REDSHIFT004: Consider CopyFromS3 for large batch sizes.
    /// </summary>
    public const string ConsiderCopyFromS3ForLargeBatches = "REDSHIFT004";
}
