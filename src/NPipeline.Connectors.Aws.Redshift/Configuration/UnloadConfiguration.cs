namespace NPipeline.Connectors.Aws.Redshift.Configuration;

/// <summary>
///     Configuration for Redshift UNLOAD operations.
/// </summary>
public class UnloadConfiguration : RedshiftConfiguration
{
    /// <summary>
    ///     File format for UNLOAD output. Valid values: PARQUET, CSV, JSON.
    ///     Default: PARQUET for best performance.
    /// </summary>
    public string UnloadFileFormat { get; set; } = "PARQUET";

    /// <summary>
    ///     Compression for UNLOAD files. Valid values: GZIP, ZSTD, BZ2, NONE.
    ///     Default: GZIP.
    /// </summary>
    public string UnloadCompression { get; set; } = "GZIP";

    /// <summary>
    ///     Enable parallel UNLOAD (multiple files). Default: true.
    /// </summary>
    public bool Parallel { get; set; } = true;

    /// <summary>
    ///     Maximum number of files for parallel UNLOAD. Default: 8.
    /// </summary>
    public int MaxFiles { get; set; } = 8;

    /// <summary>
    ///     Delete S3 files after reading. Default: true.
    /// </summary>
    public bool PurgeS3FilesAfterRead { get; set; } = true;

    /// <summary>
    ///     Include header row in CSV output. Default: true.
    /// </summary>
    public bool IncludeHeader { get; set; } = true;

    /// <summary>
    ///     S3 key prefix for UNLOAD files. Default: "npipeline/unload/".
    /// </summary>
    public string UnloadS3KeyPrefix { get; set; } = "npipeline/unload/";

    /// <summary>
    ///     Validates required configuration.
    /// </summary>
    public new void Validate()
    {
        base.Validate();

        if (string.IsNullOrWhiteSpace(S3BucketName))
            throw new InvalidOperationException($"{nameof(S3BucketName)} is required for UNLOAD.");

        if (string.IsNullOrWhiteSpace(IamRoleArn))
            throw new InvalidOperationException($"{nameof(IamRoleArn)} is required for UNLOAD.");
    }
}
