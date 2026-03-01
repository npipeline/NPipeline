namespace NPipeline.Connectors.Aws.Redshift.Configuration;

/// <summary>
///     Configuration for Redshift Spectrum external table queries.
/// </summary>
public class SpectrumConfiguration : RedshiftConfiguration
{
    /// <summary>
    ///     External schema name (e.g., "spectrum_schema").
    /// </summary>
    public string ExternalSchema { get; set; } = "spectrum";

    /// <summary>
    ///     External database name in AWS Glue Data Catalog.
    ///     Default: "default".
    /// </summary>
    public string ExternalDatabase { get; set; } = "default";

    /// <summary>
    ///     IAM role ARN for Spectrum access to S3.
    /// </summary>
    public new string IamRoleArn { get; set; } = string.Empty;

    /// <summary>
    ///     S3 path for external table data (if creating ad-hoc external table).
    /// </summary>
    public string? S3Path { get; set; }

    /// <summary>
    ///     File format for external table. Valid: PARQUET, ORC, CSV, JSON.
    /// </summary>
    public string FileFormat { get; set; } = "PARQUET";

    /// <summary>
    ///     Create external table if it doesn't exist.
    ///     Requires S3Path and column definitions.
    /// </summary>
    public bool CreateIfNotExists { get; set; }

    /// <summary>
    ///     Column definitions for CREATE EXTERNAL TABLE.
    ///     Format: "col1 INT, col2 VARCHAR(100), col3 TIMESTAMP".
    /// </summary>
    public string? ColumnDefinitions { get; set; }

    /// <summary>
    ///     Partition columns for partitioned external tables.
    ///     Format: "PARTITIONED BY (year INT, month INT)".
    /// </summary>
    public string? PartitionedBy { get; set; }

    /// <summary>
    ///     Use manifest file for file locations.
    /// </summary>
    public bool UseManifest { get; set; }

    /// <summary>
    ///     Row format SERDE for custom serialization (e.g., JSON).
    /// </summary>
    public string? RowFormatSerde { get; set; }

    /// <summary>
    ///     Validates configuration.
    /// </summary>
    public new void Validate()
    {
        base.Validate();

        if (string.IsNullOrWhiteSpace(ExternalSchema))
            throw new InvalidOperationException($"{nameof(ExternalSchema)} is required.");

        if (string.IsNullOrWhiteSpace(IamRoleArn))
            throw new InvalidOperationException($"{nameof(IamRoleArn)} is required for Spectrum access.");

        if (CreateIfNotExists && string.IsNullOrWhiteSpace(S3Path))
            throw new InvalidOperationException($"{nameof(S3Path)} is required when {nameof(CreateIfNotExists)} is true.");

        if (CreateIfNotExists && string.IsNullOrWhiteSpace(ColumnDefinitions))
            throw new InvalidOperationException($"{nameof(ColumnDefinitions)} is required when {nameof(CreateIfNotExists)} is true.");
    }
}
