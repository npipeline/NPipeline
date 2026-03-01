using System.Text;
using NPipeline.Connectors.Aws.Redshift.Mapping;

namespace NPipeline.Connectors.Aws.Redshift.Configuration;

/// <summary>Configuration settings for Redshift connector operations.</summary>
public class RedshiftConfiguration
{
    private const int DefaultBatchSize = 1_000;
    private const int DefaultMaxBatchSize = 50_000;
    private const int DefaultCommandTimeout = 300;
    private const int DefaultConnectionTimeout = 30;
    private const int DefaultMinPoolSize = 1;
    private const int DefaultMaxPoolSize = 10;
    private const int DefaultFetchSize = 10_000;

    // Connection properties
    /// <summary>Gets or sets the connection string. If set, takes precedence over individual connection properties.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Gets or sets the Redshift cluster host address.</summary>
    public string Host { get; set; } = string.Empty;

    /// <summary>Gets or sets the Redshift cluster port. Default is 5439.</summary>
    public int Port { get; set; } = 5439;

    /// <summary>Gets or sets the database name to connect to.</summary>
    public string Database { get; set; } = string.Empty;

    /// <summary>Gets or sets the username for authentication.</summary>
    public string Username { get; set; } = string.Empty;

    /// <summary>Gets or sets the password for authentication.</summary>
    public string Password { get; set; } = string.Empty;

    /// <summary>Gets or sets the schema to use. Default is "public".</summary>
    public string Schema { get; set; } = "public";

    /// <summary>Gets or sets the command timeout in seconds. Default is 300.</summary>
    public int CommandTimeout { get; set; } = DefaultCommandTimeout;

    /// <summary>Gets or sets the connection timeout in seconds. Default is 30.</summary>
    public int ConnectionTimeout { get; set; } = DefaultConnectionTimeout;

    // Pool properties
    /// <summary>Gets or sets the minimum pool size. Default is 1.</summary>
    public int MinPoolSize { get; set; } = DefaultMinPoolSize;

    /// <summary>Gets or sets the maximum pool size. Default is 10.</summary>
    public int MaxPoolSize { get; set; } = DefaultMaxPoolSize;

    // Read properties
    /// <summary>Gets or sets a value indicating whether to stream results. Default is true.</summary>
    public bool StreamResults { get; set; } = true;

    /// <summary>Gets or sets the fetch size for streaming results. Default is 10,000.</summary>
    public int FetchSize { get; set; } = DefaultFetchSize;

    /// <summary>
    ///     Gets or sets the Redshift WLM query group tag.
    ///     When non-empty, source nodes issue <c>SET query_group TO ...</c> before executing the query.
    /// </summary>
    public string QueryGroup { get; set; } = "npipeline";

    // Write properties
    /// <summary>Gets or sets the write strategy. Default is Batch.</summary>
    public RedshiftWriteStrategy WriteStrategy { get; set; } = RedshiftWriteStrategy.Batch;

    /// <summary>Gets or sets the batch size for Batch write strategy. Default is 1,000.</summary>
    public int BatchSize { get; set; } = DefaultBatchSize;

    /// <summary>Gets or sets the maximum batch size. Default is 50,000.</summary>
    public int MaxBatchSize { get; set; } = DefaultMaxBatchSize;

    /// <summary>Gets or sets a value indicating whether to use transactions. Default is true.</summary>
    public bool UseTransaction { get; set; } = true;

    // Upsert properties
    /// <summary>Gets or sets a value indicating whether to use upsert mode. Default is false.</summary>
    public bool UseUpsert { get; set; }

    /// <summary>Gets or sets the key columns for upsert operations.</summary>
    public string[]? UpsertKeyColumns { get; set; }

    /// <summary>Gets or sets the action to take on merge. Default is Update.</summary>
    public OnMergeAction OnMergeAction { get; set; } = OnMergeAction.Update;

    /// <summary>Gets or sets a value indicating whether to use MERGE syntax. Default is false.</summary>
    public bool UseMergeSyntax { get; set; }

    // Staging Table properties
    /// <summary>Gets or sets the schema for staging tables. If null, uses the default schema.</summary>
    public string? StagingSchema { get; set; }

    /// <summary>Gets or sets the prefix for staging table names. Default is "#npipeline_stage_".</summary>
    public string StagingTablePrefix { get; set; } = "#npipeline_stage_";

    /// <summary>Gets or sets a value indicating whether to use temporary staging tables. Default is true.</summary>
    public bool UseTempStagingTable { get; set; } = true;

    /// <summary>Gets or sets the distribution style for staging tables. Default is Auto.</summary>
    public RedshiftDistributionStyle StagingDistributionStyle { get; set; } = RedshiftDistributionStyle.Auto;

    /// <summary>Gets or sets the distribution key column for staging tables when distribution style is Key.</summary>
    public string? StagingDistributionKey { get; set; }

    // COPY FROM S3 properties
    /// <summary>Gets or sets the S3 bucket name for CopyFromS3 write strategy.</summary>
    public string S3BucketName { get; set; } = string.Empty;

    /// <summary>Gets or sets the S3 key prefix for uploaded files. Default is "npipeline/redshift/".</summary>
    public string S3KeyPrefix { get; set; } = "npipeline/redshift/";

    /// <summary>Gets or sets the IAM role ARN for Redshift to access S3.</summary>
    public string IamRoleArn { get; set; } = string.Empty;

    /// <summary>Gets or sets the AWS region for S3 operations.</summary>
    public string AwsRegion { get; set; } = string.Empty;

    /// <summary>Gets or sets the AWS access key ID (optional, if not using IAM role).</summary>
    public string? AwsAccessKeyId { get; set; }

    /// <summary>Gets or sets the AWS secret access key (optional, if not using IAM role).</summary>
    public string? AwsSecretAccessKey { get; set; }

    /// <summary>Gets or sets the file format for COPY command. Default is "CSV".</summary>
    public string CopyFileFormat { get; set; } = "CSV";

    /// <summary>Gets or sets the compression type for COPY command. Default is "GZIP".</summary>
    public string CopyCompression { get; set; } = "GZIP";

    /// <summary>Gets or sets a value indicating whether to purge S3 files after COPY. Default is true.</summary>
    public bool PurgeS3FilesAfterCopy { get; set; } = true;

    /// <summary>Gets or sets the error handling action for COPY command. Default is "ABORT_STATEMENT".</summary>
    public string CopyOnErrorAction { get; set; } = "ABORT_STATEMENT";

    // Error Handling properties
    /// <summary>Gets or sets the maximum retry attempts. Default is 3.</summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>Gets or sets the delay between retries. Default is 2 seconds.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Gets or sets a value indicating whether to continue on error. Default is false.</summary>
    public bool ContinueOnError { get; set; }

    /// <summary>Gets or sets a value indicating whether to throw on mapping errors. Default is true.</summary>
    public bool ThrowOnMappingError { get; set; } = true;

    // Mapping properties
    /// <summary>Gets or sets the naming convention for column mapping. Default is PascalToSnakeCase.</summary>
    public RedshiftNamingConvention NamingConvention { get; set; } = RedshiftNamingConvention.PascalToSnakeCase;

    /// <summary>Gets or sets a value indicating whether to validate identifiers. Default is true.</summary>
    public bool ValidateIdentifiers { get; set; } = true;

    /// <summary>Validates required configuration fields before use.</summary>
    public void Validate()
    {
        if (WriteStrategy == RedshiftWriteStrategy.CopyFromS3)
        {
            if (string.IsNullOrWhiteSpace(S3BucketName))
            {
                throw new InvalidOperationException(
                    $"{nameof(S3BucketName)} must be set when WriteStrategy is CopyFromS3.");
            }

            if (string.IsNullOrWhiteSpace(IamRoleArn))
            {
                throw new InvalidOperationException(
                    $"{nameof(IamRoleArn)} must be set when WriteStrategy is CopyFromS3.");
            }
        }

        if (UseUpsert && (UpsertKeyColumns is null || UpsertKeyColumns.Length == 0))
        {
            throw new InvalidOperationException(
                $"{nameof(UpsertKeyColumns)} must contain at least one column when {nameof(UseUpsert)} is true.");
        }
    }

    /// <summary>Builds a Npgsql connection string from individual components if ConnectionString is not set.</summary>
    public string BuildConnectionString()
    {
        if (!string.IsNullOrEmpty(ConnectionString))
            return ConnectionString;

        var builder = new StringBuilder();

        if (!string.IsNullOrEmpty(Host))
            builder.Append($"Host={Host};");

        builder.Append($"Port={Port};");

        if (!string.IsNullOrEmpty(Database))
            builder.Append($"Database={Database};");

        if (!string.IsNullOrEmpty(Username))
            builder.Append($"Username={Username};");

        if (!string.IsNullOrEmpty(Password))
            builder.Append($"Password={Password};");

        builder.Append($"Timeout={ConnectionTimeout};");
        builder.Append($"Command Timeout={CommandTimeout};");
        builder.Append($"Minimum Pool Size={MinPoolSize};");
        builder.Append($"Maximum Pool Size={MaxPoolSize};");
        builder.Append("SSL Mode=Require;");

        return builder.ToString();
    }
}
