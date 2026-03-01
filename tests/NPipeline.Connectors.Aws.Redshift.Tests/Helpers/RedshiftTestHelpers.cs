using NPipeline.Connectors.Aws.Redshift.Configuration;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Helpers;

/// <summary>
///     Helper utilities for Redshift integration tests.
/// </summary>
public static class RedshiftTestHelpers
{
    /// <summary>
    ///     Creates a test configuration for basic Redshift operations.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="schema">Optional schema name (default: public).</param>
    /// <returns>A configured <see cref="RedshiftConfiguration" />.</returns>
    public static RedshiftConfiguration CreateTestConfig(string connectionString, string? schema = null)
    {
        return new RedshiftConfiguration
        {
            ConnectionString = connectionString,
            Schema = schema ?? "public",
            CommandTimeout = 60,
            BatchSize = 100,
            UseTransaction = true,
        };
    }

    /// <summary>
    ///     Creates a test configuration for COPY FROM S3 operations.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="s3Bucket">The S3 bucket name.</param>
    /// <param name="iamRoleArn">The IAM role ARN for Redshift to access S3.</param>
    /// <param name="schema">Optional schema name (default: public).</param>
    /// <returns>A configured <see cref="RedshiftConfiguration" />.</returns>
    public static RedshiftConfiguration CreateCopyFromS3Config(
        string connectionString,
        string s3Bucket,
        string iamRoleArn,
        string? schema = null)
    {
        return new RedshiftConfiguration
        {
            ConnectionString = connectionString,
            Schema = schema ?? "public",
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            S3BucketName = s3Bucket,
            IamRoleArn = iamRoleArn,
            BatchSize = 100,
            CommandTimeout = 300,
            PurgeS3FilesAfterCopy = true,
        };
    }
}
