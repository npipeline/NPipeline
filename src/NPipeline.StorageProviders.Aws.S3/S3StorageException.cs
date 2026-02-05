using Amazon.S3;
using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.StorageProviders.Aws.S3;

/// <summary>
/// Exception thrown when an S3 storage operation fails.
/// </summary>
public sealed class S3StorageException : ConnectorException
{
    /// <summary>
    /// Gets the S3 bucket name.
    /// </summary>
    public string Bucket { get; }

    /// <summary>
    /// Gets the S3 object key.
    /// </summary>
    public string Key { get; }

    /// <summary>
    /// Gets the inner Amazon S3 exception, if any.
    /// </summary>
    public AmazonS3Exception? InnerS3Exception { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="S3StorageException"/> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="bucket">The S3 bucket name.</param>
    /// <param name="key">The S3 object key.</param>
    public S3StorageException(string message, string bucket, string key)
        : base(message)
    {
        Bucket = bucket;
        Key = key;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="S3StorageException"/> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="bucket">The S3 bucket name.</param>
    /// <param name="key">The S3 object key.</param>
    /// <param name="innerException">The inner exception.</param>
    public S3StorageException(string message, string bucket, string key, Exception innerException)
        : base(message, innerException)
    {
        Bucket = bucket;
        Key = key;
        InnerS3Exception = innerException as AmazonS3Exception;
    }
}
