namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     Exception thrown when a Google Cloud Storage operation fails.
///     Captures operation context including bucket, object name, and operation type.
/// </summary>
public sealed class GcsStorageException : IOException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsStorageException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="bucket">The GCS bucket name involved in the operation.</param>
    /// <param name="objectName">The GCS object name involved in the operation.</param>
    /// <param name="operation">The operation that was being performed.</param>
    /// <param name="innerException">The original exception that caused this error.</param>
    public GcsStorageException(
        string message,
        string bucket,
        string objectName,
        string? operation,
        Exception innerException)
        : base(message, innerException)
    {
        Bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        ObjectName = objectName ?? throw new ArgumentNullException(nameof(objectName));
        Operation = operation;
        OriginalException = innerException ?? throw new ArgumentNullException(nameof(innerException));
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsStorageException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="bucket">The GCS bucket name involved in the operation.</param>
    /// <param name="objectName">The GCS object name involved in the operation.</param>
    /// <param name="operation">The operation that was being performed.</param>
    public GcsStorageException(
        string message,
        string bucket,
        string objectName,
        string? operation)
        : base(message)
    {
        Bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        ObjectName = objectName ?? throw new ArgumentNullException(nameof(objectName));
        Operation = operation;
        OriginalException = this;
    }

    /// <summary>
    ///     Gets the GCS bucket name involved in the failed operation.
    /// </summary>
    public string Bucket { get; }

    /// <summary>
    ///     Gets the GCS object name involved in the failed operation.
    /// </summary>
    public string ObjectName { get; }

    /// <summary>
    ///     Gets the operation that was being performed when the error occurred.
    ///     May be null if the operation is unknown.
    /// </summary>
    public string? Operation { get; }

    /// <summary>
    ///     Gets the original exception that caused this error.
    /// </summary>
    public Exception OriginalException { get; }

    /// <summary>
    ///     Creates a formatted error message including bucket, object, and operation context.
    /// </summary>
    /// <returns>A formatted error message string.</returns>
    public string GetDetailedMessage()
    {
        var operation = Operation ?? "unknown";
        return $"GCS operation '{operation}' failed for bucket '{Bucket}', object '{ObjectName}': {Message}";
    }
}
