namespace NPipeline.Connectors.Exceptions;

/// <summary>
///     Thrown when a storage operation (read, write, exists, etc.) fails on a specific <see cref="StorageUri" />.
///     Wraps the underlying exception to preserve context while providing a consistent exception type.
/// </summary>
/// <remarks>
///     This exception is thrown by storage providers when operations fail.
///     Applications should catch this exception to handle storage operation failures gracefully, typically
///     by logging the error details (<see cref="Operation" /> and <see cref="UriText" />) and either retrying or
///     falling back to alternative strategies.
/// </remarks>
public sealed class StorageOperationException : ConnectorException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="StorageOperationException" /> class.
    /// </summary>
    /// <param name="operation">The name of the storage operation that failed (e.g., "Read", "Write").</param>
    /// <param name="uri">The storage URI where the operation failed.</param>
    /// <param name="innerException">The underlying exception that caused the operation to fail.</param>
    public StorageOperationException(string operation, StorageUri uri, Exception innerException)
        : base(BuildMessage(operation, uri), innerException)
    {
        Operation = operation;
        UriText = uri.ToString();
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="StorageOperationException" /> class.
    /// </summary>
    /// <param name="operation">The name of the storage operation that failed (e.g., "Read", "Write").</param>
    /// <param name="uriText">The text representation of the storage URI where the operation failed.</param>
    /// <param name="innerException">The underlying exception that caused the operation to fail.</param>
    public StorageOperationException(string operation, string uriText, Exception innerException)
        : base(BuildMessage(operation, uriText), innerException)
    {
        Operation = operation;
        UriText = uriText;
    }

    /// <summary>
    ///     Gets the name of the storage operation that failed (e.g., "Read", "Write", "Delete", "Exists").
    /// </summary>
    public string Operation { get; }

    /// <summary>
    ///     Gets the text representation of the storage URI where the operation failed.
    /// </summary>
    public string UriText { get; }

    private static string BuildMessage(string operation, StorageUri uri)
    {
        return BuildMessage(operation, uri.ToString());
    }

    private static string BuildMessage(string operation, string uriText)
    {
        return $"Storage operation '{operation}' failed for '{uriText}'. See inner exception for details.";
    }
}
