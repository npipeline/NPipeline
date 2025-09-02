namespace NPipeline.Connectors.Exceptions;

/// <summary>
///     Thrown when a storage operation (read, write, exists, etc.) fails on a specific <see cref="NPipeline.Connectors.StorageUri" />.
///     Wraps the underlying exception to preserve context while providing a consistent exception type.
/// </summary>
internal sealed class StorageOperationException : ConnectorException
{
    public StorageOperationException(string operation, StorageUri uri, Exception innerException)
        : base(BuildMessage(operation, uri), innerException)
    {
        Operation = operation;
        UriText = uri.ToString();
    }

    public StorageOperationException(string operation, string uriText, Exception innerException)
        : base(BuildMessage(operation, uriText), innerException)
    {
        Operation = operation;
        UriText = uriText;
    }

    public string Operation { get; }
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
