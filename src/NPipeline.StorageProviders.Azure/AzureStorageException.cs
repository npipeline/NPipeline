using Azure;
using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Exception thrown when an Azure storage operation fails.
/// </summary>
public sealed class AzureStorageException : ConnectorException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureStorageException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="container">The Azure container name.</param>
    /// <param name="blob">The Azure blob name.</param>
    public AzureStorageException(string message, string container, string blob)
        : base(message)
    {
        Container = container;
        Blob = blob;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureStorageException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="container">The Azure container name.</param>
    /// <param name="blob">The Azure blob name.</param>
    /// <param name="innerException">The inner exception.</param>
    public AzureStorageException(string message, string container, string blob, Exception innerException)
        : base(message, innerException)
    {
        Container = container;
        Blob = blob;
        InnerAzureException = innerException as RequestFailedException;
    }

    /// <summary>
    ///     Gets the Azure container name.
    /// </summary>
    public string Container { get; }

    /// <summary>
    ///     Gets the Azure blob name.
    /// </summary>
    public string Blob { get; }

    /// <summary>
    ///     Gets the inner Azure RequestFailedException, if any.
    /// </summary>
    public RequestFailedException? InnerAzureException { get; }
}
