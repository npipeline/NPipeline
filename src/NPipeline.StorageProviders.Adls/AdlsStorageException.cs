using Azure;
using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     Exception thrown when an ADLS Gen2 storage operation fails.
/// </summary>
public sealed class AdlsStorageException : ConnectorException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="AdlsStorageException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="filesystem">The ADLS filesystem name.</param>
    /// <param name="path">The ADLS path.</param>
    public AdlsStorageException(string message, string filesystem, string path)
        : base(message)
    {
        Filesystem = filesystem;
        Path = path;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="AdlsStorageException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="filesystem">The ADLS filesystem name.</param>
    /// <param name="path">The ADLS path.</param>
    /// <param name="innerException">The inner exception.</param>
    public AdlsStorageException(string message, string filesystem, string path, Exception innerException)
        : base(message, innerException)
    {
        Filesystem = filesystem;
        Path = path;
        InnerAdlsException = innerException as RequestFailedException;
    }

    /// <summary>
    ///     Gets the ADLS filesystem name.
    /// </summary>
    public string Filesystem { get; }

    /// <summary>
    ///     Gets the ADLS path.
    /// </summary>
    public string Path { get; }

    /// <summary>
    ///     Gets the inner Azure RequestFailedException, if any.
    /// </summary>
    public RequestFailedException? InnerAdlsException { get; }
}
