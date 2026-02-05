using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Exceptions;

/// <summary>
///     Thrown when a storage URI contains an unsupported or unrecognized scheme.
/// </summary>
internal sealed class UnsupportedStorageSchemeException : ConnectorException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="UnsupportedStorageSchemeException" /> class.
    /// </summary>
    /// <param name="scheme">The unsupported storage scheme.</param>
    public UnsupportedStorageSchemeException(string scheme)
        : base($"Unsupported storage scheme '{scheme}'.")
    {
        Scheme = scheme;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="UnsupportedStorageSchemeException" /> class.
    /// </summary>
    /// <param name="uri">The storage URI with an unsupported scheme.</param>
    public UnsupportedStorageSchemeException(StorageUri uri)
        : this(uri.Scheme.ToString())
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="UnsupportedStorageSchemeException" /> class.
    /// </summary>
    /// <param name="scheme">The unsupported storage scheme.</param>
    /// <param name="innerException">The underlying exception that caused the error.</param>
    public UnsupportedStorageSchemeException(string scheme, Exception innerException)
        : base($"Unsupported storage scheme '{scheme}'.", innerException)
    {
        Scheme = scheme;
    }

    public string Scheme { get; }
}
