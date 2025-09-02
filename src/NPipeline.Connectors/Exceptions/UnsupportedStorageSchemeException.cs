namespace NPipeline.Connectors.Exceptions;

/// <summary>
///     Thrown when a storage URI contains an unsupported or unrecognized scheme.
/// </summary>
internal sealed class UnsupportedStorageSchemeException : ConnectorException
{
    public UnsupportedStorageSchemeException(string scheme)
        : base($"Unsupported storage scheme '{scheme}'.")
    {
        Scheme = scheme;
    }

    public UnsupportedStorageSchemeException(StorageUri uri)
        : this(uri.Scheme.ToString())
    {
    }

    public UnsupportedStorageSchemeException(string scheme, Exception innerException)
        : base($"Unsupported storage scheme '{scheme}'.", innerException)
    {
        Scheme = scheme;
    }

    public string Scheme { get; }
}
