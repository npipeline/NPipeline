namespace NPipeline.Connectors.Exceptions;

/// <summary>
///     Thrown when no storage provider can handle a given <see cref="StorageUri" />.
///     Typically indicates that a connector package providing the required scheme is not referenced or registered.
/// </summary>
internal sealed class StorageProviderNotFoundException : ConnectorException
{
    public StorageProviderNotFoundException(StorageUri uri)
        : base(BuildMessage(uri))
    {
        Scheme = uri.Scheme.ToString();
        Host = uri.Host;
        Path = uri.Path;
    }

    public StorageProviderNotFoundException(StorageUri uri, Exception innerException)
        : base(BuildMessage(uri), innerException)
    {
        Scheme = uri.Scheme.ToString();
        Host = uri.Host;
        Path = uri.Path;
    }

    public string Scheme { get; }
    public string? Host { get; }
    public string Path { get; }

    private static string BuildMessage(StorageUri uri)
    {
        var scheme = uri.Scheme.ToString();

        return $"No storage provider found for scheme '{scheme}'. " +
               $"Ensure that the appropriate connector package for '{scheme}://' is referenced and registered.";
    }
}
